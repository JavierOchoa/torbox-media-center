from library.http import api_http_client, search_api_http_client, general_http_client, requestWrapper
import httpx
from enum import Enum
import PTN
from library.torbox import TORBOX_API_KEY
from library.app import SCAN_METADATA, ENABLE_AUDIO
from functions.mediaFunctions import constructSeriesTitle, cleanTitle, cleanYear
from functions.databaseFunctions import insertData, getDatabase, getDatabaseLock
import os
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from tinydb import Query
import hashlib
import json
import time

class DownloadType(Enum):
    torrent = "torrents"
    usenet = "usenet"
    webdl = "webdl"

class IDType(Enum):
    torrents = "torrent_id"
    usenet = "usenet_id"
    webdl = "web_id"

ACCEPTABLE_VIDEO_MIME_TYPES = [
    "video/x-matroska",
    "video/mp4",
]

ACCEPTABLE_AUDIO_MIME_TYPES = [
    "audio/mpeg",
    "audio/mp3",
    "audio/mp4",
    "audio/x-m4a",
    "audio/flac",
    "audio/x-flac",
    "audio/ogg",
    "audio/wav",
    "audio/x-wav",
    "audio/aac",
]

METADATA_CACHE_DB_NAME = "metadata_cache"
METADATA_CACHE_SCHEMA_VERSION = 1
METADATA_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days
METADATA_FAILURE_CACHE_TTL_SECONDS = 60 * 60 * 6  # 6 hours
METADATA_MAX_WORKERS = 2

def getAcceptedMediaType(mimetype: str | None):
    if not mimetype:
        return None

    if mimetype.startswith("video/") and mimetype in ACCEPTABLE_VIDEO_MIME_TYPES:
        return "video"

    if ENABLE_AUDIO and mimetype.startswith("audio/") and mimetype in ACCEPTABLE_AUDIO_MIME_TYPES:
        return "music"

    return None

def getBasicMusicMetadata(item: dict, file: dict):
    file_name = file.get("short_name") or file.get("name") or str(file.get("id"))
    root_folder_name = item.get("name") or item.get("hash") or "music"

    return {
        "metadata_title": file_name,
        "metadata_link": None,
        "metadata_mediatype": "music",
        "metadata_image": None,
        "metadata_backdrop": None,
        "metadata_years": None,
        "metadata_season": None,
        "metadata_episode": None,
        "metadata_filename": file_name,
        "metadata_rootfoldername": root_folder_name,
        "metadata_foldername": None,
    }

def getMetadataCacheKey(download_type: DownloadType, item: dict, file: dict):
    cache_key_data = {
        "schema_version": METADATA_CACHE_SCHEMA_VERSION,
        "download_type": download_type.value,
        "item_id": item.get("id"),
        "item_hash": item.get("hash"),
        "file_id": file.get("id"),
        "file_name": file.get("short_name") or file.get("name"),
        "file_path": file.get("name"),
        "file_mimetype": file.get("mimetype"),
    }

    return hashlib.sha256(json.dumps(cache_key_data, sort_keys=True, default=str).encode()).hexdigest()

def getCachedMetadata(cache_key: str):
    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return None

    query = Query()
    with db_lock:
        record = db.get(query.cache_key == cache_key)
        if record is None:
            return None

        now = int(time.time())
        if record.get("schema_version") != METADATA_CACHE_SCHEMA_VERSION or record.get("expires_at", 0) <= now:
            db.remove(query.cache_key == cache_key)
            return None

        return record.get("metadata"), record.get("success", False), record.get("detail", "")

def setCachedMetadata(cache_key: str, metadata: dict, success: bool, detail: str):
    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return

    now = int(time.time())
    ttl_seconds = METADATA_CACHE_TTL_SECONDS if success else METADATA_FAILURE_CACHE_TTL_SECONDS

    record = {
        "cache_key": cache_key,
        "schema_version": METADATA_CACHE_SCHEMA_VERSION,
        "success": success,
        "detail": detail,
        "metadata": metadata,
        "cached_at": now,
        "expires_at": now + ttl_seconds,
    }

    query = Query()
    with db_lock:
        db.upsert(record, query.cache_key == cache_key)

def pruneExpiredMetadataCache():
    db = getDatabase(METADATA_CACHE_DB_NAME)
    db_lock = getDatabaseLock(METADATA_CACHE_DB_NAME)

    if db is None or db_lock is None:
        return

    now = int(time.time())
    query = Query()

    with db_lock:
        removed = db.remove((query.schema_version != METADATA_CACHE_SCHEMA_VERSION) | (query.expires_at <= now))
        if removed:
            logging.info(f"Pruned {len(removed)} expired metadata cache entries.")

def process_file(item, file, type):
    """Process a single file and return the processed data"""
    short_name = file.get("short_name") or file.get("name") or str(file.get("id"))
    mimetype = file.get("mimetype")
    media_type = getAcceptedMediaType(mimetype)
    item_name = item.get("name")

    if media_type is None:
        logging.debug(f"Skipping file {short_name} with mimetype {mimetype}")
        return None
    
    data = {
        "item_id": item.get("id"),
        "type": type.value,
        "folder_name": item_name,
        "DEBUG_name": item_name,
        "DEBUG_hash": item.get("hash"),
        "DEBUG_file_name": short_name,
        "folder_hash": item.get("hash"),
        "file_id": file.get("id"),
        "file_name": short_name,
        "file_size": file.get("size"),
        "file_mimetype": mimetype,
        "path": file.get("name"),
        "download_link": f"https://api.torbox.app/v1/api/{type.value}/requestdl?token={TORBOX_API_KEY}&{IDType[type.value].value}={item.get('id')}&file_id={file.get('id')}&redirect=true",
        "extension": os.path.splitext(short_name)[-1],
    }

    if media_type == "music":
        metadata = getBasicMusicMetadata(item, file)
        data.update(metadata)
        logging.debug(data)
        insertData(data, type.value)
        return data

    title_data = PTN.parse(short_name)

    if item_name == item.get("hash"):
        item_name = title_data.get("title", short_name)
        data["folder_name"] = item_name

    cache_key = getMetadataCacheKey(type, item, file) if SCAN_METADATA else None
    metadata, _, _ = searchMetadata(
        title_data.get("title", short_name),
        title_data,
        short_name,
        f"{item_name} {short_name}",
        item.get("hash"),
        item_name,
        cache_key=cache_key,
    )
    data.update(metadata)
    logging.debug(data)
    insertData(data, type.value)
    return data

def getUserDownloads(type: DownloadType):
    offset = 0
    limit = 1000

    file_data = []
    
    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "bypass_cache": True,
        }
        try:
            response = api_http_client.get(f"/{type.value}/mylist", params=params)
        except Exception as e:
            logging.error(f"Error fetching {type.value} at offset {offset}: {e}")
            return None, False, f"Error fetching {type.value} at offset {offset}: {e}"
        if response.status_code != 200:
            return None, False, f"Error fetching {type.value} at offset {offset}. {response.status_code}"
        try:
            data = response.json().get("data", [])
        except Exception as e:
            logging.error(f"Error parsing {type.value} at offset {offset}: {e}")
            logging.error(f"Response: {response.text}")
            return None, False, f"Error parsing {type.value} at offset {offset}. {e}"
        if not data:
            break
        file_data.extend(data)
        offset += limit
        if len(data) < limit:
            break

    if not file_data:
        return None, True, f"No {type.value} found."
    
    logging.debug(f"Fetched {len(file_data)} {type.value} items from API.")

    if SCAN_METADATA:
        pruneExpiredMetadataCache()
    
    files = []
    
    # Get the number of CPU cores for parallel processing
    max_workers = int(multiprocessing.cpu_count() * 2 - 1)
    if SCAN_METADATA:
        max_workers = min(max_workers, METADATA_MAX_WORKERS)
    logging.info(f"Processing files with {max_workers} parallel threads")
    
    # Collect all files to process
    files_to_process = []
    for item in file_data:
        if not item.get("cached", False):
            continue
        for file in item.get("files", []):
            files_to_process.append((item, file))
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file, item, file, type): (item, file) 
            for item, file in files_to_process
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            try:
                data = future.result()
                if data:
                    files.append(data)
            except Exception as e:
                item, file = future_to_file[future]
                logging.error(f"Error processing file {file.get('short_name', 'unknown')}: {e}")
                logging.error(traceback.format_exc())
            
    return files, True, f"{type.value.capitalize()} fetched successfully."

def searchMetadata(query: str, title_data: dict, file_name: str, full_title: str, hash: str, item_name: str, cache_key: str | None = None):
    base_metadata = {
        "metadata_title": cleanTitle(query),
        "metadata_link": None,
        "metadata_mediatype": "movie",
        "metadata_image": None,
        "metadata_backdrop": None,
        "metadata_years": None,
        "metadata_season": None,
        "metadata_episode": None,
        "metadata_filename": file_name,
        "metadata_rootfoldername": cleanTitle(item_name) if item_name else title_data.get("item_name", None),
        "metadata_foldername": None,
    }

    def cacheAndReturn(metadata: dict, success: bool, detail: str):
        if cache_key is not None:
            setCachedMetadata(cache_key, metadata, success, detail)
        return metadata, success, detail

    if not SCAN_METADATA:
        return base_metadata, False, "Metadata scanning is disabled."

    if cache_key is not None:
        cached_result = getCachedMetadata(cache_key)
        if cached_result is not None:
            cached_metadata, cached_success, cached_detail = cached_result
            logging.debug(f"Metadata cache hit for key {cache_key}")
            return cached_metadata, cached_success, f"Metadata cache hit. {cached_detail}"

    extension = os.path.splitext(file_name)[-1]
    try:
        response = requestWrapper(search_api_http_client, "GET", f"/meta/search/{full_title}", params={"type": "file"})
    except Exception as e:
        logging.error(f"Error searching metadata: {e}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata: {e}. Searching for {query}, item hash: {hash}")
    if response.status_code != 200:
        logging.error(f"Error searching metadata: {response.status_code}. {response.text}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata. {response.status_code}. Searching for {query}, item hash: {hash}")
    try:
        metadata_results = response.json().get("data", [])
        if not metadata_results:
            return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}")

        data = metadata_results[0]

        title = cleanTitle(data.get("title"))
        base_metadata["metadata_title"] = title
        base_metadata["metadata_years"] = cleanYear(title_data.get("year", None) or data.get("releaseYears", None))

        if data.get("type") == "anime" or data.get("type") == "series":
            parsed_season = title_data.get("season", None)
            parsed_episode = title_data.get("episode", None)
            normalized_season = parsed_season if parsed_season is not None else 1

            series_season_episode = constructSeriesTitle(season=parsed_season, episode=parsed_episode)
            if series_season_episode is not None:
                file_name = f"{title} {series_season_episode}{extension}"
            else:
                file_name = f"{title}{extension}"
            base_metadata["metadata_foldername"] = constructSeriesTitle(season=normalized_season, folder=True)
            base_metadata["metadata_season"] = normalized_season
            base_metadata["metadata_episode"] = parsed_episode
        elif data.get("type") == "movie":
            if base_metadata["metadata_years"] is not None:
                file_name = f"{title} ({base_metadata['metadata_years']}){extension}"
            else:
                file_name = f"{title}{extension}"
        else:
            return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}")
            
        base_metadata["metadata_filename"] = file_name
        base_metadata["metadata_mediatype"] = data.get("type")
        base_metadata["metadata_link"] = data.get("link")
        base_metadata["metadata_image"] = data.get("image")
        base_metadata["metadata_backdrop"] = data.get("backdrop")
        if base_metadata["metadata_years"] is not None:
            base_metadata["metadata_rootfoldername"] = f"{title} ({base_metadata['metadata_years']})"
        else:
            base_metadata["metadata_rootfoldername"] = title

        return cacheAndReturn(base_metadata, True, f"Metadata found. Searching for {query}, item hash: {hash}")
    except IndexError:
        return cacheAndReturn(base_metadata, False, f"No metadata found. Searching for {query}, item hash: {hash}")
    except httpx.TimeoutException:
        return cacheAndReturn(base_metadata, False, f"Timeout searching metadata. Searching for {query}, item hash: {hash}")
    except Exception as e:
        logging.error(f"Error searching metadata: {e}")
        logging.error(f"Error searching metadata: {traceback.format_exc()}")
        return cacheAndReturn(base_metadata, False, f"Error searching metadata: {e}. Searching for {query}, item hash: {hash}")

def getDownloadLink(url: str):
    response = requestWrapper(general_http_client, "GET", url)
    if response.status_code == httpx.codes.TEMPORARY_REDIRECT or response.status_code == httpx.codes.PERMANENT_REDIRECT or response.status_code == httpx.codes.FOUND:
        return response.headers.get('Location')
    return url

def downloadFile(url: str, size: int, offset: int = 0):
    headers = {
        "Range": f"bytes={offset}-{offset + size - 1}",
        **general_http_client.headers,
    }
    response = requestWrapper(general_http_client, "GET", url, headers=headers)
    if response.status_code == httpx.codes.OK:
        return response.content
    elif response.status_code == httpx.codes.PARTIAL_CONTENT:
        return response.content
    else:
        logging.error(f"Error downloading file: {response.status_code}")
        raise Exception(f"Error downloading file: {response.status_code}")
    
