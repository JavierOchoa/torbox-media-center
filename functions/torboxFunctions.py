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
import re
from difflib import SequenceMatcher

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
METADATA_CACHE_SCHEMA_VERSION = 2
METADATA_CACHE_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days
METADATA_FAILURE_CACHE_TTL_SECONDS = 60 * 60 * 6  # 6 hours
METADATA_MAX_WORKERS = 2
METADATA_IDENTITY_CACHE_PREFIX = "metadata_identity"
METADATA_MIN_SCORE = 35.0

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

def normalizeTitle(value: str | None):
    if not value:
        return ""

    normalized = cleanTitle(str(value)).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized

def containsSpecialKeyword(value: str | None):
    normalized = normalizeTitle(value)
    return re.search(r"\b(special|specials|extra|extras|bonus|ova|oav)\b", normalized) is not None

def parseSeasonEpisodeFromText(text: str | None):
    if not text:
        return None, None

    match = re.search(r"\bs(\d{1,2})[ ._-]*e(\d{1,3})\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = re.search(r"\b(\d{1,2})x(\d{1,3})\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = re.search(r"\bseason[ ._-]*(\d{1,2})[ ._-]*(?:episode|ep)?[ ._-]*(\d{1,3})\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    match = re.search(r"\bseason[ ._-]*(\d{1,2})\b", text, re.IGNORECASE)
    if match:
        return int(match.group(1)), None

    return None, None

def getParsedSeasonEpisode(title_data: dict, file_name: str, file_path: str | None) -> tuple[int | None, int | None, bool]:
    raw_season = title_data.get("season")
    raw_episode = title_data.get("episode")

    parsed_season: int | None = None
    parsed_episode: int | None = None

    if isinstance(raw_season, list) and raw_season:
        if isinstance(raw_season[0], int):
            parsed_season = raw_season[0]
    elif isinstance(raw_season, int):
        parsed_season = raw_season

    if isinstance(raw_episode, list) and raw_episode:
        if isinstance(raw_episode[0], int):
            parsed_episode = raw_episode[0]
    elif isinstance(raw_episode, int):
        parsed_episode = raw_episode

    fallback_season, fallback_episode = parseSeasonEpisodeFromText(file_name)

    if parsed_season is None and fallback_season is not None:
        parsed_season = fallback_season
    if parsed_episode is None and fallback_episode is not None:
        parsed_episode = fallback_episode

    if parsed_season is None:
        path_season, _ = parseSeasonEpisodeFromText(file_path)
        if path_season is not None:
            parsed_season = path_season

    is_special_request = parsed_season == 0

    if not is_special_request and (containsSpecialKeyword(file_name) or containsSpecialKeyword(file_path)):
        is_special_request = True
        if parsed_season is None:
            parsed_season = 0

    return parsed_season, parsed_episode, is_special_request

def getIdentityCacheKey(download_type: DownloadType, item_hash: str | None, item_id: int | None):
    if item_hash:
        identity_value = item_hash
    elif item_id is not None:
        identity_value = str(item_id)
    else:
        return None

    return f"{METADATA_IDENTITY_CACHE_PREFIX}:item:{download_type.value}:{identity_value}"

def getSeriesIdentityCacheKeys(title: str | None, year: str | int | None):
    normalized_title = normalizeTitle(title)
    if not normalized_title:
        return []

    keys = [f"{METADATA_IDENTITY_CACHE_PREFIX}:series:{normalized_title}"]
    cleaned_year = cleanYear(year)
    if cleaned_year is not None:
        keys.insert(0, f"{METADATA_IDENTITY_CACHE_PREFIX}:series:{normalized_title}:{cleaned_year}")

    return keys

def getCachedIdentity(cache_key: str | None):
    if cache_key is None:
        return None

    cached = getCachedMetadata(cache_key)
    if cached is None:
        return None

    cached_metadata, cached_success, _ = cached
    if not cached_success or not isinstance(cached_metadata, dict):
        return None

    return cached_metadata

def scoreMetadataCandidate(candidate: dict, normalized_query: str, query_year: int | None, expects_series: bool, is_special_request: bool):
    candidate_title = candidate.get("title")
    candidate_type = candidate.get("type")
    normalized_candidate = normalizeTitle(candidate_title)

    if not normalized_candidate:
        return -100.0

    similarity_score = SequenceMatcher(None, normalized_query, normalized_candidate).ratio() if normalized_query else 0.0

    query_tokens = set(normalized_query.split())
    candidate_tokens = set(normalized_candidate.split())
    token_overlap = 0.0
    if query_tokens:
        token_overlap = len(query_tokens.intersection(candidate_tokens)) / len(query_tokens)

    score = (similarity_score * 70.0) + (token_overlap * 30.0)

    if expects_series:
        if candidate_type in ("series", "anime"):
            score += 25.0
        else:
            score -= 30.0
    elif candidate_type == "movie":
        score += 10.0

    candidate_year = cleanYear(candidate.get("releaseYears"))
    if query_year is not None and candidate_year is not None:
        if query_year == candidate_year:
            score += 10.0
        elif abs(query_year - candidate_year) <= 1:
            score += 5.0
        else:
            score -= 8.0

    candidate_is_special = containsSpecialKeyword(candidate_title)
    if is_special_request:
        if candidate_is_special:
            score += 12.0
    else:
        if candidate_is_special:
            score -= 18.0

    if normalized_query and normalized_query == normalized_candidate:
        score += 10.0
    elif normalized_query and normalized_query in normalized_candidate:
        score += 5.0

    if candidate_type not in ("movie", "series", "anime"):
        score -= 20.0

    return score

def selectBestMetadataCandidate(metadata_results: list[dict], normalized_query: str, query_year: int | None, expects_series: bool, is_special_request: bool):
    best_candidate = None
    best_score = float("-inf")

    for candidate in metadata_results:
        score = scoreMetadataCandidate(
            candidate,
            normalized_query=normalized_query,
            query_year=query_year,
            expects_series=expects_series,
            is_special_request=is_special_request,
        )

        if score > best_score:
            best_score = score
            best_candidate = candidate

    return best_candidate, best_score

def buildIdentityMetadata(candidate: dict, title_data: dict, item_name: str | None):
    metadata_title = cleanTitle(candidate.get("title") or title_data.get("title") or item_name or "Unknown")
    metadata_year = cleanYear(title_data.get("year") or candidate.get("releaseYears"))
    metadata_type = candidate.get("type")

    if metadata_type not in ("movie", "series", "anime"):
        metadata_type = "movie"

    metadata_rootfoldername = metadata_title
    if metadata_year is not None:
        metadata_rootfoldername = f"{metadata_title} ({metadata_year})"

    return {
        "metadata_title": metadata_title,
        "metadata_link": candidate.get("link"),
        "metadata_mediatype": metadata_type,
        "metadata_image": candidate.get("image"),
        "metadata_backdrop": candidate.get("backdrop"),
        "metadata_years": metadata_year,
        "metadata_rootfoldername": metadata_rootfoldername,
    }

def buildMetadataFromIdentity(identity_metadata: dict, base_metadata: dict, extension: str, parsed_season: int | None, parsed_episode: int | None, is_special_request: bool):
    metadata = dict(base_metadata)
    metadata.update(identity_metadata)

    media_type = metadata.get("metadata_mediatype")
    metadata_title = metadata.get("metadata_title") or base_metadata.get("metadata_title")

    if media_type in ("series", "anime"):
        normalized_season = parsed_season
        if normalized_season is None:
            normalized_season = 0 if is_special_request else 1

        if normalized_season == 0:
            metadata_foldername = "Specials"
        else:
            metadata_foldername = constructSeriesTitle(season=normalized_season, folder=True)

        series_identifier = constructSeriesTitle(season=normalized_season, episode=parsed_episode)
        if series_identifier:
            metadata_filename = f"{metadata_title} {series_identifier}{extension}"
        else:
            metadata_filename = f"{metadata_title}{extension}"

        metadata["metadata_foldername"] = metadata_foldername
        metadata["metadata_season"] = normalized_season
        metadata["metadata_episode"] = parsed_episode
        metadata["metadata_filename"] = metadata_filename
    elif media_type == "movie":
        metadata_year = metadata.get("metadata_years")
        if metadata_year is not None:
            metadata["metadata_filename"] = f"{metadata_title} ({metadata_year}){extension}"
        else:
            metadata["metadata_filename"] = f"{metadata_title}{extension}"
        metadata["metadata_foldername"] = None
        metadata["metadata_season"] = None
        metadata["metadata_episode"] = None

    return metadata

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

    parsed_season, parsed_episode, is_special_request = getParsedSeasonEpisode(
        title_data,
        short_name,
        file.get("name"),
    )

    item_identity_cache_key = getIdentityCacheKey(type, item.get("hash"), item.get("id"))
    expects_series_hint = parsed_season is not None or parsed_episode is not None
    series_identity_cache_keys = []
    if expects_series_hint:
        series_identity_cache_keys = getSeriesIdentityCacheKeys(
            title_data.get("title") or item_name,
            title_data.get("year"),
        )

    cache_key = getMetadataCacheKey(type, item, file) if SCAN_METADATA else None
    metadata, _, _ = searchMetadata(
        title_data.get("title", short_name),
        title_data,
        short_name,
        f"{item_name} {short_name}",
        item.get("hash"),
        item_name,
        cache_key=cache_key,
        parsed_season=parsed_season,
        parsed_episode=parsed_episode,
        is_special_request=is_special_request,
        item_identity_cache_key=item_identity_cache_key,
        series_identity_cache_keys=series_identity_cache_keys,
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

def searchMetadata(
    query: str,
    title_data: dict,
    file_name: str,
    full_title: str,
    hash: str,
    item_name: str,
    cache_key: str | None = None,
    parsed_season: int | None = None,
    parsed_episode: int | None = None,
    is_special_request: bool = False,
    item_identity_cache_key: str | None = None,
    series_identity_cache_keys: list[str] | None = None,
):
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

    identity_cache_keys = []
    if item_identity_cache_key is not None:
        identity_cache_keys.append(item_identity_cache_key)
    if series_identity_cache_keys:
        identity_cache_keys.extend(series_identity_cache_keys)

    for identity_cache_key in identity_cache_keys:
        cached_identity = getCachedIdentity(identity_cache_key)
        if cached_identity is None:
            continue

        metadata_from_identity = buildMetadataFromIdentity(
            cached_identity,
            base_metadata=base_metadata,
            extension=extension,
            parsed_season=parsed_season,
            parsed_episode=parsed_episode,
            is_special_request=is_special_request,
        )
        return cacheAndReturn(metadata_from_identity, True, f"Metadata identity cache hit for key {identity_cache_key}")

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

        normalized_query = normalizeTitle(query) or normalizeTitle(item_name) or normalizeTitle(full_title)
        query_year = cleanYear(title_data.get("year"))
        expects_series = parsed_season is not None or parsed_episode is not None

        selected_candidate, selected_score = selectBestMetadataCandidate(
            metadata_results,
            normalized_query=normalized_query,
            query_year=query_year,
            expects_series=expects_series,
            is_special_request=is_special_request,
        )

        if selected_candidate is None or selected_score < METADATA_MIN_SCORE:
            return cacheAndReturn(
                base_metadata,
                False,
                f"No confident metadata found. Best score {selected_score:.2f}. Searching for {query}, item hash: {hash}",
            )

        if expects_series and selected_candidate.get("type") not in ("series", "anime"):
            return cacheAndReturn(
                base_metadata,
                False,
                f"Series metadata could not be confidently matched. Best type: {selected_candidate.get('type')}. Searching for {query}, item hash: {hash}",
            )

        identity_metadata = buildIdentityMetadata(
            selected_candidate,
            title_data=title_data,
            item_name=item_name,
        )

        metadata = buildMetadataFromIdentity(
            identity_metadata,
            base_metadata=base_metadata,
            extension=extension,
            parsed_season=parsed_season,
            parsed_episode=parsed_episode,
            is_special_request=is_special_request,
        )

        if item_identity_cache_key is not None:
            setCachedMetadata(item_identity_cache_key, identity_metadata, True, "Item metadata identity cached.")

        if series_identity_cache_keys and identity_metadata.get("metadata_mediatype") in ("series", "anime"):
            for identity_cache_key in set(series_identity_cache_keys):
                setCachedMetadata(identity_cache_key, identity_metadata, True, "Series metadata identity cached.")

        return cacheAndReturn(
            metadata,
            True,
            f"Metadata found with score {selected_score:.2f}. Searching for {query}, item hash: {hash}",
        )
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
    
