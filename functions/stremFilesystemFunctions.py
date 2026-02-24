import os
import glob
import logging
from library.app import RAW_MODE
from library.filesystem import MOUNT_PATH
from functions.appFunctions import getAllUserDownloads

def getMountCategory(media_type: str | None):
    if media_type == "movie":
        return "movies"
    if media_type == "music":
        return "music"
    if media_type == "series" or media_type == "anime":
        return "series"
    return None

def generateFolderPath(data: dict) -> str | None:
    """
    Takes in a user download and returns the folder path for the download.
    """
    
    if RAW_MODE:
        original_path = data.get("path")
        if original_path:
            return os.path.dirname(original_path)
        return None
    else:
        root_folder: str | None = data.get("metadata_rootfoldername", None)
        metadata_foldername: str | None = data.get("metadata_foldername", None)
        media_type = data.get("metadata_mediatype")

        if not root_folder:
            return None

        if media_type == "series" or media_type == "anime":
            if not metadata_foldername:
                return None
            return os.path.join(
                root_folder,
                metadata_foldername,
            )

        if media_type == "movie" or media_type == "music":
            return os.path.join(root_folder)

        return None

def generateStremFile(file_path: str, url: str, type: str, file_name: str, download=None):
    if RAW_MODE:
        if download is None:
            return False
        original_path = download.get("path")
        if not original_path:
            return False
        full_path = os.path.join(MOUNT_PATH, os.path.dirname(original_path))
    else:
        mount_category = getMountCategory(type)
        if mount_category is None:
            return False
        full_path = os.path.join(MOUNT_PATH, mount_category, file_path)
    try:
        os.makedirs(full_path, exist_ok=True)
        with open(f"{full_path}/{file_name}.strm", "w") as file:
            file.write(url)
        logging.debug(f"Created strm file: {full_path}/{file_name}.strm")
        return True
    except FileNotFoundError as e:
        logging.error(f"Error creating strm file (likely bad naming scheme of file): {e}")
        return False
    except OSError as e:
        logging.error(f"Error creating strm file (likely bad or missing permissions): {e}")
        return False
    except Exception as e:
        logging.error(f"Error creating strm file: {e}")
        return False

def runStrm():
    all_downloads = getAllUserDownloads()
    # Get all existing .strm files
    existing_strm_files = set(glob.glob(os.path.join(MOUNT_PATH, "**", "*.strm"), recursive=True))

    new_strm_files = set()
    for download in all_downloads:
        file_path = generateFolderPath(download)
        if file_path is None:
            continue
        if RAW_MODE:
            strm_path = os.path.join(MOUNT_PATH, file_path, f"{download.get('metadata_filename')}.strm")
        else:
            mount_category = getMountCategory(download.get("metadata_mediatype"))
            if mount_category is None:
                continue
            strm_path = os.path.join(MOUNT_PATH, mount_category, file_path, f"{download.get('metadata_filename')}.strm")
        new_strm_files.add(strm_path)
        generateStremFile(file_path, download.get("download_link"), download.get("metadata_mediatype"), download.get("metadata_filename"), download)

    # Remove .strm files for deleted downloads
    for strm_file in existing_strm_files:
        if strm_file not in new_strm_files:
            try:
                os.remove(strm_file)
                logging.debug(f"Removed stale .strm file: {strm_file}")
                # Remove empty directories
                dir = os.path.dirname(strm_file)
                while dir != MOUNT_PATH and not os.listdir(dir):
                    os.rmdir(dir)
                    dir = os.path.dirname(dir)
            except Exception as e:
                logging.error(f"Error removing .strm file: {e}")

    logging.debug(f"Updated {len(all_downloads)} strm files.")

def unmountStrm():
    """
    Deletes all strm files and any subfolders in the mount path for cleaning up.
    """
    folders = [
        MOUNT_PATH,
        os.path.join(MOUNT_PATH, "movies"),
        os.path.join(MOUNT_PATH, "series"),
        os.path.join(MOUNT_PATH, "music"),
    ]
    for folder in folders:
        if os.path.exists(folder):
            logging.debug(f"Folder {folder} already exists. Deleting...")
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                if os.path.isdir(item_path):
                    import shutil
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
