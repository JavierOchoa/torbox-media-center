from library.app import RAW_MODE
from functions.torboxFunctions import getUserDownloads, DownloadType
from library.filesystem import MOUNT_METHOD, MOUNT_PATH
from library.app import MOUNT_REFRESH_TIME
from library.torbox import TORBOX_API_KEY
from functions.databaseFunctions import getAllData, clearDatabase
import logging
import os
import shutil
from library.app import getCurrentVersion
import git

def initializeFolders():
    folders = [MOUNT_PATH]
    if not RAW_MODE:
        folders.extend([
            os.path.join(MOUNT_PATH, "movies"),
            os.path.join(MOUNT_PATH, "series"),
        ])
    for folder in folders:
        if os.path.exists(folder):
            logging.debug(f"Folder {folder} already exists. Deleting...")
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
        else:
            logging.debug(f"Creating folder {folder}...")
            os.makedirs(folder, exist_ok=True)

def getAllUserDownloadsFresh():
    all_downloads = []
    logging.info("Fetching all user downloads...")
    for download_type in DownloadType:
        logging.debug(f"Clearing database for {download_type.value}...")
        success, detail = clearDatabase(download_type.value)
        if not success:
            logging.error(f"Error clearing {download_type.value} database: {detail}")
            continue
        logging.debug(f"Fetching {download_type.value} downloads...")
        downloads, success, detail = getUserDownloads(download_type)
        if not success:
            logging.error(f"Error fetching {download_type.value}: {detail}")
            continue
        if not downloads:
            logging.info(f"No {download_type.value} downloads found.")
            continue
        all_downloads.extend(downloads)
        logging.debug(f"Fetched {len(downloads)} {download_type.value} downloads.")
    return all_downloads

def getAllUserDownloads():
    all_downloads = []
    for download_type in DownloadType:
        logging.debug(f"Fetching {download_type.value} downloads...")
        downloads, success, detail = getAllData(download_type.value)
        if not success:
            logging.error(f"Error fetching {download_type.value}: {detail}")
            continue
        all_downloads.extend(downloads)
        logging.debug(f"Fetched {len(downloads)} {download_type.value} downloads.")
    return all_downloads

def bootUp():
    logging.debug("Booting up...")
    logging.info("Mount method: %s", MOUNT_METHOD)
    logging.info("Mount path: %s", MOUNT_PATH)
    logging.info("TorBox API Key: %s", TORBOX_API_KEY)
    logging.info("Mount refresh time: %s %s", MOUNT_REFRESH_TIME, "hours")

    # check version
    latest_version = getLatestVersion()
    current_version = getCurrentVersion()

    if latest_version != current_version:
        logging.warning(f"!!! A new version of TorBox Media Center is available: {latest_version}. You are running version: {current_version}. Please consider updating to the latest version. !!!")

    initializeFolders()

    return True

def getMountMethod():
    return MOUNT_METHOD

def getMountPath():
    return MOUNT_PATH

def getMountRefreshTime():
    return MOUNT_REFRESH_TIME

def getLatestVersion():
    try:
        url = "https://github.com/torbox-app/torbox-media-center.git"
        g = git.cmd.Git()
        tags_output = g.ls_remote("--tags", url)
        tags = [line.split("refs/tags/")[1] for line in tags_output.splitlines() if "refs/tags/" in line]
        tags = [tag for tag in tags if not tag.endswith("^{}")]
        tags.sort(key=lambda s: list(map(int, s.lstrip('v').split('.'))))
        latest_tag = tags[-1] if tags else None
        return latest_tag
    except Exception as e:
        logging.error(f"Error fetching latest version: {e}")
        return None

