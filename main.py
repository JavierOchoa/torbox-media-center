from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from functions.appFunctions import bootUp, getMountMethod, getMountRefreshTime, runRefreshCycle
from functions.databaseFunctions import closeAllDatabases
import atexit
import logging
import os
import signal
import threading
from sys import platform

PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".torbox-media-center.pid")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logging.getLogger("httpx").setLevel(logging.WARNING)

def writePidFile():
    try:
        with open(PID_FILE, "w") as pid_file:
            pid_file.write(str(os.getpid()))
    except OSError as e:
        logging.warning(f"Unable to write PID file: {e}")

def removePidFile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except OSError as e:
        logging.warning(f"Unable to remove PID file: {e}")

def runManualRefresh(mount_method: str):
    logging.info("Received manual refresh signal.")
    success, detail = runRefreshCycle(
        mount_method=mount_method,
        include_mount_sync=True,
        trigger="manual",
    )
    if success:
        logging.info("Manual refresh request completed.")
    else:
        logging.warning(f"Manual refresh request was not completed: {detail}")

def handleManualRefreshSignal(_, __, mount_method: str):
    threading.Thread(target=runManualRefresh, args=(mount_method,), daemon=True).start()

if __name__ == "__main__":
    bootUp()
    mount_method = getMountMethod()

    if mount_method == "strm":
        scheduler = BlockingScheduler()
    elif mount_method == "fuse":
        if platform == "win32":
            logging.error("The FUSE mount method is not supported on Windows. Please use the STRM mount method or run this application on a Linux system.")
            exit(1)
        scheduler = BackgroundScheduler()
    else:
        logging.error("Invalid mount method specified.")
        exit(1)

    writePidFile()
    atexit.register(removePidFile)

    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, lambda signum, frame: handleManualRefreshSignal(signum, frame, mount_method))
    else:
        logging.warning("Manual refresh signal is not supported on this platform.")

    runRefreshCycle(
        mount_method=mount_method,
        include_mount_sync=False,
        trigger="startup",
    )

    scheduler.add_job(
        runRefreshCycle,
        "interval",
        hours=getMountRefreshTime(),
        kwargs={
            "mount_method": mount_method,
            "include_mount_sync": False,
            "trigger": "scheduled",
        },
        id="get_all_user_downloads_fresh",
    )

    try:
        logging.info("Starting scheduler and mounting...")
        if mount_method == "strm":
            from functions.stremFilesystemFunctions import runStrm
            runStrm()
            scheduler.add_job(
                runStrm,
                "interval",
                minutes=5,
                id="run_strm",
            )
            scheduler.start()
        elif mount_method == "fuse":
            from functions.fuseFilesystemFunctions import runFuse
            scheduler.start()
            runFuse()
    except (KeyboardInterrupt, SystemExit):
        if mount_method == "fuse":
            from functions.fuseFilesystemFunctions import unmountFuse
            unmountFuse()
        elif mount_method == "strm":
            from functions.stremFilesystemFunctions import unmountStrm
            unmountStrm()
        closeAllDatabases()
        exit(0)
