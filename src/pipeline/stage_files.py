import json
import argparse
import os
import shutil
#import gzip
import subprocess
import zipfile
import threading
import time
import sys
from zipfile import BadZipFile
from pathlib import Path
from cosipy.util import fetch_wasabi_file

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def heartbeat(stop_event, msg):
    """Prints a heartbeat message every 15s to keep connection alive."""
    while not stop_event.is_set():
        print(f"[heartbeat] {msg} - Still working...", flush=True, file=sys.stderr)
        time.sleep(15)

def ready_files(target_dir: Path) -> list[Path]:
    return [p for p in target_dir.iterdir() if p.is_file() and not p.name.endswith((".zip", ".gz"))]

def ready_files_exist(target_dir: Path) -> bool:
    return len(ready_files(target_dir)) > 0

def validate_background_fits(f: Path) -> bool:
    """Tries to open HDU[1] to catch truncated files."""
    try:
        from astropy.io import fits
        with fits.open(f, memmap=True) as hdul:
            _ = hdul[1].data.shape
        return True
    except Exception as e:
        print(f"[stage] Background FITS validation failed: {f} ({e})", file=sys.stderr)
        return False

def gunzip_to_same_dir(gz_path: Path) -> Path:
    ready = gz_path.with_suffix("")
    if ready.exists():
        return ready
    # This is the old way to gunzip the file
    #with gzip.open(gz_path, "rb") as gz_f, open(ready, "wb") as out_f:
    #    shutil.copyfileobj(gz_f, out_f)
    # This is the new way to gunzip the file, and wait until the file is ready
    subprocess.run(["gunzip", "-f", str(gz_path)], check=True)
    while not ready.exists():
        time.sleep(1)
    # Remove the gzip file
    gz_path.unlink(missing_ok=True)
    return ready

def fetch_wasabi(remote_key: str, out_path: Path) -> None:
    ensure_dir(out_path.parent)
    print(f"[stage] Starting download of {remote_key}...", flush=True, file=sys.stderr)
    
    stop_event = threading.Event()
    t = threading.Thread(target=heartbeat, args=(stop_event, remote_key))
    t.start()
    
    try:
        fetch_wasabi_file(f"COSI-SMEX/DC3/Data/{remote_key}", output=out_path)
    finally:
        stop_event.set()
        t.join()
        print(f"[stage] Finished download of {remote_key}", flush=True, file=sys.stderr)

def redownload(remote_key: str, out_path: Path) -> None:
    try:
        out_path.unlink(missing_ok=True)
    except Exception:
        pass
    fetch_wasabi(remote_key, out_path)

def download_or_use_one(remote_or_local: str, target_dir: Path, *, kind: str) -> str:
    """
    Downloads or uses a file from Wasabi or local path.
    Returns the path to the file that was downloaded or used.
    Parameters:
        remote_or_local: The path to the file to download or use.
        target_dir: The directory to download the file to.
        kind: The kind of file to download or use.
    Returns:
        The path to the file that was downloaded or used.
    """
    # Ensure target directory exists
    ensure_dir(target_dir)

    # print(f"[stage] Remote or local path: {remote_or_local}")
    file_name = Path(remote_or_local).name
    # check how many extensions are in the file name
    extensions = file_name.split(".")[1:]
    if len(extensions) > 1:
        # remove the extensions from the file name
        file_name = ".".join(file_name.split(".")[:-1])
        # print(f"[stage] The file is zipped, filename is: {file_name}")
    # If ready files exist, return them
    if os.path.exists(target_dir / file_name):
        # print(f"[stage] Ready file {file_name} already in {target_dir}, skipping.", file=sys.stderr)
        return str(target_dir / file_name)

    # If not ready file exists, download the file
    target_file = target_dir / file_name
    fetch_wasabi(remote_or_local, target_file)
    
    # If the file is zipped, unzip it
    if target_file.suffix == ".gz":
        ready = gunzip_to_same_dir(target_file)
        return str(ready)
    # If the file is a zip, extract it
    if target_file.suffix == ".zip":
        try:
            with zipfile.ZipFile(target_file, "r") as zf:
                zf.testzip()
                zf.extractall(target_dir)
            return str(target_file)
        except BadZipFile as e:
            raise RuntimeError(f"[stage] Local .zip seems corrupted: {target_file} ({e})")
    if kind == "background" and target_file.suffix.lower() == ".fits" and not validate_background_fits(target_file):
        raise RuntimeError(f"[stage] Local background FITS seems corrupted: {dst}")
    return str(target_file)


def main():
    parser = argparse.ArgumentParser(description="Stage files from Wasabi or local path")
    parser.add_argument("--inputs", type=str, required=True, help="JSON string of inputs")
    parser.add_argument("--dirs", type=str, required=True, help="JSON string of directories")
    args = parser.parse_args()

    inputs = json.loads(args.inputs)
    dirs = json.loads(args.dirs)

    staged = {
        "response":   download_or_use_one(inputs["response"],   Path(dirs["response"]),   kind="response"),
        "orientation":download_or_use_one(inputs["orientation"],Path(dirs["orientation"]),kind="orientation"),
        "source":     download_or_use_one(inputs["source"],     Path(dirs["source"]),     kind="source"),
        "background": download_or_use_one(inputs["background"], Path(dirs["background"]), kind="background"),
    }
    
    # Print JSON result to stdout for XCom
    print(json.dumps(staged))

if __name__ == "__main__":
    main()
