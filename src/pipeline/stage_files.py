import json
import argparse
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

DEFAULT_WASABI_BASE = "COSI-SMEX/DC3/Data"

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

def normalize_wasabi_key(remote_key: str, wasabi_base: str) -> str:
    remote_key = remote_key.strip().lstrip("/")
    if remote_key.startswith("COSI-SMEX/"):
        return remote_key
    return f"{wasabi_base.rstrip('/')}/{remote_key}"

def compressed_ready_path(target_file: Path) -> Path:
    if target_file.suffix in (".gz", ".zip"):
        return target_file.with_suffix("")
    return target_file

def unzip_to_same_dir(zip_path: Path) -> Path:
    ready = zip_path.with_suffix("")
    if ready.exists():
        return ready
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            bad_file = zf.testzip()
            if bad_file:
                raise RuntimeError(f"[stage] Corrupted file inside zip: {bad_file}")
            members = [info for info in zf.infolist() if not info.is_dir()]
            zf.extractall(zip_path.parent)
    except BadZipFile as e:
        raise RuntimeError(f"[stage] Local .zip seems corrupted: {zip_path} ({e})")

    if len(members) == 1:
        return zip_path.parent / members[0].filename
    return ready if ready.exists() else zip_path

def fetch_wasabi(remote_key: str, out_path: Path, wasabi_base: str = DEFAULT_WASABI_BASE) -> None:
    ensure_dir(out_path.parent)
    wasabi_key = normalize_wasabi_key(remote_key, wasabi_base)
    print(f"[stage] Starting download of {wasabi_key}...", flush=True, file=sys.stderr)
    
    stop_event = threading.Event()
    t = threading.Thread(target=heartbeat, args=(stop_event, wasabi_key))
    t.start()
    
    try:
        fetch_wasabi_file(wasabi_key, output=out_path)
    finally:
        stop_event.set()
        t.join()
        print(f"[stage] Finished download of {wasabi_key}", flush=True, file=sys.stderr)

def redownload(remote_key: str, out_path: Path, wasabi_base: str = DEFAULT_WASABI_BASE) -> None:
    try:
        out_path.unlink(missing_ok=True)
    except Exception:
        pass
    fetch_wasabi(remote_key, out_path, wasabi_base)

def download_or_use_one(
    remote_or_local: str,
    target_dir: Path,
    *,
    kind: str,
    wasabi_base: str = DEFAULT_WASABI_BASE,
) -> str:
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

    local_path = Path(remote_or_local).expanduser()
    target_file = target_dir / Path(remote_or_local).name
    ready_file = compressed_ready_path(target_file)

    if ready_file.exists():
        return str(ready_file)

    if local_path.exists():
        if local_path.resolve() != target_file.resolve():
            shutil.copy2(local_path, target_file)
    elif not target_file.exists():
        fetch_wasabi(remote_or_local, target_file, wasabi_base)

    # If the file is zipped, unzip it
    if target_file.suffix == ".gz":
        ready = gunzip_to_same_dir(target_file)
        return str(ready)
    # If the file is a zip, extract it
    if target_file.suffix == ".zip":
        ready = unzip_to_same_dir(target_file)
        return str(ready)
    if kind == "background" and target_file.suffix.lower() == ".fits" and not validate_background_fits(target_file):
        raise RuntimeError(f"[stage] Local background FITS seems corrupted: {target_file}")
    return str(target_file)


def main():
    parser = argparse.ArgumentParser(description="Stage files from Wasabi or local path")
    parser.add_argument("--inputs", type=str, required=True, help="JSON string of inputs")
    parser.add_argument("--dirs", type=str, required=True, help="JSON string of directories")
    parser.add_argument("--wasabi-base", type=str, default=DEFAULT_WASABI_BASE, help="Base Wasabi key for relative inputs")
    args = parser.parse_args()

    inputs = json.loads(args.inputs)
    dirs = json.loads(args.dirs)
    wasabi_base = args.wasabi_base

    staged = {
        "response": download_or_use_one(
            inputs["response"], Path(dirs["response"]), kind="response", wasabi_base=wasabi_base
        ),
        "orientation": download_or_use_one(
            inputs["orientation"], Path(dirs["orientation"]), kind="orientation", wasabi_base=wasabi_base
        ),
        "source": download_or_use_one(
            inputs["source"], Path(dirs["source"]), kind="source", wasabi_base=wasabi_base
        ),
        "background": download_or_use_one(
            inputs["background"], Path(dirs["background"]), kind="background", wasabi_base=wasabi_base
        ),
    }
    
    # Print JSON result to stdout for XCom
    print(json.dumps(staged))

if __name__ == "__main__":
    main()
