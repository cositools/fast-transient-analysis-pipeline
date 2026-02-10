import json
import argparse
import os
import shutil
import gzip
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
    """Prints a heartbeat message every 60s to keep connection alive."""
    while not stop_event.is_set():
        print(f"[heartbeat] {msg} - Still working...", flush=True, file=sys.stderr)
        time.sleep(60)

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
    with gzip.open(gz_path, "rb") as gz_f, open(ready, "wb") as out_f:
        shutil.copyfileobj(gz_f, out_f)
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

def download_or_use_one(remote_or_local: str, target_dir: Path, *, kind: str) -> list[Path]:
    # Ensure target directory exists
    ensure_dir(target_dir)

    # If ready files exist, return them
    if ready_files_exist(target_dir):
        files = ready_files(target_dir)
        # If background files exist, validate them
        if kind == "background":
            # Find the background FITS file
            bg_fits = next((p for p in files if p.suffix.lower() == ".fits" or p.name.endswith(".fits")), None)
            if bg_fits and not validate_background_fits(bg_fits):
                print(f"[stage] Detected corrupted ready background in {target_dir}, cleaning and re-fetching…", file=sys.stderr)
                for p in target_dir.iterdir():
                    if p.is_file():
                        p.unlink(missing_ok=True)
            # If background files are valid, return them
            else:
                print(f"[stage] Ready files already in {target_dir}, skipping.", file=sys.stderr)
                return files
        # If not background files, return them
        else:
            print(f"[stage] Ready files already in {target_dir}, skipping.", file=sys.stderr)
            return files

    # If not ready files, download the file
    src = Path(remote_or_local)
    
    # If the file is absolute and exists, link it to the target directory
    if src.is_absolute() and src.exists():
        dst = target_dir / src.name
        # If the link does not exist, create it
        if not dst.exists():
            # Try to link the file, if it fails, copy it
            try:
                os.link(src, dst)
            except OSError:
                shutil.copy2(src, dst)
        # If the file is a gzip, gunzip it
        if dst.suffix == ".gz":
            ready = gunzip_to_same_dir(dst)
            return [ready]
        # If the file is a zip, extract it
        if dst.suffix == ".zip":
            try:
                with zipfile.ZipFile(dst, "r") as zf:
                    zf.testzip()
                    zf.extractall(target_dir)
                return ready_files(target_dir)
            except BadZipFile as e:
                raise RuntimeError(f"[stage] Local .zip seems corrupted: {dst} ({e})")
        if kind == "background" and dst.suffix.lower() == ".fits" and not validate_background_fits(dst):
            raise RuntimeError(f"[stage] Local background FITS seems corrupted: {dst}")
        return [dst]

    out_path = target_dir / src.name

    if not out_path.exists():
        fetch_wasabi(remote_or_local, out_path)

    if out_path.suffix == ".gz":
        try:
            ready = gunzip_to_same_dir(out_path)
        except Exception:
            print(f"[stage] Corrupted .gz at {out_path}, re-downloading…", file=sys.stderr)
            redownload(remote_or_local, out_path)
            ready = gunzip_to_same_dir(out_path)
        if kind == "background" and not validate_background_fits(ready):
            print(f"[stage] Re-downloading background FITS after failed validation…", file=sys.stderr)
            redownload(remote_or_local, out_path)
            ready = gunzip_to_same_dir(out_path)
            if not validate_background_fits(ready):
                raise RuntimeError(f"[stage] Background FITS still invalid after re-download: {ready}")
        return [ready]

    if out_path.suffix == ".zip":
        try:
            with zipfile.ZipFile(out_path, "r") as zf:
                zf.testzip()
                zf.extractall(target_dir)
        except BadZipFile:
            print(f"[stage] Corrupted .zip at {out_path}, re-downloading…", file=sys.stderr)
            redownload(remote_or_local, out_path)
            with zipfile.ZipFile(out_path, "r") as zf:
                zf.testzip()
                zf.extractall(target_dir)
        files = ready_files(target_dir)
        return files

    if kind == "background" and out_path.suffix.lower() == ".fits" and not validate_background_fits(out_path):
        print(f"[stage] Background FITS invalid at {out_path}, re-downloading…", file=sys.stderr)
        redownload(remote_or_local, out_path)
        if not validate_background_fits(out_path):
            raise RuntimeError(f"[stage] Background FITS still invalid after re-download: {out_path}")
    return [out_path]

def main():
    parser = argparse.ArgumentParser(description="Stage files from Wasabi or local path")
    parser.add_argument("--inputs", type=str, required=True, help="JSON string of inputs")
    parser.add_argument("--dirs", type=str, required=True, help="JSON string of directories")
    args = parser.parse_args()

    inputs = json.loads(args.inputs)
    dirs = json.loads(args.dirs)

    staged = {
        "response":   [str(p) for p in download_or_use_one(inputs["response"],   Path(dirs["response"]),   kind="response")],
        "orientation":[str(p) for p in download_or_use_one(inputs["orientation"],Path(dirs["orientation"]),kind="orientation")],
        "source":     [str(p) for p in download_or_use_one(inputs["source"],     Path(dirs["source"]),     kind="source")],
        "background": [str(p) for p in download_or_use_one(inputs["background"], Path(dirs["background"]), kind="background")],
    }
    
    # Print JSON result to stdout for XCom
    print(json.dumps(staged))

if __name__ == "__main__":
    main()
