#!/usr/bin/env python3
"""Minimal diagnostic to test tar stream reading."""
import sys
import tarfile
import csv
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from tagminder.core.tm_config import resolve_config_path

def main():
    print("Starting tar read diagnostic...", flush=True)
    
    # Load config
    config_path = resolve_config_path()
    print(f"Config: {config_path}", flush=True)
    
    config = __import__("tomllib" if sys.version_info >= (3, 11) else "tomli").loads(
        config_path.read_text()
    )
    tar_path = Path(config["musicbrainz"]["dump_archive"])
    print(f"Tar path: {tar_path}", flush=True)
    print(f"Tar exists: {tar_path.exists()}", flush=True)
    
    # Open tar
    print("Opening tar...", flush=True)
    t0 = time.time()
    tar = tarfile.open(tar_path, "r|bz2")
    print(f"Tar opened in {time.time() - t0:.2f}s", flush=True)
    
    # Find link_type member
    print("Searching for mbdump/link_type...", flush=True)
    t0 = time.time()
    link_type_member = None
    for member in tar:
        if member.name == "mbdump/link_type":
            link_type_member = member
            break
        if member.name.startswith("mbdump/link_type"):
            print(f"  Found: {member.name}", flush=True)
    print(f"Search took {time.time() - t0:.2f}s", flush=True)
    
    if not link_type_member:
        print("ERROR: mbdump/link_type not found!", flush=True)
        tar.close()
        return
    
    # Extract and read
    print("Extracting and reading link_type...", flush=True)
    t0 = time.time()
    f = tar.extractfile(link_type_member)
    if not f:
        print("ERROR: Could not extract file!", flush=True)
        tar.close()
        return
    
    row_count = 0
    try:
        for row in csv.reader(f, delimiter="\t"):
            row_count += 1
            if row_count % 100 == 0:
                print(f"  {row_count} rows...", flush=True)
            if row_count >= 200:  # Just read first 200 rows for testing
                break
    except Exception as e:
        print(f"ERROR reading: {e}", flush=True)
    
    elapsed = time.time() - t0
    print(f"Read {row_count} rows in {elapsed:.2f}s", flush=True)
    
    tar.close()
    print("Done!", flush=True)

if __name__ == "__main__":
    main()
