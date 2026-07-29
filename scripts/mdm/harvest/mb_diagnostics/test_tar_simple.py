#!/usr/bin/env python3
"""Minimal test of tar stream reading."""
import tarfile
import csv
import time
from pathlib import Path

tar_path = Path("/mnt/usbc1/MUSIC_MASTER_METADATA/mbdump.tar.bz2")
print(f"Tar exists: {tar_path.exists()}", flush=True)

print("Opening tar...", flush=True)
t0 = time.time()
try:
    tar = tarfile.open(tar_path, "r|bz2")
    print(f"Tar opened in {time.time() - t0:.2f}s", flush=True)
except Exception as e:
    print(f"ERROR opening tar: {e}", flush=True)
    exit(1)

# Stream through and find link_type
print("Searching for link_type...", flush=True)
t0 = time.time()
found = False
for member in tar:
    if member.name == "mbdump/link_type":
        print(f"Found at {time.time() - t0:.2f}s", flush=True)
        f = tar.extractfile(member)
        if f:
            print("Extracting...", flush=True)
            row_count = 0
            for row in csv.reader(f, delimiter="\t"):
                row_count += 1
                if row_count % 100 == 0:
                    print(f"  Row {row_count}", flush=True)
            print(f"Total rows: {row_count}", flush=True)
        found = True
        break

if not found:
    print("NOT FOUND", flush=True)

tar.close()
print("Done", flush=True)
