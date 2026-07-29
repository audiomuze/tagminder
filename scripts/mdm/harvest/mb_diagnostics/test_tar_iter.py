#!/usr/bin/env python3
"""Test basic tar iteration."""
import tarfile

tar_path = "/mnt/usbc1/MUSIC_MASTER_METADATA/mbdump.tar.bz2"
print(f"Opening {tar_path}", flush=True)

tar = tarfile.open(tar_path, "r:bz2")
print(f"Tar opened", flush=True)

count = 0
target_members = {"mbdump/link_type", "mbdump/link"}
found = set()

print("Starting iteration...", flush=True)
try:
    for member in tar:
        count += 1
        if count % 10000 == 0:
            print(f"Scanned {count} members, found {len(found)}", flush=True)
        if member.name in target_members:
            print(f"Found: {member.name}", flush=True)
            found.add(member.name)
        if len(found) == len(target_members):
            print(f"All targets found after {count} members", flush=True)
            break
except Exception as e:
    print(f"ERROR: {e}", flush=True)
    import traceback
    traceback.print_exc()

print(f"Done: scanned {count}, found {found}", flush=True)
tar.close()
