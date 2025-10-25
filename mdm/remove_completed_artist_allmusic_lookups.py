"""
Artist Deduplicator for AllMusic Scraper

This script removes artists from artists.txt that already exist in allmusic_data.csv,
using the EXACT same matching method as the scraper: simple exact string matching
on the first column (artist name) of the CSV.

No similarity checking is used - only exact matches are removed.
"""

from pathlib import Path
from datetime import datetime

# Configuration - matches the scraper's settings
INPUT_ARTISTS_FILE = "artists.txt"
OUTPUT_CSV_FILE = "allmusic_data.csv"
DELIMITER = "|"


def load_artists():
    """
    Load artists from input file.
    Exact replication of the scraper's load_artists() function.

    Returns:
        list: Artist names (stripped, empty lines removed)
    """
    with open(INPUT_ARTISTS_FILE, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f if line.strip()]
    return artists


def load_scraped_artists():
    """
    Load set of already scraped artists to avoid duplicates.
    EXACT replication of the scraper's load_scraped_artists() function.

    Returns:
        set: Artist names from first column of CSV (exact matches only)
    """
    if not Path(OUTPUT_CSV_FILE).exists():
        return set()
    with open(OUTPUT_CSV_FILE, "r", encoding="utf-8") as f:
        return {row.split(DELIMITER)[0] for row in f.readlines()[1:]}


def create_backup(filepath: str) -> str:
    """
    Create a timestamped backup of the original file.

    Args:
        filepath: Path to file to backup

    Returns:
        str: Path to backup file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{filepath}.backup_{timestamp}"

    if Path(filepath).exists():
        Path(filepath).rename(backup_path)
        print(f"✓ Backup created: {backup_path}")

    return backup_path


def deduplicate_artists():
    """
    Remove already-scraped artists from artists.txt using exact string matching.

    This replicates the scraper's logic:
        remaining_artists = [a for a in artists if a not in scraped_artists]
    """
    print("=" * 60)
    print("Remove already-scraped artists from artists.txt")
    print("=" * 60)
    print()

    # Load data using exact same functions as scraper
    artists = load_artists()
    scraped_artists = load_scraped_artists()

    # Apply exact same filtering logic as scraper
    remaining_artists = [a for a in artists if a not in scraped_artists]

    # Statistics
    total_artists = len(artists)
    already_scraped = len(scraped_artists)
    to_remove = total_artists - len(remaining_artists)
    remaining = len(remaining_artists)

    print(f"Analysis:")
    print(f" Total artists in {INPUT_ARTISTS_FILE}: {total_artists}")
    print(f" Already scraped in {OUTPUT_CSV_FILE}: {already_scraped}")
    print(f" Artists to remove (exact matches): {to_remove}")
    print(f" Remaining artists to scrape: {remaining}")
    print()

    if to_remove == 0:
        print(
            " No matching records found. artists.txt contains only names not in allmusic_data.csv."
        )
        return

    # Confirm action
    print(f"This will:")
    print(f"  1. Create backup: {INPUT_ARTISTS_FILE}.backup_[timestamp]")
    print(f"  2. Overwrite {INPUT_ARTISTS_FILE} with {remaining} artists")
    print()

    response = input("Proceed? (y/n): ").strip().lower()
    if response != "y":
        print("❌ Operation cancelled.")
        return

    # Create backup
    backup_path = create_backup(INPUT_ARTISTS_FILE)

    # Write cleaned artists file
    with open(INPUT_ARTISTS_FILE, "w", encoding="utf-8") as f:
        for artist in remaining_artists:
            f.write(f"{artist}\n")

    print()
    print("=" * 60)
    print("Deduplication complete!")
    print(f" Removed: {to_remove} artists")
    print(f" Remaining: {remaining} artists")
    print(f" Backup: {backup_path}")
    print("=" * 60)


if __name__ == "__main__":
    deduplicate_artists()
