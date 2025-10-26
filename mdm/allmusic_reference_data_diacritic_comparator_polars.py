import sqlite3
import polars as pl
import unicodedata as ud
from unidecode import unidecode
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def normalize_lower(s: str) -> str:
    """
    Normalize a string using NFC normalization and convert to lowercase.

    Args:
        s: Input string to normalize

    Returns:
        Normalized lowercase string, or None if input is None
    """
    if s is None:
        return None
    return ud.normalize("NFC", s).lower()


def remove_diacritics(s: str) -> str:
    """
    Remove diacritics from a string using unidecode.

    Args:
        s: Input string

    Returns:
        String without diacritics, or None if input is None
    """
    if s is None:
        return None
    return unidecode(s)


def main():
    # Connect to SQLite database
    conn = sqlite3.connect("/tmp/amg/dbtemplate.db")

    logger.info("Loading records where similarity_override is NULL")

    # Load records where similarity_override is null
    query = """
    SELECT artist, allmusic_artist, similarity_override
    FROM allmusic_reference_data
    WHERE similarity_override IS NULL
    """

    df = pl.read_database(query, connection=conn)
    logger.info(f"Loaded {len(df)} records")

    # Apply normalization using vectorized operations via map_elements
    # This applies the function to entire columns at once
    logger.info("Normalizing artist names")
    df = df.with_columns(
        [
            pl.col("artist")
            .map_elements(normalize_lower, return_dtype=pl.Utf8)
            .alias("artist_normalized"),
            pl.col("allmusic_artist")
            .map_elements(normalize_lower, return_dtype=pl.Utf8)
            .alias("allmusic_normalized"),
        ]
    )

    # Remove diacritics using vectorized operations
    logger.info("Removing diacritics for comparison")
    df = df.with_columns(
        [
            pl.col("artist_normalized")
            .map_elements(remove_diacritics, return_dtype=pl.Utf8)
            .alias("artist_no_diacritics"),
            pl.col("allmusic_normalized")
            .map_elements(remove_diacritics, return_dtype=pl.Utf8)
            .alias("allmusic_no_diacritics"),
        ]
    )

    # Vectorized filter: identify rows where normalized strings differ BUT
    # strings without diacritics are identical (i.e., only diacritics differ)
    logger.info("Identifying diacritic-only differences using vectorized filtering")
    diacritic_only_diff = df.filter(
        (pl.col("artist_normalized") != pl.col("allmusic_normalized"))
        & (pl.col("artist_no_diacritics") == pl.col("allmusic_no_diacritics"))
    )

    count = len(diacritic_only_diff)
    logger.info(f"Found {count} rows where the only difference is diacritics")

    if count > 0:
        # Extract the artist and allmusic_artist pairs that need updating
        # This uses vectorized column selection
        update_pairs = diacritic_only_diff.select(["artist", "allmusic_artist"])

        logger.info(f"Updating {count} records with similarity_override = '99'")

        # Update the database using parameterized queries
        cursor = conn.cursor()

        # Convert to list of tuples for executemany (efficiently handles multiple updates)
        # Polars rows() returns an iterator of tuples - this is memory efficient
        update_data = [("99", row[0], row[1]) for row in update_pairs.iter_rows()]

        cursor.executemany(
            """
            UPDATE allmusic_reference_data
            SET similarity_override = ?
            WHERE artist = ? AND allmusic_artist = ?
            """,
            update_data,
        )

        conn.commit()
        logger.info(f"Successfully updated {cursor.rowcount} records")

        # Log sample of records for whom similarity_override will be updated.
        sample_size = min(10, count)
        logger.info(f"Sample of matched records (showing {sample_size} of {count}):")
        for i, row in enumerate(update_pairs.head(sample_size).iter_rows(named=True)):
            logger.info(f"  {i + 1}. '{row['artist']}' -> '{row['allmusic_artist']}'")

        cursor.close()
    else:
        logger.info("No records to update")

    conn.close()
    logger.info("Database connection closed")

    return count


if __name__ == "__main__":
    updated_count = main()
    logger.info(f"Process completed. Total records updated: {updated_count}")
