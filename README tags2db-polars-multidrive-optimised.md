# tags2db-polars-multidrive-optimised - Comprehensive Guide

## Overview

This script is the **foundational component** of the tagminder suite, responsible for creating and maintaining the SQLite database that all other tagminder scripts depend on. It imports and exports audio file metadata tags to/from a SQLite database, with specialized optimizations for multi-drive setups.

The script reads all metadata tags present in audio files (excluding embedded images) and stores them in a SQLite database for fast querying and manipulation. It creates the central database (`alib` table) that serves as the backbone for all tagminder operations.

## Key Features

- **Comprehensive tag import**: Imports all metadata tags present in audio files (excluding embedded images)
- **Multi-drive optimization**: Uses dedicated worker pools per physical drive to maximize concurrent I/O
- **Vectorized processing**: Leverages Polars DataFrame operations for efficient data handling
- **Flexible export**: Allows selective export of tags back to audio files
- **Database foundation**: Creates the core database structure required by all tagminder tools
- **Dynamic schema**: Automatically expands database schema to accommodate any tags found in files

## Supported Audio Formats

The script processes files with the following extensions:
- `.flac` - Free Lossless Audio Codec
- `.wv` - WavPack
- `.m4a` - MPEG-4 Audio
- `.aiff` - Audio Interchange File Format
- `.ape` - Monkey's Audio
- `.mp3` - MPEG Audio Layer III
- `.ogg` - Ogg Vorbis

## Usage

### Import Mode - Creating/Updating the Database

```bash
python tags2db-polars-multidrive-optimised.py import <database_path> <music_directory>... [options]
```

**Arguments:**
- `<database_path>`: Path to the SQLite database file (will be created if it doesn't exist)
- `<music_directory>...`: One or more paths to music directories to scan (space-separated)

**Options:**
- `--workers N`: Number of worker processes per drive (default: CPU cores ÷ number of drives)
- `--chunk-size N`: Files to process per chunk (default: 4000)
- `--log LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL - default: INFO)

**Examples:**

Single directory import:
```bash
python tags2db-polars-multidrive-optimised.py import /path/to/music.db /music/library
```

Multi-drive import with custom workers:
```bash
python tags2db-polars-multidrive-optimised.py import /path/to/music.db /qnap/drive1 /qnap/drive2 /qnap/drive3 --workers 8
```

Large library with custom chunk size:
```bash
python tags2db-polars-multidrive-optimised.py import /path/to/music.db /music/library --chunk-size 2000 --workers 4
```

### Export Mode - Writing Tags Back to Files

```bash
python tags2db-polars-multidrive-optimised.py export <database_path> <music_directory> [options]
```

**Arguments:**
- `<database_path>`: Path to the existing SQLite database file
- `<music_directory>`: Directory path to filter exported files (only files under this path will be updated)

**Options:**
- `--ignore-lastmodded`: Don't preserve original file modification timestamps when writing tags
- `--log LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL - default: INFO)

**Examples:**

Export all tags for a specific directory:
```bash
python tags2db-polars-multidrive-optimised.py export /path/to/music.db /music/classical
```

Export without preserving modification times:
```bash
python tags2db-polars-multidrive-optimised.py export /path/to/music.db /music/library --ignore-lastmodded
```

## What Gets Imported

The script imports **all metadata tags** found in audio files, including but not limited to:

### Standard Tags
- **Basic metadata**: title, artist, album, genre, year, track numbers
- **Advanced metadata**: composer, conductor, performer, producer, engineer
- **Album information**: albumartist, compilation, label, catalog numbers
- **Technical data**: bitrate, sample rate, channels, length, file size

### Specialized Tags
- **MusicBrainz IDs**: Complete set of MusicBrainz identifiers for precise music identification
- **ReplayGain data**: Album and track gain/peak values for volume normalization
- **Acoustic analysis**: AcousticBrainz mood data, audio fingerprints
- **Streaming service tags**: Roon tags, Bliss analysis data
- **Custom tags**: Any non-standard tags present in files

### File System Metadata
- **Full file paths** and directory structure
- **File timestamps**: creation, modification, and access times
- **File size information** in multiple formats
- **Extension and filetype** detection

**Note**: Embedded images (album artwork) are **not** imported to keep the database size manageable.

## Database Structure

The script creates a SQLite database with table name `alib` where:
- **Primary Key**: `__path` (full file path)
- **System Fields**: File system metadata (prefixed with `__`)
- **Tag Fields**: All audio metadata tags found in files
- **Dynamic Schema**: Automatically expands to accommodate any tags found in your files
- **Data Types**: Primarily text fields with automatic type handling
- **Multi-value tags**: Stored as delimited strings for complex tag relationships
- **UPSERT Operations**: Uses INSERT OR REPLACE to handle both new records and updates

## Multi-Drive Optimization

The script is specifically designed for high-performance operation across multiple physical drives:

### Architecture Benefits
- **Dedicated worker pools**: Each drive gets its own ProcessPoolExecutor with dedicated workers
- **Concurrent I/O**: Multiple drives processed simultaneously without I/O contention
- **Reduced disk contention**: Workers focus on single physical disks
- **Locality optimization**: Files processed in sorted order for better disk read patterns

### Memory Efficiency
- **Chunked processing**: Files processed in configurable batches to manage memory consumption
- **Vectorized operations**: Polars DataFrame operations for efficient data transformation
- **Streaming database writes**: Efficient bulk insert operations
- **Progress tracking**: Detailed logging of processing progress per drive

### Automatic Scaling
- **CPU-aware defaults**: Worker count automatically calculated based on available cores
- **Drive-aware scaling**: Worker distribution considers number of active drives
- **Configurable batch sizes**: Tunable for different system configurations

## ⚠️ Critical Performance Warning

**DO NOT specify multiple paths on the same physical drive during import operations.** This will cause:
- Extreme I/O stress on the drive
- Significant performance degradation
- Potential drive head thrashing
- Dramatically increased processing time

The multi-drive optimization is designed to work with **one path per physical drive**. If you need to process multiple directories on the same drive, specify the common parent directory instead.

**Correct multi-drive usage:**
```bash
# Good: Each path is on a different physical drive
python tags2db-polars-multidrive-optimised.py import music.db /drive1/music /drive2/music /drive3/music
```

**Incorrect same-drive usage:**
```bash
# Bad: Multiple paths on the same physical drive
python tags2db-polars-multidrive-optimised.py import music.db /drive1/classical /drive1/jazz /drive1/rock
```

**Better same-drive approach:**
```bash
# Good: Single parent path covers all subdirectories
python tags2db-polars-multidrive-optimised.py import music.db /drive1/music
```

## Performance Considerations

### System Resources
- **Memory Usage**: Reduce `--chunk-size` if experiencing memory pressure with large libraries
- **CPU Utilization**: Automatically distributes CPU cores across active drives
- **I/O Patterns**: Optimizes disk access patterns through file sorting and drive-specific worker pools

### Tuning Parameters
- **Worker count**: Increase `--workers` for systems with many CPU cores, decrease for I/O-bound systems
- **Chunk size**: Larger chunks use more memory but may improve throughput
- **Logging level**: Use DEBUG for detailed progress tracking, INFO for normal operation

## Requirements

- **Python 3.5+** (uses `os.scandir` for fast directory traversal)
- **puddlestuff**: For audio file tag reading/writing (`audioinfo` module)
- **polars**: For high-performance DataFrame operations
- **SQLite3**: Built into Python standard library

## Integration with tagminder

This script serves as the **database foundation** for the entire tagminder ecosystem:

1. **Run this script first** to create your music database
2. **Other tagminder tools** read from this database for their operations
3. **Regular re-runs** keep the database synchronized with file system changes
4. **Export functionality** allows propagating database changes back to files

## Error Handling

The script includes comprehensive error handling:
- **Individual file errors** don't stop the entire operation
- **Failed files** are logged with specific error messages
- **Processing statistics** provided for processed vs. failed files
- **Database schema** automatically migrated to accommodate new tag fields
- **File validation** during export operations ensures files exist before writing

## Troubleshooting

### Common Issues

**Permission errors**: Ensure read access to music directories and write access to database location

**Memory usage**: Reduce `--chunk-size` if experiencing memory pressure with large libraries

**Slow performance**:
- Increase `--workers` for systems with many CPU cores
- Decrease workers for I/O-bound systems
- Ensure you're not specifying multiple paths on the same physical drive

**Database locked**: Ensure no other tagminder processes are accessing the database simultaneously

### Logging and Debugging

Use `--log DEBUG` for detailed processing information, particularly useful for:
- Tracking progress through large libraries
- Identifying problematic files
- Debugging performance issues
- Monitoring multi-drive coordination

## Example Workflows

### Initial Setup
```bash
# Create the foundational database
python tags2db-polars-multidrive-optimised.py import music.db /music/library --log INFO

# Verify import completed successfully
python tags2db-polars-multidrive-optimised.py import music.db /music/library --log INFO
```

### Regular Maintenance
```bash
# Update database with new files (incremental)
python tags2db-polars-multidrive-optimised.py import music.db /music/library

# Export modified tags back to files
python tags2db-polars-multidrive-optimised.py export music.db /music/library
```

### Multi-Drive Setup
```bash
# Import from multiple drives optimally
python tags2db-polars-multidrive-optimised.py import music.db \
    /nas/drive1 /nas/drive2 /nas/drive3 \
    --workers 6 --chunk-size 5000
```

### Selective Export
```bash
# Export tags for specific directory only
python tags2db-polars-multidrive-optimised.py export music.db /music/classical

# Export without preserving modification timestamps
python tags2db-polars-multidrive-optimised.py export music.db /music/library --ignore-lastmodded
```

## Important Notes

- **Database dependency**: This script creates the foundational database that all other tagminder scripts require
- **Comprehensive import**: All metadata tags present in audio files are imported (except embedded images)
- **Path filtering**: Export operations can be filtered by directory path to update only specific subsets of files
- **File validation**: Export operations validate file existence before attempting to write tags
- **Schema flexibility**: Database schema automatically adapts to accommodate any tags found in your music collection
