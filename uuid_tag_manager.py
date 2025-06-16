"""
UUID Tag Manager - Ensures all audio files have a unique tagminder_uuid tag
This script scans audio files and adds UUID tags where missing or empty.
Supports FLAC and WavPack formats with comprehensive logging and error handling.
"""

import os
import sys
import uuid
import logging
from pathlib import Path
from typing import Generator, Tuple, Optional
from mutagen.flac import FLAC
from mutagen.wavpack import WavPack
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4
from mutagen.apev2 import APEv2
from mutagen import MutagenError

try:
    from os import scandir
except ImportError:
    from scandir import scandir  # Fallback for Python < 3.5


class UUIDTagManager:
    """Manages UUID tag operations for audio files with comprehensive logging."""
    
    SUPPORTED_EXTENSIONS = {'.flac', '.wv', '.mp3', '.m4a', '.ape'}
    UUID_TAG = 'tagminder_uuid'
    
    def __init__(self, log_level: str = 'INFO'):
        """Initialize the UUID tag manager with logging configuration."""
        self.setup_logging(log_level)
        self.stats = {
            'processed': 0,
            'modified': 0,
            'errors': 0,
            'skipped': 0
        }
    
    def setup_logging(self, level: str) -> None:
        """Configure console-only logging."""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        
        # Configure root logger for console output only
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format=log_format,
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)
    
    def scan_audio_files(self, root_path: str) -> Generator[os.DirEntry, None, None]:
        """
        Recursively scan directory for supported audio files.
        
        Args:
            root_path: Root directory to scan
            
        Yields:
            DirEntry objects for supported audio files
        """
        try:
            for entry in scandir(root_path):
                if entry.is_dir(follow_symlinks=False):
                    yield from self.scan_audio_files(entry.path)
                elif entry.is_file() and Path(entry.name).suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    yield entry
        except (OSError, PermissionError) as e:
            self.logger.error(f"Failed to scan directory '{root_path}': {e}")
            self.stats['errors'] += 1
    
    def load_audio_file(self, file_path: str) -> Optional[object]:
        """
        Load audio file using appropriate mutagen class.
        
        Args:
            file_path: Path to audio file
            
        Returns:
            Loaded audio file object or None if failed
        """
        file_ext = Path(file_path).suffix.lower()
        
        try:
            if file_ext == '.flac':
                return FLAC(file_path)
            elif file_ext == '.wv':
                return WavPack(file_path)
            elif file_ext == '.mp3':
                return MP3(file_path)
            elif file_ext == '.m4a':
                return MP4(file_path)
            elif file_ext == '.ape':
                return APEv2(file_path)
            else:
                self.logger.warning(f"Unsupported file type: {file_path}")
                return None
                
        except MutagenError as e:
            self.logger.error(f"Failed to load audio file '{file_path}': {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error loading '{file_path}': {e}")
            return None
    
    def has_valid_uuid(self, audio_file: object) -> bool:
        """
        Check if audio file has a valid non-empty UUID tag.
        
        Args:
            audio_file: Loaded audio file object
            
        Returns:
            True if valid UUID exists, False otherwise
        """
        if not hasattr(audio_file, 'tags') or audio_file.tags is None:
            return False
        
        # Handle different tag formats for different file types
        if isinstance(audio_file, MP4):
            # MP4 uses different tag structure
            uuid_values = audio_file.tags.get('----:com.apple.iTunes:TAGMINDER_UUID')
            if uuid_values:
                return bool(uuid_values[0] if isinstance(uuid_values, list) else uuid_values)
        else:
            # Standard Vorbis-style tags for FLAC, WV, APE, MP3
            uuid_values = audio_file.tags.get(self.UUID_TAG, [])
            
            # Handle both single values and lists
            if isinstance(uuid_values, list):
                return bool(uuid_values and str(uuid_values[0]).strip())
            else:
                return bool(uuid_values and str(uuid_values).strip())
        
        return False
    
    def add_uuid_tag(self, audio_file: object, file_path: str) -> bool:
        """
        Add UUID tag to audio file and save changes.
        
        Args:
            audio_file: Loaded audio file object
            file_path: Path to the file for preserving timestamps
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate new UUID
            new_uuid = str(uuid.uuid4())
            
            # Ensure tags exist
            if audio_file.tags is None:
                audio_file.add_tags()
            
            # Add UUID tag based on file type
            if isinstance(audio_file, MP4):
                # MP4 uses freeform tags for custom fields
                audio_file.tags['----:com.apple.iTunes:TAGMINDER_UUID'] = [new_uuid.encode('utf-8')]
            else:
                # Standard Vorbis-style tags for FLAC, WV, APE, MP3
                audio_file.tags[self.UUID_TAG] = [new_uuid]
            
            # Preserve original modification time
            original_stat = os.stat(file_path)
            original_mtime = original_stat.st_mtime
            original_atime = original_stat.st_atime
            
            # Save file
            audio_file.save()
            
            # Restore timestamps
            os.utime(file_path, (original_atime, original_mtime))
            
            self.logger.info(f"Added UUID '{new_uuid}' to: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add UUID to '{file_path}': {e}")
            return False
    
    def process_file(self, entry: os.DirEntry) -> None:
        """
        Process a single audio file, adding UUID if needed.
        
        Args:
            entry: DirEntry object for the file to process
        """
        file_path = entry.path
        self.stats['processed'] += 1
        
        self.logger.debug(f"Processing: {file_path}")
        
        # Load audio file
        audio_file = self.load_audio_file(file_path)
        if audio_file is None:
            self.stats['errors'] += 1
            return
        
        # Check if UUID already exists and is valid
        if self.has_valid_uuid(audio_file):
            self.logger.debug(f"UUID already exists: {file_path}")
            self.stats['skipped'] += 1
            return
        
        # Add UUID tag
        if self.add_uuid_tag(audio_file, file_path):
            self.stats['modified'] += 1
        else:
            self.stats['errors'] += 1
    
    def process_directory(self, directory: str) -> None:
        """
        Process all audio files in directory and subdirectories.
        
        Args:
            directory: Root directory to process
        """
        if not os.path.isdir(directory):
            self.logger.error(f"Directory does not exist: {directory}")
            return
        
        self.logger.info(f"Starting UUID tag processing for: {directory}")
        
        try:
            for entry in self.scan_audio_files(directory):
                self.process_file(entry)
                
                # Progress indicator for large collections
                if self.stats['processed'] % 100 == 0:
                    self.logger.info(f"Progress: {self.stats['processed']} files processed")
                    
        except KeyboardInterrupt:
            self.logger.warning("Process interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error during processing: {e}")
        
        self.print_summary()
    
    def print_summary(self) -> None:
        """Print processing summary statistics."""
        self.logger.info("=" * 50)
        self.logger.info("PROCESSING SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Files processed: {self.stats['processed']}")
        self.logger.info(f"Files modified: {self.stats['modified']}")
        self.logger.info(f"Files skipped (UUID exists): {self.stats['skipped']}")
        self.logger.info(f"Errors encountered: {self.stats['errors']}")
        
        if self.stats['processed'] > 0:
            success_rate = ((self.stats['processed'] - self.stats['errors']) / 
                          self.stats['processed']) * 100
            self.logger.info(f"Success rate: {success_rate:.1f}%")


def clear_screen() -> None:
    """Clear the terminal screen."""
    os.system('clear' if os.name == 'posix' else 'cls')


def main():
    """Main entry point for the UUID tag manager script."""
    clear_screen()
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python uuid_tag_manager.py <directory> [log_level]")
        print("Log levels: DEBUG, INFO, WARNING, ERROR")
        sys.exit(1)
    
    directory = sys.argv[1]
    log_level = sys.argv[2] if len(sys.argv) > 2 else 'INFO'
    
    # Validate log level
    valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR'}
    if log_level.upper() not in valid_levels:
        print(f"Invalid log level: {log_level}")
        print(f"Valid levels: {', '.join(valid_levels)}")
        sys.exit(1)
    
    # Initialize and run UUID tag manager
    manager = UUIDTagManager(log_level)
    manager.process_directory(directory)


if __name__ == '__main__':
    main()
