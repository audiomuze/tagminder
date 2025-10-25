"""
COMPLETE Optimized Personnel Processor with Polars
- Handles both personnel and performer columns
- Uses existing lentity for case-insensitive matching
- Pure Polars implementation
- Robust error handling
- Full changelog integration
"""

import sqlite3
import polars as pl
import re
from collections import defaultdict
from datetime import datetime, timezone
import logging
from typing import Tuple, Set, List, Dict, Any

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["personnel", "performer"]
DELIM = r'\\'
SCRIPT_NAME = "personnel-polars-complete.py"
CHUNK_SIZE = 10_000  # For memory efficiency

# Initialize logging
logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s - %(levelname)s - %(message)s")


def get_alib_schema(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Dynamically generates a Polars schema for the 'alib' table.

    It defaults all columns to pl.Utf8, then overrides specific columns
    like 'rowid' and 'sqlmodded' to pl.Int64.

    Args:
        conn (sqlite3.Connection): An open connection to the SQLite database.

    Returns:
        Dict[str, Any]: A dictionary representing the Polars schema.
    """
    try:
        # Use a PRAGMA query to get information about all columns in the table
        cursor = conn.execute("PRAGMA table_info(alib)")

        # Extract just the column names from the query result
        column_names = [row[1] for row in cursor.fetchall()]

        # Initialize the schema with pl.Utf8 for all columns
        schema = {name: pl.Utf8 for name in column_names}

        # Override specific columns with their correct types
        # Note: This assumes 'rowid' and 'sqlmodded' exist in the table
        schema["rowid"] = pl.Int64
        schema["sqlmodded"] = pl.Int64

        return schema

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return {}


# ---------- Role Mapping (Complete) ----------
ROLE_MAPPING = {
    'Arranger': 'arranger',
    'Instrumentation': 'arranger',
    'MusicalAdviser': 'arranger',
    'Orchestral Arranger': 'arranger',
    'RecordingArranger': 'arranger',
    'String Arranger': 'arranger',
    'StringArranger': 'arranger',
    'Strings Arranger': 'arranger',
    'Vocal Arranger': 'arranger',
    'VocalArranger': 'arranger',
    'WorkArranger': 'arranger',
    'Adapter': 'arranger',
    'Artist': 'artist',
    'Contributor': 'artist',
    'Mixed Artist': 'artist',
    'Performance': 'artist',
    'Performed by': 'artist',
    'Performer': 'artist',
    'BackgroundVocalist': 'artist',
    'Background Vocals': 'artist',
    'Background Vocal': 'artist',
    'BackgroundVocals': 'artist',
    'Backing Vocals': 'artist',
    'Banjo': 'banjoist',
    'Bass': 'bassist',
    'Bass Guitar': 'bassist',
    'Bass Programmer': 'bassist',
    'Bass Synthesizer': 'bassist',
    'Double Bass': 'bassist',
    'DoubleBass': 'bassist',
    'Electric Bass': 'bassist',
    'Electric Bass Guitar': 'bassist',
    'Synthbass': 'bassist',
    'Synth Bass': 'bassist',
    'Upright Bass': 'bassist',
    'UprightBass': 'bassist',
    'Bass guitar (all types)': 'bassist',
    'BassGuitar': 'bassist',
    'BassClarinet': 'clarinetist',
    'Clarinet': 'clarinetist',
    'Bells': 'percussionist',
    'AfricanPercussion': 'percussionist',
    'Bongo Drums': 'percussionist',
    'Chimes': 'percussionist',
    'Claps': 'percussionist',
    'Congas': 'percussionist',
    'Cowbell': 'percussionist',
    'Cowbell Percussion': 'percussionist',
    'Cymbals': 'percussionist',
    'Glockenspiel': 'percussionist',
    'Kalimba Percussion': 'percussionist',
    'Percussion instrument': 'percussionist',
    'Percussion': 'percussionist',
    'Shaker': 'percussionist',
    'Stick Percussion': 'percussionist',
    'Tambourine': 'percussionist',
    'Timbales Drums': 'percussionist',
    'Vibraphone': 'percussionist',
    'Wood Block Percussion': 'percussionist',
    'Bandoneon': 'bandoneonist',
    'Bouzouki': 'bouzouki',
    'Accordion': 'accordionist',
    'Cello': 'cellist',
    'Cornet': 'cornetist',
    'CoProducer': 'producer',
    'Composer': 'composer',
    'ComposerLyricist': 'composer',
    'Music': 'composer',
    'Conductor': 'conductor',
    'MusicDirector': 'conductor',
    'Orchestral Conductor': 'conductor',
    'Assistant': 'engineer',
    'Assistant Engineer': 'engineer',
    'AssistantEngineer': 'engineer',
    'Assistant Mastering Engineer': 'engineer',
    'Recording Assistant': 'engineer',
    'Recording Second Engineer': 'engineer',
    'RecordingSecondEngineer': 'engineer',
    'Assistant Recording Engineer': 'engineer',
    'Assistant Mixer': 'mixer',
    'Assistant Mixing Engineer': 'mixer',
    'AssistantMixingEngineer': 'mixer',
    'MixingSecondEngineer': 'mixer',
    'AssistantProducer': 'producer',
    'ProductionAssistant': 'producer',
    'Author': 'composer',
    'Writer': 'composer',
    'Dobro': 'dobro',
    'DobroGuitar': 'dobro',
    'Drum': 'drummer',
    'DrumKit': 'drummer',
    'Drum Machine Drums': 'drummer',
    'Drum Programmer': 'drummer',
    'DrumProgrammer': 'drummer',
    'DrumProgramming': 'drummer',
    'Drums': 'drummer',
    'Snare Drum': 'drummer',
    'Drums (Drum Set)': 'drummer',
    'Editing Engineer': 'engineer',
    'Electric Guitar': 'guitarist',
    'ElectricGuitar': 'guitarist',
    'Guitar (acoustic)': 'guitarist',
    'Guitar (any type)': 'guitarist',
    'Guitar (electric)': 'guitarist',
    'Guitar': 'guitarist',
    'Nylon': 'guitarist',
    'Nylon Strung Guitar': 'guitarist',
    'Rhodes Guitar': 'guitarist',
    'Rhythm Guitar': 'guitarist',
    'Slide Guitar': 'guitarist',
    'SlideGuitar': 'guitarist',
    'Solo Guitar': 'guitarist',
    '12 String Guitar': 'guitarist',
    'Acoustic Guitar': 'guitarist',
    'Baritone Guitar': 'guitarist',
    'BaritoneGuitar': 'guitarist',
    'Hohner Guitaret Guitar': 'guitarist',
    'Lead Guitar': 'guitarist',
    'AcousticGuitar': 'guitarist',
    'Keyboard': 'keyboardist',
    'Keyboards': 'keyboardist',
    'Mellotron': 'keyboardist',
    'ModularSynth': 'keyboardist',
    'Omnichord Synthesizer': 'keyboardist',
    'Organ': 'keyboardist',
    'Piano': 'keyboardist',
    'Prepared Piano': 'keyboardist',
    'Rhodes': 'keyboardist',
    'Rhodes Piano': 'keyboardist',
    'Rhodes Solo': 'keyboardist',
    'Synthesizer': 'keyboardist',
    'SynthPad': 'keyboardist',
    'WurlitzerElectricPiano': 'keyboardist',
    'Wurlitzer': 'keyboardist',
    'Wurlitzer Piano': 'keyboardist',
    'AdditionalKeyboard': 'keyboardist',
    'Clavinet': 'keyboardist',
    'Electric organ': 'keyboardist',
    'HammondB3': 'keyboardist',
    'Hammond B3 Organ': 'keyboardist',
    'Hammond Organ': 'keyboardist',
    'Harpsichord': 'keyboardist',
    'Juno Synthesizer': 'keyboardist',
    'DigitalPiano': 'keyboardist',
    'HammondOrgan': 'keyboardist',
    'Harp': 'harpist',
    'Horn': 'hornist',
    'Lead Vocalist': 'artist',
    'LeadVocals': 'artist',
    'Mandolin': 'mandolinist',
    'Organistrum': 'vielleur',
    'AssociatedPerformer': 'artist',
    'Featured Vocals': 'artist',
    'BassVocalist': 'artist',
    'Vocal accompaniment': 'artist',
    'Vocalist': 'artist',
    'Vocals': 'artist',
    'Vocal': 'artist',
    'Voice': 'artist',
    'Lead Vocals': 'artist',
    'Background': 'artist',
    'Lead': 'artist',
    'Flute': 'flutist',
    'Fiddle': 'fiddler',
    'Harmonica': 'harmonicist',
    'Lyricist': 'lyricist',
    'Lyrics': 'lyricist',
    'Songwriter': 'composer',
    'Oboe': 'oboist',
    'Orchestra': 'ensemble',
    'String Orchestra': 'ensemble',
    'StringQuartet': 'ensemble',
    'Ensemble': 'ensemble',
    'Steel Guitar': 'guitarist',
    'SteelGuitar': 'guitarist',
    'Pedal Steel Guitar': 'guitarist',
    'Viola': 'violist',
    'Violin': 'violinist',
    'Whistle': 'whistler',
    'Woodwinds': 'instrumentalist',
    'String instrument': 'instrumentalist',
    'Strings': 'instrumentalist',
    'Trumpet': 'trumpeter',
    'Trombone': 'trombonist',
    'Tenor Saxophone': 'saxophonist',
    'TenorSaxophone': 'saxophonist',
    'Saxophone': 'saxophonist',
    'SoundEffects': 'programmer',
    'Programmer': 'programmer',
    'ProgrammingEngineer': 'programmer',
    'Programming': 'programmer',
    'Sampler': 'programmer',
    'Sequencer': 'programmer',
    'Drum Machine': 'programmer',
    'Additional Engineer': 'engineer',
    'Additional Masterer': 'engineer',
    'Additional Production': 'producer',
    'Additional Recorder': 'engineer',
    'Additional Vocal Recording Engineer': 'engineer',
    'AdditionalEngineer': 'engineer',
    'AltoRecorder': 'flautist',
    'Archival Producer': 'producer',
    'Artist background vocal engineer': 'engineer',
    'AssociateProducer': 'producer',
    'Audio Mastering': 'engineer',
    'Audio Recording Engineer': 'engineer',
    'BalanceEngineer': 'engineer',
    'DigitalEditingEngineer': 'engineer',
    'Electronics': 'programmer',
    'Engineer': 'engineer',
    'Executive Producer': 'producer',
    'ExecutiveProducer': 'producer',
    'FeaturedArtist': 'artist',
    'ImmersiveMixingEngineer': 'engineer',
    'Instruments': 'instrumentalist',
    'Lute': 'lutist',
    'MainArtist': 'albumartist',
    'Masterer': 'engineer',
    'Mastering Engineer': 'engineer',
    'MasteringEngineer': 'engineer',
    'Mixer': 'mixer',
    'Mixing Engineer': 'engineer',
    'MixingEngineer': 'engineer',
    'MusicPublisher': 'label',
    'OrchestraContractor': 'ensemble',
    'Overdub Engineer': 'engineer',
    'Producer': 'producer',
    'Production': 'producer',
    'Recorded by': 'engineer',
    'Recording': 'engineer',
    'Recording & Mixing': 'engineer',
    'Recording Engineer': 'engineer',
    'RecordingEngineer': 'engineer',
    'Sound Engineer': 'engineer',
    'Sound Restoration Engineer': 'engineer',
    'SoundEngineer': 'engineer',
    'SoundRecordist': 'engineer',
    'Studio': 'recordinglocation',
    'StudioMusician': 'artist',
    'StudioPersonnel': 'artist',
    'StudioProducer': 'producer',
    'Technical Engineer': 'engineer',
    'Tracking Engineer': 'engineer',
    'Vocal Engineer': 'engineer',
    'Vocal Recording Engineer': 'engineer',
    'VocalEditingEngineer': 'engineer',
    'VocalEngineer': 'engineer',
    'VocalProducer': 'producer',
    'ReissueProducer': 'producer',

    # Percussion instruments
    'Bongos': 'percussionist',
    'Claves': 'percussionist',
    'Castanets': 'percussionist',
    'Handclaps': 'percussionist',
    'Tubular Bells': 'percussionist',
    'Gong': 'percussionist',
    'Marimba': 'percussionist',
    'Mark Tree': 'percussionist',
    'Timbales': 'percussionist',
    'Surdo': 'percussionist',
    'Washboard': 'percussionist',
    'Tabla': 'percussionist',
    'Body Percussion': 'percussionist',

    # String instruments
    'Ukulele': 'ukulele',
    'Resonator Guitar': 'guitarist',
    'Tenor Guitar': 'guitarist',
    'Lap Steel Guitar': 'guitarist',
    'Classical Guitar': 'guitarist',
    'Flamenco Guitar': 'guitarist',
    'Violino Piccolo': 'violinist',

    # Wind instruments
    'Bass Trumpet': 'trumpeter',
    'French Horn': 'hornist',
    'Flugelhorn': 'hornist',
    'Recorder': 'flautist',
    'Uilleann Pipes': 'piper',
    'Natural Horn': 'hornist',

    # Keyboard instruments
    'Farfisa': 'keyboardist',
    'Tack Piano': 'keyboardist',
    'Reed Organ': 'keyboardist',
    'Gamelan': 'instrumentalist',
    'Toy Piano': 'keyboardist',
    'Celesta': 'keyboardist',
    'Synclavier': 'keyboardist',
    'Moog': 'keyboardist',
    'Minimoog': 'keyboardist',

    # Other instruments
    'Harmonium': 'harmonium',
    'Mbira': 'instrumentalist',
    'Guitorgan': 'instrumentalist',
    'Autoharp': 'instrumentalist',
    'Electric Sitar': 'sitar',
    'Theremin': 'theremin',
    'Stylophone': 'instrumentalist',

    # Electronic/Programming
    'Effects': 'programmer',
    'Voice Synthesizer': 'programmer',
    'Vocoder': 'programmer',
    'Tape': 'programmer',
    'Electronic Drum Set': 'programmer',
    'Electronic Instruments': 'programmer',

    # Vocal roles (some exist but adding variations)
    'Vocals (Lead)': 'artist',
    'Vocals (Background)': 'artist',

    # Section groupings
    'Woodwind': 'instrumentalist',
    'Brass': 'instrumentalist',
}

# ---------- Helper Functions ----------
def get_unique_mapped_roles() -> List[str]:
    """Get unique mapped roles in original order"""
    seen = set()
    return [role for role in ROLE_MAPPING.values()
            if not (role in seen or seen.add(role))]

def load_reference_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Load MB reference data with precomputed lentity"""
    return pl.read_database(
        "SELECT entity, lentity FROM _REF_mb_disambiguated",
        conn
    ).with_columns([
        pl.col("entity").cast(pl.Utf8),
        pl.col("lentity").cast(pl.Utf8)
    ]).unique()

def is_valid_name_role_segment(segment: str) -> bool:
    """Check if segment follows name/role patterns"""
    segment = (segment or "").strip()
    return (('(' in segment and ')' in segment) or
            (',' in segment) or
            (len(segment.split()) >= 2))

def extract_roles_from_group(group: str) -> Tuple[str, List[str]]:
    """Extract name and roles from a group string"""
    group = group.strip()

    # Case 1: "Name (Role)" format
    if '(' in group and ')' in group:
        name = group.split('(')[0].strip()
        roles_part = group.split('(')[1].split(')')[0].strip()
        roles = [r.strip() for r in roles_part.split('\\') if r.strip()]
        return name, roles

    # Case 2: "Name, Role" format
    if ',' in group:
        parts = [p.strip() for p in group.split(',')]
        return parts[0], parts[1:]

    # Case 3: Fallback to space separation
    parts = group.split()
    return ' '.join(parts[:-1]), [parts[-1]]

def process_segments(
    personnel_str: str,
    performer_str: str,
    mb_ref_df: pl.DataFrame
) -> Tuple[Dict[str, Set[str]], Set[str], List[str]]:
    """Process all segments with optimized Polars operations"""
    role_to_names = defaultdict(set)
    unmapped_roles = set()
    discarded_segments = []

    # Extract all potential groups
    groups = []
    for s in filter(None, [personnel_str, performer_str]):
        groups.extend(re.split(r'\s*-\s*', s.strip()))

    # Process each group
    for group in filter(None, groups):
        if not is_valid_name_role_segment(group):
            discarded_segments.append(group)
            continue

        try:
            name, roles = extract_roles_from_group(group)

            # Handle standalone names
            if not roles and ' ' in name:
                # Lookup in MB reference
                matches = mb_ref_df.filter(
                    pl.col("lentity") == name.lower()
                )["entity"].to_list()

                if matches:
                    role_to_names["artist"].add(matches[0])
                else:
                    discarded_segments.append(f"UNMATCHED_NAME:{name}")
                continue

            # Process roles
            for role in roles:
                if role in ROLE_MAPPING:
                    mapped = ROLE_MAPPING[role]
                    if '\\' in mapped:
                        for r in mapped.split('\\'):
                            role_to_names[r.strip()].add(name)
                    else:
                        role_to_names[mapped].add(name)
                else:
                    unmapped_roles.add(role)

        except Exception as e:
            discarded_segments.append(f"ERROR:{group} ({str(e)})")

    return role_to_names, unmapped_roles, discarded_segments

def create_updates_df(
    personnel_df: pl.DataFrame,
    mb_ref_df: pl.DataFrame,
    role_columns: List[str]
) -> pl.DataFrame:
    """Create DataFrame with all role updates"""
    # Process in chunks for memory efficiency
    processed_chunks = []

    for i in range(0, personnel_df.height, CHUNK_SIZE):
        chunk = personnel_df.slice(i, CHUNK_SIZE)

        # Process each row
        chunk_results = []
        for row in chunk.iter_rows(named=True):
            roles, unmapped, discarded = process_segments(
                row["personnel"],
                row["performer"],
                mb_ref_df
            )

            # Convert to row format
            row_data = {"rowid": row["rowid"]}
            for col in role_columns:
                if col in roles:
                    row_data[col] = DELIM.join(sorted(roles[col]))

            chunk_results.append(row_data)

        # Create DataFrame with explicit types
        schema = {"rowid": pl.Int64}
        schema.update({col: pl.Utf8 for col in role_columns})

        chunk_df = pl.DataFrame(chunk_results, schema=schema)
        processed_chunks.append(chunk_df)

    return pl.concat(processed_chunks) if processed_chunks else pl.DataFrame(schema=schema)

def integrate_updates(
    conn: sqlite3.Connection,
    alib_df: pl.DataFrame,
    updates_df: pl.DataFrame,
    role_columns: List[str]
) -> None:
    """Apply updates to database with changelog"""
    # Create changelog entries
    changelog_data = []
    timestamp = datetime.now(timezone.utc).isoformat()

    # Get current values for changed rows
    changed_rows = alib_df.join(
        updates_df.select("rowid"),
        on="rowid",
        how="inner"
    )

    # Prepare updates
    for row in changed_rows.iter_rows(named=True):
        for col in role_columns:
            if col in updates_df.columns:
                new_val = updates_df.filter(pl.col("rowid") == row["rowid"])[col].item()
                old_val = row.get(col)

                if new_val != old_val:
                    changelog_data.append({
                        "alib_rowid": row["rowid"],
                        "column": f"alib.{col}",
                        "old_value": old_val,
                        "new_value": new_val,
                        "timestamp": timestamp,
                        "script": SCRIPT_NAME
                    })

    # Apply updates
    if updates_df.height > 0:
        # Create temp table with explicit schema
        updates_df.write_database(
            "temp_updates",
            conn,
            if_exists="replace"
        )

        # Execute SQL update
        conn.executescript(f"""
            CREATE TABLE IF NOT EXISTS alib_updates AS
            SELECT alib.* FROM alib
            JOIN temp_updates ON alib.rowid = temp_updates.rowid;

            UPDATE alib
            SET {', '.join(f'{col} = temp_updates.{col}'
                          for col in updates_df.columns if col != 'rowid')}
            FROM temp_updates
            WHERE alib.rowid = temp_updates.rowid;

            DROP TABLE temp_updates;
        """)

        # Write changelog with explicit schema
        if changelog_data:
            changelog_schema = {
                "alib_rowid": pl.Int64,
                "column": pl.Utf8,
                "old_value": pl.Utf8,
                "new_value": pl.Utf8,
                "timestamp": pl.Utf8,
                "script": pl.Utf8
            }
            pl.DataFrame(changelog_data, schema=changelog_schema).write_database(
                "changelog",
                conn,
                if_exists="append"
            )

        logging.info(f"Updated {updates_df.height} rows with {len(changelog_data)} changes")
    else:
        logging.info("No updates to apply")

def main():
    logging.info("Starting complete processing")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Load data with explicit types
        mb_ref_df = load_reference_data(conn)
        role_columns = get_unique_mapped_roles()

        alib_schema = get_alib_schema(conn)

        # Use the generated schema to read the database without inference
        alib_df = pl.read_database(
            "SELECT rowid, * FROM alib",
            conn,
            schema_overrides=alib_schema
        )

        # Define an explicit schema for the personnel and performer columns
        # to prevent the schema inference error with long strings.
        explicit_schema = {
            "rowid": pl.Int64,
            "personnel": pl.Utf8,
            "performer": pl.Utf8,
        }

        # Read the personnel data using the explicit schema
        personnel_df = pl.read_database(
            f"""SELECT rowid, {', '.join(COLUMNS)} FROM alib
                WHERE {' OR '.join(f'{col} IS NOT NULL' for col in COLUMNS)}""",
            conn,
            schema_overrides=explicit_schema
        )

        logging.info(f"Processing {personnel_df.height} tracks")

        # Process data
        updates_df = create_updates_df(personnel_df, mb_ref_df, role_columns)

        # Apply updates
        integrate_updates(conn, alib_df, updates_df, role_columns)

        logging.info("Processing completed successfully")

    except Exception as e:
        conn.rollback()
        logging.error(f"Processing failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
