"""
Script Name: 04-personnel-polars.py

Purpose:
This script processes music library records from the 'alib' table to extract personnel information
into dedicated role-based columns without modifying the original personnel field.
It parses the personnel column to extract role-based personnel assignments and creates new columns
for each mapped role, populating them with the appropriate names.

The script:
1. Parses personnel strings in the format "Name, Role1, Role2 - Name2, Role3"
2. Creates columns for all mapped roles from ROLE_MAPPING
3. Populates role columns with extracted names using case-insensitive deduplication
4. Maintains original personnel field unchanged
5. Creates alib2 table with all original data plus new role columns
6. Maintains a changelog of all modifications made to the database

Usage:
python 04-personnel-polars.py
uv run 04-personnel-polars.py

Author: audiomuze
Created: 2025-08-08
Modified: 2025-08-10
"""

import sqlite3
import polars as pl
import logging
import re

from datetime import datetime, timezone
from collections import defaultdict

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["personnel"]
DELIM = r'\\'
SCRIPT_NAME = "personnel-polars.py"

# ---------- Role Mapping ----------
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
    'Drums & Keyboards': 'drummer\\\\keyboardist',
    'Editor': 'editor',
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
    'Keyboard & Bass': 'keyboardist\\\\bassist',
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
    'HammondOrgan': 'organist',
    'Harp': 'harpist',
    'Horn': 'hornist',
    'Lead Vocalist': 'artist',
    'LeadVocals': 'artist',
    'Mandolin': 'Mandolinist',
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
    'Engineer & Mixer': 'engineer\\\\mixer',
    'Executive Producer': 'producer',
    'ExecutiveProducer': 'producer',
    'FeaturedArtist': 'artist',
    'ImmersiveMixingEngineer': 'engineer',
    'Instruments': 'instrumentalist',
    'Lute': 'Lutist',
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
    'ReissueProducer': 'producer'
}


# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Helper Functions ----------
def get_unique_mapped_roles() -> list[str]:
    """Get unique mapped roles in order they appear in ROLE_MAPPING"""
    seen = set()
    unique_roles = []

    for mapped_role in ROLE_MAPPING.values():
        if DELIM in mapped_role:
            # Handle compound roles
            roles = mapped_role.split(DELIM)
            for role in roles:
                role = role.strip()
                if role not in seen:
                    seen.add(role)
                    unique_roles.append(role)
        else:
            if mapped_role not in seen:
                seen.add(mapped_role)
                unique_roles.append(mapped_role)

    return unique_roles

def add_name_to_delimited_string(existing_value: str, new_name: str) -> str:
    """Add name to delimited string if not already present (case insensitive)"""
    if not existing_value or existing_value.strip() == "":
        return new_name

    existing_names = [name.strip() for name in existing_value.split(DELIM) if name.strip()]
    existing_names_lower = [name.lower() for name in existing_names]

    if new_name.lower() not in existing_names_lower:
        existing_names.append(new_name)
        return DELIM.join(existing_names)

    return existing_value

def fetch_full_alib_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch complete alib table data using actual database schema"""
    # Get the actual table schema
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(alib)")
    schema_info = cursor.fetchall()

    # Build column type mapping from schema
    column_types = {}
    for row in schema_info:
        col_name = row[1]  # column name
        col_type = row[2].upper()  # column type

        if col_type in ['INTEGER', 'INT']:
            column_types[col_name] = pl.Int64
        elif col_type in ['REAL', 'FLOAT', 'DOUBLE']:
            column_types[col_name] = pl.Float64
        else:
            # Default to string for TEXT, VARCHAR, etc.
            column_types[col_name] = pl.Utf8

    # Fetch the data
    cursor.execute("SELECT * FROM alib")
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Build the dataframe with proper types
    data = {}
    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]
        dtype = column_types.get(name, pl.Utf8)

        if dtype == pl.Int64:
            data[name] = pl.Series(name=name, values=[int(x) if x is not None else None for x in col_data], dtype=dtype)
        elif dtype == pl.Float64:
            data[name] = pl.Series(name=name, values=[float(x) if x is not None else None for x in col_data], dtype=dtype)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else None for x in col_data], dtype=dtype)

    return pl.DataFrame(data)

def fetch_personnel_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch only records with personnel data for processing"""
    query = """
        SELECT __path, personnel
        FROM alib
        WHERE personnel IS NOT NULL AND TRIM(personnel) != ''
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]
        data[name] = pl.Series(name=name, values=[str(x) if x is not None else None for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Personnel Processing Functions ----------
def process_personnel_to_role_columns(df: pl.DataFrame, role_columns: list[str]) -> tuple[pl.DataFrame, set]:
    """Process personnel data and extract into role-based columns"""

    group_split_pattern = re.compile(r'\s*-\s*')
    all_unmapped_roles = set()

    def extract_role_data(personnel_str: str) -> dict:
        """Extract role-based data from personnel string"""
        if not personnel_str or not personnel_str.strip():
            return {}

        role_to_names = defaultdict(set)
        unmapped_roles = set()

        # Split into name/roles groups
        groups = group_split_pattern.split(personnel_str.strip())
        for group in groups:
            if not group:
                continue
            parts = [p.strip() for p in group.split(',')]
            if not parts:
                continue

            name = parts[0]
            raw_roles = parts[1:]

            for raw_role in raw_roles:
                if not raw_role:
                    continue

                mapped_role = ROLE_MAPPING.get(raw_role)
                if mapped_role:
                    # Handle compound roles (delimited by \\)
                    if DELIM in mapped_role:
                        roles = mapped_role.split(DELIM)
                        for role in roles:
                            role_to_names[role.strip()].add(name)
                    else:
                        role_to_names[mapped_role].add(name)
                else:
                    unmapped_roles.add(raw_role)

        all_unmapped_roles.update(unmapped_roles)

        # Convert to dictionary with delimited strings
        result = {}
        for role in role_columns:
            if role in role_to_names:
                names = sorted(role_to_names[role])
                result[role] = DELIM.join(names)
            else:
                result[role] = None

        return result

    # Process each row to extract role data
    role_data_list = []
    for personnel_str in df["personnel"].to_list():
        role_data = extract_role_data(personnel_str)
        role_data_list.append(role_data)

    # Create new dataframe with role columns
    result_data = {"__path": df["__path"].to_list()}

    for role in role_columns:
        result_data[role] = [row_data.get(role) for row_data in role_data_list]

    result_df = pl.DataFrame(result_data, schema={"__path": pl.Utf8, **{role: pl.Utf8 for role in role_columns}})

    return result_df, all_unmapped_roles

# ---------- Store Unmapped Roles ----------
def store_unmapped_roles(conn: sqlite3.Connection, unmapped_roles: set):
    """Store unmapped roles in the database for tracking"""
    if not unmapped_roles:
        return

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unmapped_roles (
            role TEXT PRIMARY KEY
        )
    """)

    conn.executemany(
        "INSERT OR IGNORE INTO unmapped_roles (role) VALUES (?)",
        [(role,) for role in sorted(unmapped_roles)]
    )

    logging.info(f"Stored {len(unmapped_roles)} unmapped roles")

# ---------- Create and Update Database ----------
def create_alib2_table(conn: sqlite3.Connection, df: pl.DataFrame, role_columns: list[str], unmapped_roles: set) -> int:
    """Create alib2 table with all data plus new role columns"""

    timestamp = datetime.now(timezone.utc).isoformat()

    # Store unmapped roles
    store_unmapped_roles(conn, unmapped_roles)

    # Create alib2 table by dropping if exists and recreating
    conn.execute("DROP TABLE IF EXISTS alib2")

    # Get original alib schema for proper column types
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(alib)")
    schema_info = cursor.fetchall()

    original_column_defs = {}
    for row in schema_info:
        col_name = row[1]  # column name
        col_type = row[2]  # column type
        not_null = row[3]  # not null
        default_val = row[4]  # default value
        pk = row[5]  # primary key

        # Build column definition
        col_def = f"{col_name} {col_type}"
        if not_null:
            col_def += " NOT NULL"
        if default_val is not None:
            col_def += f" DEFAULT {default_val}"
        if pk:
            col_def += " PRIMARY KEY"

        original_column_defs[col_name] = col_def

    # Build CREATE TABLE statement
    create_sql_parts = []

    # Add original columns with their original definitions
    original_columns = [col for col in df.columns if col not in role_columns]
    for col in original_columns:
        if col in original_column_defs:
            create_sql_parts.append(original_column_defs[col])
        else:
            # Fallback if column not found in schema
            create_sql_parts.append(f"[{col}] TEXT")

    # Add role columns as TEXT
    for role in role_columns:
        create_sql_parts.append(f"[{role}] TEXT")

    create_sql = f"CREATE TABLE alib2 ({', '.join(create_sql_parts)})"
    conn.execute(create_sql)

    # Create changelog table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    # Insert data into alib2
    columns_list = list(df.columns)
    placeholders = ', '.join(['?' for _ in columns_list])
    insert_sql = f"INSERT INTO alib2 ({', '.join(columns_list)}) VALUES ({placeholders})"

    rows_data = []
    changelog_data = []
    updated_rows = 0

    for row_dict in df.to_dicts():
        row_values = [row_dict[col] for col in columns_list]
        rows_data.append(tuple(row_values))

        # Check if any role columns have data for changelog
        path_value = row_dict.get("__path")
        for role in role_columns:
            role_value = row_dict.get(role)
            if role_value:  # If role column has data
                changelog_data.append((
                    None,  # alib_rowid (we're using __path as key)
                    f"alib2.{role}",
                    None,  # old_value (new column)
                    role_value,
                    timestamp,
                    SCRIPT_NAME
                ))
                updated_rows += 1

    # Execute inserts
    conn.executemany(insert_sql, rows_data)

    # Log changes to changelog
    if changelog_data:
        conn.executemany(
            "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            changelog_data
        )

    logging.info(f"Created alib2 table with {len(df)} rows")
    logging.info(f"Added {len(role_columns)} new role columns")
    logging.info(f"Updated {updated_rows} column values with role data")

    return len(role_columns)

# ---------- Main ----------
def main():
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Get unique role columns in order
        role_columns = get_unique_mapped_roles()
        logging.info(f"Will create {len(role_columns)} role columns: {role_columns[:10]}...")

        # Load personnel data for processing
        personnel_df = fetch_personnel_data(conn)
        logging.info(f"Loaded {personnel_df.height} tracks with personnel data for processing")

        # Process personnel data to extract role information
        role_data_df, unmapped_roles = process_personnel_to_role_columns(personnel_df, role_columns)

        if unmapped_roles:
            logging.info(f"Found {len(unmapped_roles)} unmapped roles: {sorted(list(unmapped_roles))[:10]}...")

        # Load complete alib table
        full_alib_df = fetch_full_alib_data(conn)
        logging.info(f"Loaded complete alib table with {full_alib_df.height} rows and {len(full_alib_df.columns)} columns")

        # Merge role data with full alib data
        final_df = full_alib_df.join(role_data_df, on="__path", how="left")

        # Create alib2 table with all data
        conn.execute("BEGIN TRANSACTION")
        added_columns = create_alib2_table(conn, final_df, role_columns, unmapped_roles)
        conn.commit()

        logging.info(f"Successfully created alib2 table with {added_columns} new role columns added")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error occurred: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
