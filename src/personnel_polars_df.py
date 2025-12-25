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
5. Creates parsed_roles table with extracted role data
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
# ROLE_MAPPING = {
#     'Arranger': 'arranger',
#     'Instrumentation': 'arranger',
#     'MusicalAdviser': 'arranger',
#     'Orchestral Arranger': 'arranger',
#     'RecordingArranger': 'arranger',
#     'String Arranger': 'arranger',
#     'StringArranger': 'arranger',
#     'Strings Arranger': 'arranger',
#     'Vocal Arranger': 'arranger',
#     'VocalArranger': 'arranger',
#     'WorkArranger': 'arranger',
#     'Adapter': 'arranger',
#     'Artist': 'artist',
#     'Contributor': 'artist',
#     'Mixed Artist': 'artist',
#     'Performance': 'artist',
#     'Performed by': 'artist',
#     'Performer': 'artist',
#     'BackgroundVocalist': 'artist',
#     'Background Vocals': 'artist',
#     'Background Vocal': 'artist',
#     'BackgroundVocals': 'artist',
#     'Backing Vocals': 'artist',
#     'Banjo': 'banjoist',
#     'Bass': 'bassist',
#     'Bass Guitar': 'bassist',
#     'Bass Programmer': 'bassist',
#     'Bass Synthesizer': 'bassist',
#     'Double Bass': 'bassist',
#     'DoubleBass': 'bassist',
#     'Electric Bass': 'bassist',
#     'Electric Bass Guitar': 'bassist',
#     'Synthbass': 'bassist',
#     'Synth Bass': 'bassist',
#     'Upright Bass': 'bassist',
#     'UprightBass': 'bassist',
#     'Bass guitar (all types)': 'bassist',
#     'BassGuitar': 'bassist',
#     'BassClarinet': 'clarinetist',
#     'Clarinet': 'clarinetist',
#     'Bells': 'percussionist',
#     'AfricanPercussion': 'percussionist',
#     'Bongo Drums': 'percussionist',
#     'Chimes': 'percussionist',
#     'Claps': 'percussionist',
#     'Congas': 'percussionist',
#     'Cowbell': 'percussionist',
#     'Cowbell Percussion': 'percussionist',
#     'Cymbals': 'percussionist',
#     'Glockenspiel': 'percussionist',
#     'Kalimba Percussion': 'percussionist',
#     'Percussion instrument': 'percussionist',
#     'Percussion': 'percussionist',
#     'Shaker': 'percussionist',
#     'Stick Percussion': 'percussionist',
#     'Tambourine': 'percussionist',
#     'Timbales Drums': 'percussionist',
#     'Vibraphone': 'percussionist',
#     'Wood Block Percussion': 'percussionist',
#     'Bandoneon': 'bandoneonist',
#     'Bouzouki': 'bouzouki',
#     'Accordion': 'accordionist',
#     'Cello': 'cellist',
#     'Cornet': 'cornetist',
#     'CoProducer': 'producer',
#     'Composer': 'composer',
#     'ComposerLyricist': 'composer',
#     'Music': 'composer',
#     'Conductor': 'conductor',
#     'MusicDirector': 'conductor',
#     'Orchestral Conductor': 'conductor',
#     'Assistant': 'engineer',
#     'Assistant Engineer': 'engineer',
#     'AssistantEngineer': 'engineer',
#     'Assistant Mastering Engineer': 'engineer',
#     'Recording Assistant': 'engineer',
#     'Recording Second Engineer': 'engineer',
#     'RecordingSecondEngineer': 'engineer',
#     'Assistant Recording Engineer': 'engineer',
#     'Assistant Mixer': 'mixer',
#     'Assistant Mixing Engineer': 'mixer',
#     'AssistantMixingEngineer': 'mixer',
#     'MixingSecondEngineer': 'mixer',
#     'AssistantProducer': 'producer',
#     'ProductionAssistant': 'producer',
#     'Author': 'composer',
#     'Writer': 'composer',
#     'Dobro': 'dobro',
#     'DobroGuitar': 'dobro',
#     'Drum': 'drummer',
#     'DrumKit': 'drummer',
#     'Drum Machine Drums': 'drummer',
#     'Drum Programmer': 'drummer',
#     'DrumProgrammer': 'drummer',
#     'DrumProgramming': 'drummer',
#     'Drums': 'drummer',
#     'Snare Drum': 'drummer',
#     'Drums & Keyboards': 'drummer\\\\keyboardist',
#     'Editing Engineer': 'engineer',
#     'Electric Guitar': 'guitarist',
#     'ElectricGuitar': 'guitarist',
#     'Guitar (acoustic)': 'guitarist',
#     'Guitar (any type)': 'guitarist',
#     'Guitar (electric)': 'guitarist',
#     'Guitar': 'guitarist',
#     'Nylon': 'guitarist',
#     'Nylon Strung Guitar': 'guitarist',
#     'Rhodes Guitar': 'guitarist',
#     'Rhythm Guitar': 'guitarist',
#     'Slide Guitar': 'guitarist',
#     'SlideGuitar': 'guitarist',
#     'Solo Guitar': 'guitarist',
#     '12 String Guitar': 'guitarist',
#     'Acoustic Guitar': 'guitarist',
#     'Baritone Guitar': 'guitarist',
#     'BaritoneGuitar': 'guitarist',
#     'Hohner Guitaret Guitar': 'guitarist',
#     'Lead Guitar': 'guitarist',
#     'AcousticGuitar': 'guitarist',
#     'Keyboard & Bass': 'keyboardist\\\\bassist',
#     'Keyboard': 'keyboardist',
#     'Keyboards': 'keyboardist',
#     'Mellotron': 'keyboardist',
#     'ModularSynth': 'keyboardist',
#     'Omnichord Synthesizer': 'keyboardist',
#     'Organ': 'keyboardist',
#     'Piano': 'keyboardist',
#     'Prepared Piano': 'keyboardist',
#     'Rhodes': 'keyboardist',
#     'Rhodes Piano': 'keyboardist',
#     'Rhodes Solo': 'keyboardist',
#     'Synthesizer': 'keyboardist',
#     'SynthPad': 'keyboardist',
#     'WurlitzerElectricPiano': 'keyboardist',
#     'Wurlitzer': 'keyboardist',
#     'Wurlitzer Piano': 'keyboardist',
#     'AdditionalKeyboard': 'keyboardist',
#     'Clavinet': 'keyboardist',
#     'Electric organ': 'keyboardist',
#     'HammondB3': 'keyboardist',
#     'Hammond B3 Organ': 'keyboardist',
#     'Hammond Organ': 'keyboardist',
#     'Harpsichord': 'keyboardist',
#     'Juno Synthesizer': 'keyboardist',
#     'DigitalPiano': 'keyboardist',
#     'HammondOrgan': 'organist',
#     'Harp': 'harpist',
#     'Horn': 'hornist',
#     'Lead Vocalist': 'artist',
#     'LeadVocals': 'artist',
#     'Mandolin': 'mandolinist',
#     'Organistrum': 'vielleur',
#     'AssociatedPerformer': 'artist',
#     'Featured Vocals': 'artist',
#     'BassVocalist': 'artist',
#     'Vocal accompaniment': 'artist',
#     'Vocalist': 'artist',
#     'Vocals': 'artist',
#     'Vocal': 'artist',
#     'Voice': 'artist',
#     'Lead Vocals': 'artist',
#     'Flute': 'flutist',
#     'Fiddle': 'fiddler',
#     'Harmonica': 'harmonicist',
#     'Lyricist': 'lyricist',
#     'Lyrics': 'lyricist',
#     'Songwriter': 'composer',
#     'Oboe': 'oboist',
#     'Orchestra': 'ensemble',
#     'String Orchestra': 'ensemble',
#     'StringQuartet': 'ensemble',
#     'Ensemble': 'ensemble',
#     'Steel Guitar': 'guitarist',
#     'SteelGuitar': 'guitarist',
#     'Pedal Steel Guitar': 'guitarist',
#     'Viola': 'violist',
#     'Violin': 'violinist',
#     'Whistle': 'whistler',
#     'Woodwinds': 'instrumentalist',
#     'String instrument': 'instrumentalist',
#     'Strings': 'instrumentalist',
#     'Trumpet': 'trumpeter',
#     'Trombone': 'trombonist',
#     'Tenor Saxophone': 'saxophonist',
#     'TenorSaxophone': 'saxophonist',
#     'Saxophone': 'saxophonist',
#     'SoundEffects': 'programmer',
#     'Programmer': 'programmer',
#     'ProgrammingEngineer': 'programmer',
#     'Programming': 'programmer',
#     'Sampler': 'programmer',
#     'Sequencer': 'programmer',
#     'Drum Machine': 'programmer',
#     'Additional Engineer': 'engineer',
#     'Additional Masterer': 'engineer',
#     'Additional Production': 'producer',
#     'Additional Recorder': 'engineer',
#     'Additional Vocal Recording Engineer': 'engineer',
#     'AdditionalEngineer': 'engineer',
#     'AltoRecorder': 'flautist',
#     'Archival Producer': 'producer',
#     'Artist background vocal engineer': 'engineer',
#     'AssociateProducer': 'producer',
#     'Audio Mastering': 'engineer',
#     'Audio Recording Engineer': 'engineer',
#     'BalanceEngineer': 'engineer',
#     'DigitalEditingEngineer': 'engineer',
#     'Electronics': 'programmer',
#     'Engineer': 'engineer',
#     'Engineer & Mixer': 'engineer\\\\mixer',
#     'Executive Producer': 'producer',
#     'ExecutiveProducer': 'producer',
#     'FeaturedArtist': 'artist',
#     'ImmersiveMixingEngineer': 'engineer',
#     'Instruments': 'instrumentalist',
#     'Lute': 'lutist',
#     'MainArtist': 'albumartist',
#     'Masterer': 'engineer',
#     'Mastering Engineer': 'engineer',
#     'MasteringEngineer': 'engineer',
#     'Mixer': 'mixer',
#     'Mixing Engineer': 'engineer',
#     'MixingEngineer': 'engineer',
#     'MusicPublisher': 'label',
#     'OrchestraContractor': 'ensemble',
#     'Overdub Engineer': 'engineer',
#     'Producer': 'producer',
#     'Production': 'producer',
#     'Recorded by': 'engineer',
#     'Recording': 'engineer',
#     'Recording & Mixing': 'engineer',
#     'Recording Engineer': 'engineer',
#     'RecordingEngineer': 'engineer',
#     'Sound Engineer': 'engineer',
#     'Sound Restoration Engineer': 'engineer',
#     'SoundEngineer': 'engineer',
#     'SoundRecordist': 'engineer',
#     'Studio': 'recordinglocation',
#     'StudioMusician': 'artist',
#     'StudioPersonnel': 'artist',
#     'StudioProducer': 'producer',
#     'Technical Engineer': 'engineer',
#     'Tracking Engineer': 'engineer',
#     'Vocal Engineer': 'engineer',
#     'Vocal Recording Engineer': 'engineer',
#     'VocalEditingEngineer': 'engineer',
#     'VocalEngineer': 'engineer',
#     'VocalProducer': 'producer',
#     'ReissueProducer': 'producer'
# }

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

    # Fetch the data including rowid
    cursor.execute("SELECT rowid, * FROM alib")
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Build the dataframe with proper types
    data = {}
    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]
        
        # Handle rowid specially - it's always INTEGER
        if name == 'rowid':
            dtype = pl.Int64
        else:
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
        SELECT rowid, personnel
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
        if name == 'rowid':
            data[name] = pl.Series(name=name, values=[int(x) if x is not None else None for x in col_data], dtype=pl.Int64)
        else:
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

    def has_mapped_roles(role_data: dict) -> bool:
        """Check if role_data contains any non-None values"""
        return any(value is not None for value in role_data.values())

    # Process each row to extract role data and filter out empty rows
    role_data_list = []
    valid_rowids = []
    
    for i, personnel_str in enumerate(df["personnel"].to_list()):
        role_data = extract_role_data(personnel_str)
        if has_mapped_roles(role_data):
            role_data_list.append(role_data)
            valid_rowids.append(df["rowid"].to_list()[i])

    # Create new dataframe with role columns only for rows with mapped roles
    result_data = {"rowid": valid_rowids}

    for role in role_columns:
        result_data[role] = [row_data.get(role) for row_data in role_data_list]

    result_df = pl.DataFrame(result_data, schema={"rowid": pl.Int64, **{role: pl.Utf8 for role in role_columns}})

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

# ---------- Data Integration Functions ----------
def integrate_parsed_roles_into_alib(alib_df: pl.DataFrame, parsed_roles_df: pl.DataFrame, role_columns: list[str]) -> tuple[pl.DataFrame, list]:
    """Integrate parsed_roles data into alib_df and track changes"""
    
    # Get columns that appear in parsed_roles but not in alib_df (excluding rowid)
    missing_columns = [col for col in parsed_roles_df.columns if col != 'rowid' and col not in alib_df.columns]
    
    # Add missing columns to alib_df as TEXT (Utf8) columns
    if missing_columns:
        logging.info(f"Adding {len(missing_columns)} missing columns to alib_df: {missing_columns[:10]}...")
        for col in missing_columns:
            alib_df = alib_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
    
    # Convert to dictionaries for easier processing
    alib_dict = {row['rowid']: row for row in alib_df.to_dicts()}
    parsed_roles_dict = {row['rowid']: row for row in parsed_roles_df.to_dicts()}
    
    changelog_entries = []
    changed_rowids = set()
    timestamp = datetime.now(timezone.utc).isoformat()
    
    def split_delimited_value(value):
        """Split delimited value into list, handling None and empty strings"""
        if not value or value.strip() == "":
            return []
        return [item.strip() for item in value.split(DELIM) if item.strip()]
    
    def case_insensitive_exists(item, item_list):
        """Check if item exists in list (case insensitive)"""
        return item.lower() in [existing_item.lower() for existing_item in item_list]
    
    # Process each row in parsed_roles
    for rowid, parsed_row in parsed_roles_dict.items():
        if rowid not in alib_dict:
            continue  # Skip if rowid not in alib
            
        alib_row = alib_dict[rowid]
        row_changed = False
        
        # Check each role column
        for role_col in role_columns:
            parsed_value = parsed_row.get(role_col)
            if not parsed_value:  # Skip if no value in parsed_roles
                continue
                
            alib_value = alib_row.get(role_col)
            
            # Split values into lists
            parsed_items = split_delimited_value(parsed_value)
            alib_items = split_delimited_value(alib_value)
            
            # Find items in parsed_roles that don't exist in alib (case insensitive)
            new_items = [item for item in parsed_items if not case_insensitive_exists(item, alib_items)]
            
            if new_items:
                # Combine existing and new items
                combined_items = alib_items + new_items
                new_value = DELIM.join(combined_items)
                
                # Update the row
                old_value = alib_value if alib_value else None
                alib_row[role_col] = new_value
                row_changed = True
                
                # Record changelog entry
                changelog_entries.append({
                    'alib_rowid': rowid,
                    'column': f'alib_updates.{role_col}',
                    'old_value': old_value,
                    'new_value': new_value,
                    'timestamp': timestamp,
                    'script': SCRIPT_NAME
                })
        
        if row_changed:
            # Increment sqlmodded
            current_sqlmodded = alib_row.get('sqlmodded', 0)
            if current_sqlmodded is None:
                current_sqlmodded = 0
            alib_row['sqlmodded'] = current_sqlmodded + 1
            changed_rowids.add(rowid)
    
    # Convert back to DataFrame
    updated_rows = [alib_dict[rowid] for rowid in alib_dict.keys()]
    
    # Reconstruct the DataFrame with proper types
    result_data = {}
    original_schema = alib_df.schema
    
    for col_name in alib_df.columns + missing_columns:
        col_values = [row.get(col_name) for row in updated_rows]
        
        if col_name in original_schema:
            dtype = original_schema[col_name]
        else:
            dtype = pl.Utf8  # New columns are TEXT
            
        if dtype == pl.Int64:
            result_data[col_name] = pl.Series(name=col_name, values=[int(x) if x is not None else None for x in col_values], dtype=dtype)
        elif dtype == pl.Float64:
            result_data[col_name] = pl.Series(name=col_name, values=[float(x) if x is not None else None for x in col_values], dtype=dtype)
        else:
            result_data[col_name] = pl.Series(name=col_name, values=[str(x) if x is not None else None for x in col_values], dtype=dtype)
    
    updated_df = pl.DataFrame(result_data)
    
    return updated_df, changelog_entries, changed_rowids

def create_alib_updates_table(conn: sqlite3.Connection, df: pl.DataFrame, changed_rowids: set, changelog_entries: list):
    """Create alib_updates table with only changed rows"""
    
    if not changed_rowids:
        logging.info("No rows were changed, skipping alib_updates table creation")
        return
    
    # Filter to only changed rows
    changed_df = df.filter(pl.col('rowid').is_in(list(changed_rowids)))
    
    # Drop existing alib_updates table and recreate
    conn.execute("DROP TABLE IF EXISTS alib_updates")
    
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
        col_def = f"[{col_name}] {col_type}"
        if not_null:
            col_def += " NOT NULL"
        if default_val is not None:
            col_def += f" DEFAULT {default_val}"
        if pk:
            col_def += " PRIMARY KEY"
            
        original_column_defs[col_name] = col_def
    
    # Build CREATE TABLE statement
    create_sql_parts = []
    
    for col in changed_df.columns:
        if col in original_column_defs:
            create_sql_parts.append(original_column_defs[col])
        else:
            # New columns as TEXT
            create_sql_parts.append(f"[{col}] TEXT")
    
    create_sql = f"CREATE TABLE alib_updates ({', '.join(create_sql_parts)})"
    conn.execute(create_sql)
    
    # Insert changed rows
    columns_list = list(changed_df.columns)
    placeholders = ', '.join(['?' for _ in columns_list])
    insert_sql = f"INSERT INTO alib_updates ({', '.join(f'[{col}]' for col in columns_list)}) VALUES ({placeholders})"
    
    rows_data = []
    for row_dict in changed_df.to_dicts():
        row_values = [row_dict[col] for col in columns_list]
        rows_data.append(tuple(row_values))
    
    conn.executemany(insert_sql, rows_data)
    
    # Insert changelog entries
    if changelog_entries:
        conn.executemany(
            "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            [(entry['alib_rowid'], entry['column'], entry['old_value'], 
              entry['new_value'], entry['timestamp'], entry['script']) for entry in changelog_entries]
        )
    
    logging.info(f"Created alib_updates table with {len(changed_df)} changed rows")
    logging.info(f"Added {len(changelog_entries)} changelog entries")

# ---------- Create parsed_roles Table ----------
def create_parsed_roles_table(conn: sqlite3.Connection, df: pl.DataFrame, role_columns: list[str], unmapped_roles: set) -> int:
    """Create parsed_roles table with role data"""

    timestamp = datetime.now(timezone.utc).isoformat()

    # Store unmapped roles
    store_unmapped_roles(conn, unmapped_roles)

    # Create parsed_roles table by dropping if exists and recreating
    conn.execute("DROP TABLE IF EXISTS parsed_roles")

    # Build CREATE TABLE statement
    create_sql_parts = ["rowid INTEGER PRIMARY KEY"]

    # Add role columns as TEXT
    for role in role_columns:
        create_sql_parts.append(f"[{role}] TEXT")

    create_sql = f"CREATE TABLE parsed_roles ({', '.join(create_sql_parts)})"
    conn.execute(create_sql)

    # Create changelog table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            alib_column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    # Insert data into parsed_roles
    columns_list = list(df.columns)
    placeholders = ', '.join(['?' for _ in columns_list])
    insert_sql = f"INSERT INTO parsed_roles ({', '.join(columns_list)}) VALUES ({placeholders})"

    rows_data = []
    changelog_data = []
    updated_rows = 0

    for row_dict in df.to_dicts():
        row_values = [row_dict[col] for col in columns_list]
        rows_data.append(tuple(row_values))

        # Check if any role columns have data for changelog
        rowid_value = row_dict.get("rowid")
        for role in role_columns:
            role_value = row_dict.get(role)
            if role_value:  # If role column has data
                changelog_data.append((
                    rowid_value,
                    f"parsed_roles.{role}",
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
            "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            changelog_data
        )

    logging.info(f"Created parsed_roles table with {len(df)} rows")
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

        # Create parsed_roles table with role data
        conn.execute("BEGIN TRANSACTION")
        added_columns = create_parsed_roles_table(conn, role_data_df, role_columns, unmapped_roles)
        conn.commit()

        logging.info(f"Successfully created parsed_roles table with {added_columns} new role columns added")

        # Load complete alib table
        alib_updates = fetch_full_alib_data(conn)
        logging.info(f"Loaded complete alib table with {alib_updates.height} rows and {len(alib_updates.columns)} columns")

        # Integrate parsed_roles data into alib_updates
        conn.execute("BEGIN TRANSACTION")
        updated_alib, changelog_entries, changed_rowids = integrate_parsed_roles_into_alib(alib_updates, role_data_df, role_columns)
        
        # Create alib_updates table with only changed rows
        create_alib_updates_table(conn, updated_alib, changed_rowids, changelog_entries)
        conn.commit()

        logging.info(f"Successfully integrated parsed_roles data. {len(changed_rowids)} rows were updated.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error occurred: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
