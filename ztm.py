import argparse
from collections import OrderedDict
import csv
import os
from os.path import exists, dirname
import pandas as pd
import numpy as np
from string_grouper import match_strings, match_most_similar, \
    group_similar_strings, compute_pairwise_similarities, \
    StringGrouper
import re
import sqlite3
import sys
import time
import uuid



''' function to clear screen '''
cls = lambda: os.system('clear')

def firstlettercaps(s):
    ''' returns first letter caps for each word but respects apostrophes '''
    return re.sub(r"[A-Za-z]+('[A-Za-z]+)?", lambda mo: mo.group(0)[0].upper() + mo.group(0)[1:].lower(), s)


def us_state(s):

    return s.upper() in ['AL',
                        'AK',
                        'AZ',
                        'AR',
                        'CA',
                        'CZ',
                        'CO',
                        'CT',
                        'DE',
                        'DC',
                        'FL',
                        'GA',
                        'GU',
                        'HI',
                        'ID',
                        'IL',
                        'IN',
                        'IA',
                        'KS',
                        'KY',
                        'LA',
                        # 'ME',
                        'MD',
                        'MA',
                        'MI',
                        'MN',
                        'MS',
                        'MO',
                        'MT',
                        'NE',
                        'NV',
                        'NH',
                        'NJ',
                        'NM',
                        'NY',
                        'NC',
                        'ND',
                        'OH',
                        'OK',
                        'OR',
                        'PA',
                        'PR',
                        'RI',
                        'SC',
                        'SD',
                        'TN',
                        'TX',
                        'UT',
                        'VT',
                        'VI',
                        'VA',
                        'WA',
                        'WV',
                        'WI',
                        'WY']


def title_case(value):
    # turns a word into Title Case and takes care of numbering and apostrophes

    titled = value.title()
    titled = re.sub(r"([a-z])'([A-Z])", lowercase_match, titled)  # Fix Don'T
    titled = re.sub(r"\d([A-Z])", lowercase_match, titled)  # Fix 1St and 2Nd
    return titled

def lowercase_match(match):
    """Lowercase the whole regular expression match group."""
    return match.group().lower()

def replace_demimiters(string, entity = ''):

    # Define a regular expression pattern to match comma, forward slash, or semicolon
    # if artist or albumartist do not include & in splitting logic
    if entity in ('artist', 'albumartist'):
        pattern = r'[,\;/]'
    else:
        pattern = r'[,\;/&]'

    # Replace occurrences of the pattern with double backslash
    replaced_string = re.sub(pattern, r'\\\\', string)

    # Remove spaces immediately before and after the double backslash
    replaced_string = re.sub(r'\s*\\\\\s*', r'\\\\', replaced_string)

    return replaced_string


def first_alpha(string):
    # returns the pos of the first alpha char in a string, else -1
    match = re.search(r'[a-zA-Z]', string)
    if match:
        return match.start()
    else:
        return -1

def last_alpha(string):
    # returns the pos of the last alpha char in a string, else -1
    last_alpha = -1
    for i, c in enumerate(string):
        if c.isalpha():
            last_alpha = i

    return last_alpha


def capitalise_first_alpha(s):
    # Capitalises first alpha char in s

    for i, c in enumerate(s):

        if c.isalpha():

            tmp = s[:i] + c.upper() + s[i+1:]
            if always_upper(tmp) or us_state(tmp):
                tmp = tmp.upper()
            return tmp
    return s

# stuff intended to handle single words

def is_roman_numeral(word):
    first_char = first_alpha(word)
    if first_char > -1: # i.e. there is indeed at least one alpha char in string
        last_char = last_alpha(word) + 1
        word = word[first_char:last_char]

    ''' determines whether word passed is a roman numeral within the stricter meaning of the term and returns it properly formatted '''
    return bool(re.match(r'^(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})$', word.upper()))

# def always_upper(word):
#     '''determines whether word is in list of words that will always be uppercase'''
#     return word.upper() in ('ABBA', 'BBC', 'BMG', 'EP', 'EU', 'FM', 'LP', 'MFSL', 'MOFI', 'MTV','NRG', 'NYC', 'UDSACD', 'UMG','USA', 'U.S.A.')

# def always_upper(word):
    
#     '''determines whether word is in list of words that will always be uppercase after stripping out the last char where it's a bracket'''
#     # consider using if not word[-1].isalpha() to catch all instances of a word  e.g. USA1, USA- etc.
#     if word[-1] in (')', ']', '}'):
#         word = word[:len(word)-1]
        
#     return word.upper() in ('ABBA', 'BBC', 'BMG', 'EP', 'EU', 'FM', 'LP', 'MFSL', 'MOFI', 'NRG', 'NYC', 'UDSACD', 'UMG','USA', 'U.S.A.')

def always_upper(word):
    '''determines whether word is in list of words that will always be uppercase after stripping out non-alpha chars at the beginning and end of the string '''    
    first_char = first_alpha(word)
    if first_char > -1: # i.e. there is indeed at least one alpha char in string
        last_char = last_alpha(word) + 1
        word = word[first_char:last_char]
       
    return word.upper() in ('ABBA', 'AFZ', 'BBC', 'BMG', 'EP', 'EU', 'FM', 'HBO', 'KCRW', 'LP', 'MFSL', 'MOFI', 'MTV', 'NRG', 'NYC', 'UDSACD', 'UMG','USA', 'U.S.A.')





def capitalise_first_word(sentence):
    # capitalises the first word in a string passed to it

    if not sentence:  # empty sentence check
        return ''

    words = sentence.split()
    first_word  = sentence[0]

    if first_word and re.match(r'(:|\?|!|\}|\—|\(|\)|"| )', first_word):

        first_word = capitalise_word(first_word)

    return ' '.join(words)

def capitalise_last_word(sentence):
    # capitalises the last word in a string passed to it
    if not sentence:  # empty sentence check
        return ''

    words = sentence.split()
    *_, lastword = words

    if lastword and re.match(r'(:|\?|!|\}|\—|\(|\)|"| )', lastword):

        lastword = capitalise_word(lastword)
    
    return ' '.join(words)


def capitalise_word(word):
    ''' loose implementation of RYM's capitalisation standards '''

    if word.lower() in ['a', 'an', 'and', 'at', 'but', 'by', 'cetera ', 'et', 'etc.', 'for', 'in', 'nor', 'of', 'on', 'or', 'the', 'to', 'v.', 'versus', 'vs.', 'yet']:
        return word.lower()
    # elif word.lower() in ['am', 'are', 'as', 'be', 'been', 'from', 'he', 'if', 'into', 'is', 'it', 'she', 'so', 'upon', 'was', 'we', 'were', 'with']:
    #     return word.capitalize()
    elif word.lower() == 'khz':
        return 'kHz'
    elif word.lower() ==  'khz]':        
        return 'kHz]'
    elif word.lower() ==  '10cc':        
        return '10cc'
    elif is_roman_numeral(word) or always_upper(word) or us_state(word):
        return word.upper()
    else:
        # if it doesn't meet any of thse special conditions. capitalise it taking into account first aplha character as capitalisation candidate
        # return capitalise_first_alpha(word)
        return capitalise_first_alpha(word.capitalize())


# this handles the full string

def rymify(sentence):
    ''' Breaks a sentence down into words and capitalises each according to capitalise_word() '''

    if not sentence:  # empty sentence check
        return ''

    parts = re.split(r'(:|\?|!|\—|\(|\)|"| )', sentence)
    for i in range(len(parts)):
        if parts[i] and not re.match(r'(:|\?|!|\—|\(|\)|"|&| )', parts[i]):

            parts[i] = capitalise_word(parts[i])
    
    # Join parts while maintaining original spacing
    capitalised_sentence = ''.join(parts)
    
    # Capitalize first and last word
    capitalised_sentence = capitalise_first_alpha(capitalise_last_word(capitalised_sentence))

    return capitalised_sentence

def trim_whitespace(string):
    ''' get rid of multiple spaces between characters in strings '''
    return " ".join(string.split())

def sanitize_dirname(dirname):
    """
    Sanitize a file path by removing illegal characters.
    ''.strip() basically means that nothing is returned to replace the illegal char
    """
    illegal_chars = '#%{}\\<>*?$":@+`|='
    sanitized_dirname = ''.join(char if char not in illegal_chars else ''.strip() for char in dirname)
    return sanitized_dirname

def sanitize_filename(filename):
    """
    Sanitize a file path by removing illegal characters.
    ''.strip() basically means that nothing is returned to replace the illegal char
    """
    illegal_chars = '#%{}\\<>*?/$":@+`|='
    sanitized_filename = ''.join(char if char not in illegal_chars else ''.strip() for char in filename)
    return sanitized_filename


def pad_text(text, pad_len = 2):
    ''' pad incoming text to padlen '''
    padding_prefix = '0'
    if len(text) < pad_len:
        text = padding_prefix * ((pad_len - len(text)) // len(padding_prefix)) + text
    return text


def table_exists(table_name):
    ''' test whether table exists in a database '''
    dbcursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    #if the count is 1, then table exists
    return (dbcursor.fetchone()[0] == 1)


def get_columns(table_name):
    ''' return the list of columns in a table '''
    dbcursor.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}');")
    return(dbcursor.fetchall())


def tag_in_table(tag, table_name):
    ''' check if tag exists in table '''
    dbcursor.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}');")
    dbtags = dbcursor.fetchall()
    ''' generate a list of the first element of each tuple in the list of tuples that is dbtags '''
    dblist = list(zip(*dbtags))[0]
    ''' build list of matching tagnames in dblist '''
    return(tag in dblist)


def dedupe_and_sort(input_string, delimiter=r'\\'):
    ''' get a list items that contains a delimited string, dedupe and sort it and pass it back '''
    distinct_items = set(x.strip() for x in input_string.split(delimiter))
    return delimiter.join(sorted(distinct_items))


def eliminate_duplicates_ordered_dict(input_string):
    ''' utility function to elimiate duplicate words from a string whilst preserving the order of the string.  If order wasn't important set() would be faster '''

    word_list = input_string.split()
    unique_words = list(OrderedDict.fromkeys(word_list))
    return ' '.join(unique_words)    


def delete_repeated_phrase2(s, phrase):

    if phrase == None:
        return s
    j = s.find(phrase)
    if j >= 0:
        k = j + len(phrase)
        s = s[:k] + s[k:].replace(phrase, "")
    return s



def delete_repeated_phrase(sentence, phrase, lastonly = False):
    ''' deletes all but the first instance of phrase from sentence, unless lastonly == True.  Pass True if you only want to remove the last instance of a phrase from sentence '''

    # first count the number of instances of a phrase in the sentence, if no occurences, return the original sentence
    if sentence.count(phrase) == 0:
        return sentence

    # get phrase length
    phrase_len = len(phrase)
        
    # reverse the string because we want to remove the phrase from end of sentence to start of sentence
    reversed_sentence = sentence[::-1]
    reversed_phrase = phrase[::-1]

    if lastonly:
        # slice string to remove the first occurence of the phrase
        index = reversed_sentence.find(reversed_phrase)
        
        reversed_sentence = reversed_sentence[0:index] + reversed_sentence[index + 1 + phrase_len:]
        new_sentence = reversed_sentence[::-1]
        return new_sentence
        
    else:

        # while there remains more than 1 instance of phrase in sentence
        while reversed_sentence.count(reversed_phrase) > 1:
            
            # slice string to remove the first occurence of the phrase
            index = reversed_sentence.find(reversed_phrase)

            reversed_sentence = reversed_sentence[0:index] + reversed_sentence[index + 1 + phrase_len:]

        new_sentence = reversed_sentence[::-1]
        return new_sentence


def get_spurious_items(source, target):
    ''' function to return all items in source that do not appear in target '''
    return [item for item in source if item not in target]


def get_permitted_list(source: list, target: tuple):
    ''' function to return all items in source that appear in target '''
    return sorted(set(source).intersection(target))

def vetted_list_intersection(source: list, target: tuple):
    intersection = []
    s = [x.lower() for x in source]
    for t in target:
        if t.lower() in s:
            intersection.append(t)
    return intersection


def delimited_string_to_list(input_string, delimiter=r'\\'):
    ''' convert delimited string to list and pass it back '''
    return input_string.split(delimiter)


def list_to_delimited_string(input_list: list, delimiter=r'\\'):
    ''' convert a list of items to a delimited string '''
    return delimiter.join(map(str, input_list))

def tally_mods():
    ''' start and stop counter that returns how many changes have been triggered at the point of call - will be >= 0 '''
    dbcursor.execute('SELECT SUM(sqlmodded) FROM alib WHERE sqlmodded IS NOT NULL;')
    matches = dbcursor.fetchone()

    if matches[0] == None:
        ''' sqlite returns null from a sum operation if the field values are null, so test for it, because if the script is run iteratively that'll be the case where alib has been readied for export '''
        return(0)
    return(matches[0])



def changed_records():
    ''' returns how many records have been changed at the point of call - will be >= 0 '''
    dbcursor.execute('SELECT count(sqlmodded) FROM alib;')
    matches = dbcursor.fetchone()
    return (matches[0])



def library_size():
    ''' returns record count in alib '''
    dbcursor.execute('SELECT count(*) FROM alib;')
    matches = dbcursor.fetchone()
    return (matches[0])



def affected_dirpaths():
    ''' get list of all affected __dirpaths '''
    dbcursor.execute('SELECT DISTINCT __dirpath FROM alib where sqlmodded IS NOT NULL;')
    matches = dbcursor.fetchall()
    return(matches)


    
def affected_dircount():
    ''' sum number of distinct __dirpaths with changed content '''    
    return(len(affected_dirpaths()))



def create_indexes():

    ''' set up indexes to be used throughout the script operations'''
    print("Creating table indexes...")
    if table_exists('alib'):
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_filepaths ON alib(__path);''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_artists ON alib (artist) WHERE artist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lartists ON alib (LOWER(artist)) WHERE artist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists ON alib (albumartist) WHERE albumartist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lalbartists ON alib (LOWER(albumartist)) WHERE albumartist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_performers ON alib (performer) WHERE performer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lperformers ON alib (LOWER(performer)) WHERE performer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_composers ON alib (composer) WHERE composer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lcomposers ON alib (LOWER(composer)) WHERE composer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_writers ON alib (writer) WHERE writer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lwriters ON alib (LOWER(writer)) WHERE writer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_titles ON alib(title) WHERE title IS NOT NULL;''')    
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_ltitles ON alib(LOWER(title)) WHERE title IS NOT NULL;''')    
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_genres ON alib(genre) WHERE genre IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_styles ON alib(style) WHERE style IS NOT NULL;''')   


def establish_environment():
    ''' define tables and fields required for the script to do its work '''

    good_tags = [
    "__accessed",
    "__app",
    "__bitrate",
    "__bitrate_num",
    "__bitspersample",
    "__channels",
    "__created",
    "__dirname",
    "__dirpath",
    "__ext",
    "__file_access_date",
    "__file_access_datetime",
    "__file_access_datetime_raw",
    "__file_create_date",
    "__file_create_datetime",
    "__file_create_datetime_raw",
    "__file_mod_date",
    "__file_mod_datetime",
    "__file_mod_datetime_raw",
    "__file_size",
    "__file_size_bytes",
    "__file_size_kb",
    "__file_size_mb",
    "__filename",
    "__filename_no_ext",
    "__filetype",
    "__frequency",
    "__frequency_num",
    "__image_mimetype",
    "__image_type",
    "__layer",
    "__length",
    "__length_seconds",
    "__md5sig",
    "__mode",
    "__modified",
    "__num_images",
    "__parent_dir",
    "__path",
    "__size",
    "__tag",
    "__tag_read",
    "__vendorstring",
    "__version",
    "tagminder_uuid",    
    "_releasecomment",
    "acousticbrainz_mood",
    "acoustid_fingerprint",
    "acoustid_id",
    "album",
    "album_dr",
    "albumartist",
    "amg_album_id",
    "amg_boxset_url",
    "amg_url",
    "amgtagged",
    "analysis",
    "arranger",
    "artist",
    "asin",
    "barcode",
    "bootleg",
    "catalog",
    "catalognumber",
    "compilation",
    "composer",
    "conductor",
    "country",
    "discnumber",
    "discogs_artist_url",
    "discogs_release_url",
    "discsubtitle",
    "engineer",
    "ensemble",
    "fingerprint",
    "genre",
    "isrc",
    "label",
    "live",
    "lyricist",
    "lyrics",
    "movement",
    "mixer",
    "mood",
    "musicbrainz_albumartistid",
    "musicbrainz_albumid",
    "musicbrainz_artistid",
    "musicbrainz_composerid",
    "musicbrainz_engineerid",
    "musicbrainz_discid",
    "musicbrainz_producerid",
    "musicbrainz_releasegroupid",
    "musicbrainz_releasetrackid",
    "musicbrainz_trackid",
    "musicbrainz_workid",
    "originaldate",
    "originalreleasedate",
    "originalyear",
    "part",
    "performancedate",
    "performer",
    "personnel",
    "producer",
    "rating",
    "recordinglocation",
    "recordingstartdate",
    "reflac",
    "releasetype",
    "remixer",
    "replaygain_album_gain",
    "replaygain_album_peak",
    "replaygain_track_gain",
    "replaygain_track_peak",
    "review",
    "roonalbumtag",
    "roonradioban",
    "roontracktag",
    "roonid",
    "sqlmodded",
    "style",
    "subtitle",
    "theme",
    "title",
    "track",
    "upc",
    "version",
    "work",
    "writer",
    "year"]

    print("Populating permitted tags table...")
    dbcursor.execute('drop table if exists permitted_tags;')
    dbcursor.execute('create table permitted_tags (tagname text);')

    for tag in good_tags:

        dbcursor.execute(f"INSERT INTO permitted_tags ('tagname') VALUES ('{tag}')")

    # create enduring indexes required to operate efficiently
    create_indexes()

    ''' ensure trigger is in place to record incremental changes until such time as tracks are written back '''
    dbcursor.execute("CREATE TRIGGER IF NOT EXISTS sqlmods AFTER UPDATE ON alib FOR EACH ROW WHEN old.sqlmodded IS NULL BEGIN UPDATE alib SET sqlmodded = iif(sqlmodded IS NULL, '1', (CAST (sqlmodded AS INTEGER) + 1) )  WHERE rowid = NEW.rowid; END;")

    ''' alib_rollback is a master copy of alib table untainted by any changes made by this script.  if a rollback table already exists we are applying further changes or imports, so leave it intact '''

    ######################################################################################################
    # TEMP DISABLE
    dbcursor.execute("CREATE TABLE IF NOT EXISTS alib_rollback AS SELECT * FROM alib order by __path;")
    ######################################################################################################



    # check whether there's a musicbrainz entities table containing distinct names and mbid's.  If it exists, leverage it.  Note, this table is the entire musicbrainz master before removing namesakes.
    # Namesakes need to be dealt with manually in userland, it's not something that can be automated as there's no way to reliably distinguish one namesake from another when considering only name
    if table_exists('mb_entities'):
            # index it for speed
            dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lmb_master on mb_entities(lower(entity)) WHERE entity IS NOT NULL;''')

            # given a user may have preferences as to how an artist name is written, check whether any changes need to be made to mb_entities to align with user preference as captured in cleansed_contributors table
            # as an example, most of my tagging over the years leveraged allmusic.com artist names and those are reflected in cleansed_contributors where I've had to make changes to sourced metadata in the past
            if table_exists('cleansed_contributors'):

                dbcursor.execute('''UPDATE mb_entities
                                       SET entity = cleansed_contributors.replacement_val,
                                           updated_from_cleansed_contributors = '1'
                                      FROM cleansed_contributors
                                     WHERE (cleansed_contributors.lreplacement_val == mb_entities.lentity AND 
                                           cleansed_contributors.replacement_val != mb_entities.entity);
                    ''')


            # create a table of namesakes for users to browse if they need to disambiguate
            dbcursor.execute('''DROP TABLE IF EXISTS mb_namesakes;''')
            dbcursor.execute('''CREATE TABLE IF NOT EXISTS mb_namesakes AS SELECT *
                                                                             FROM (
                                                                                  WITH cte AS (
                                                                                          SELECT entity
                                                                                            FROM mb_entities
                                                                                           GROUP BY lower(entity) 
                                                                                          HAVING count() > 1
                                                                                           ORDER BY lower(entity) 
                                                                                      )
                                                                                      SELECT mbid,
                                                                                             entity,
                                                                                             lentity
                                                                                        FROM mb_entities
                                                                                       WHERE entity IN cte
                                                                                  );''')

            # index it for speed
            #dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_mb_namesakes on mb_namesakes(lower(entity)) WHERE entity IS NOT NULL;''')
            dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lmb_namesakes on mb_namesakes(lentity) WHERE entity IS NOT NULL;''')

            # create a table of entity names that only appear once in mb_master for use within tagminder when adding mbid's to artist, albumartist, composer, engineer, producer, label and recordinglocation tags
            dbcursor.execute('''DROP TABLE IF EXISTS mb_disambiguated;''')
            dbcursor.execute('''CREATE TABLE IF NOT EXISTS mb_disambiguated AS SELECT *
                                                                                 FROM (
                                                                                          SELECT mbid,
                                                                                                 entity,
                                                                                                 lentity
                                                                                            FROM mb_entities
                                                                                           GROUP BY lower(entity) 
                                                                                          HAVING count() == 1
                                                                                           ORDER BY lower(entity) 
                                                                                      );''')
            # index it for speed
            #dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_mb_disambiguated on mb_disambiguated(lower(entity)) WHERE entity IS NOT NULL;''')
            dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_mb_ldisambiguated on mb_disambiguated(lentity) WHERE entity IS NOT NULL;''')

    conn.commit()
    return


# genre related functionality module addressing specifics related to genres

def vetted_genre_pool():
    ''' return a tuple of sanctioned genres  - based on allmusic.com 4/11/2023 with some custom genres added '''
    return ("Aboriginal Rock",
        "Acadian",
        "Acappella",
        "Acid Folk",
        "Acid House",
        "Acid Jazz",
        "Acid Rock",
        "Acid Techno",
        "Acoustic Blues",
        "Acoustic Chicago Blues",
        "Acoustic Louisiana Blues",
        "Acoustic Memphis Blues",
        "Acoustic New Orleans Blues",
        "Acoustic Texas Blues",
        "Adult Alternative",
        "Adult Alternative Pop/Rock",
        "Adult Contemporary",
        "Adult Contemporary R&B",
        "Afghanistan",
        "Afoxe",
        "African Folk",
        "African Jazz",
        "African Psychedelia",
        "African Rap",
        "African Traditions",
        "Afrikaans",
        "Afro-beat",
        "Afro-Brazilian",
        "Afro-Colombian",
        "Afro-Cuban",
        "Afro-Cuban Jazz",
        "Afro-Peruvian",
        "Afro-Pop",
        "Afroswing",
        "Al-Jil",
        "Albanian",
        "Album Rock",
        "Algerian",
        "Alpine",
        "Alt-Country",
        "Alterna Movimiento",
        "Alternative CCM",
        "Alternative Corridos",
        "Alternative Country",
        "Alternative Country-Rock",
        "Alternative Dance",
        "Alternative Folk",
        "Alternative Latin",
        "Alternative Metal",
        "Alternative Pop/Rock",
        "Alternative R&B",
        "Alternative Rap",
        "Alternative Singer/Songwriter",
        "Alternative/Indie Rock",
        "AM Pop",
        "Ambient",
        "Ambient Breakbeat",
        "Ambient Dub",
        "Ambient House",
        "Ambient Pop",
        "Ambient Techno",
        "American Jewish Pop",
        "American Popular Song",
        "American Punk",
        "American Trad Rock",
        "American Underground",
        "Americana",
        "Anarchist Punk",
        "Andalus Classical",
        "Andean Folk",
        "Angolan",
        "Anime Music",
        "Anti-Folk",
        "Apala",
        "Appalachian",
        "Arabic",
        "Arena Rock",
        "Argentinian Folk",
        "Armenian",
        "Armenian Folk",
        "Art Rock",
        "Art-Rock/Experimental",
        "Asian Folk",
        "Asian Pop",
        "Asian Psychedelia",
        "Asian Rap",
        "Asian Rock",
        "Asian Traditions",
        "Aussie Rock",
        "Australasian",
        "Australian",
        "Austrian",
        "AustroPop",
        "Avant-Garde",
        "Avant-Garde Jazz",
        "Avant-Garde Metal",
        "Avant-Garde Music",
        "Avant-Prog",
        "Axe",
        "Azerbaijani",
        "Azorean",
        "Bachata",
        "Bahamian",
        "Baile Funk",
        "Bakersfield Sound",
        "Balinese",
        "Balkan",
        "Ballet",
        "Ballroom Dance",
        "Baltic",
        "Bambara",
        "Band Music",
        "Banda",
        "Bangladeshi",
        "Bar Band",
        "Barbershop Quartet",
        "Baroque Pop",
        "Baseline",
        "Basque",
        "Bass Music",
        "Bava",
        "Bavarian",
        "Bay Area Rap",
        "Beach",
        "Beat Poetry",
        "Bedroom Pop",
        "Beguine",
        "Beguine Moderne",
        "Beguine Vide",
        "Belair",
        "Belarusian",
        "Belgian",
        "Belly Dancing",
        "Benga",
        "Bengali",
        "Berber",
        "Bhangra",
        "Big Band",
        "Big Band/Swing",
        "Big Beat",
        "Bikutsi",
        "Bird Calls",
        "Black Gospel",
        "Black Metal",
        "Blackgaze",
        "Blaxploitation",
        "Blue Humor",
        "Blue-Eyed Soul",
        "Bluebeat",
        "Bluegrass",
        "Bluegrass-Gospel",
        "Blues",
        "Blues Gospel",
        "Blues Revival",
        "Blues-Rock",
        "Bolero",
        "Bolivian",
        "Bollywood",
        "Bomba",
        "Bongo Flava",
        "Boogaloo",
        "Boogie Rock",
        "Boogie-Woogie",
        "Bop",
        "Bop Vocals",
        "Bornean",
        "Bosnian",
        "Bossa Nova",
        "Brazilian Folk",
        "Brazilian Jazz",
        "Brazilian Pop",
        "Brazilian Traditions",
        "Breakcore",
        "Breton",
        "Brill Building Pop",
        "British",
        "British Blues",
        "British Dance Bands",
        "British Folk",
        "British Folk-Rock",
        "British Invasion",
        "British Metal",
        "British Psychedelia",
        "British Punk",
        "British Rap",
        "British Trad Rock",
        "Britpop",
        "Bro-Country",
        "Broken Beat",
        "Brown-Eyed Soul",
        "Bubblegum",
        "Buddhist",
        "Bulgarian",
        "Bulgarian Folk",
        "Burundi",
        "C-86",
        "C-Pop",
        "Cabaret",
        "Cadence",
        "Cajun",
        "Calypso",
        "Cambodian",
        "Cameroonian",
        "Canadian",
        "Canterbury Scene",
        "Cantopop",
        "Cape Verdean",
        "Caribbean Traditions",
        "Carnatic",
        "Carnival",
        "Carols",
        "Cartoon Music",
        "Cast Recordings",
        "CCM",
        "Celebrity",
        "Celtic",
        "Celtic Folk",
        "Celtic Fusion",
        "Celtic Gospel",
        "Celtic New Age",
        "Celtic Pop",
        "Celtic Rock",
        "Celtic/British Isles",
        "Central African",
        "Central American Traditions",
        "Central European Traditions",
        "Central/West Asian Traditions",
        "Ceremonial",
        "Cha-Cha",
        "Chamber Jazz",
        "Chamber Music",
        "Chamber Pop",
        "Changui",
        "Chants",
        "Chanukah",
        "Charanga",
        "Chassidic",
        "Chicago Blues",
        "Chicago House",
        "Chicago Jazz",
        "Chicago Soul",
        "Children's",
        "Children's Folk",
        "Children's Pop",
        "Children's Rock",
        "Children's Songwriters",
        "Chilean",
        "Chillwave",
        "Chimurenga",
        "Chinese Classical",
        "Chinese Rap",
        "Chinese Rock",
        "Chinese Traditions",
        "Chiptunes",
        "Choral",
        "Choro",
        "Chouval Bwa",
        "Christian Comedy",
        "Christian Metal",
        "Christian Punk",
        "Christian Rap",
        "Christian Rock",
        "Christmas",
        "City Pop",
        "Classic Blues Vocals",
        "Classic Female Blues",
        "Classical",
        "Classical Crossover",
        "Classical Pop",
        "Close Harmony",
        "Cloud Rap",
        "Club/Dance",
        "Clubjazz",
        "Cocktail",
        "Cold Wave",
        "College Rock",
        "Colombian",
        "Comedy",
        "Comedy Rap",
        "Comedy Rock",
        "Comedy/Spoken",
        "Compas",
        "Composer Songbook",
        "Computer Music",
        "Conceptual Art",
        "Concerto",
        "Congolese",
        "Conjunto",
        "Contemporary Bluegrass",
        "Contemporary Blues",
        "Contemporary Celtic",
        "Contemporary Christian",
        "Contemporary Country",
        "Contemporary Flamenco",
        "Contemporary Folk",
        "Contemporary Gospel",
        "Contemporary Instrumental",
        "Contemporary Jazz",
        "Contemporary Jazz Vocals",
        "Contemporary Native American",
        "Contemporary Pop/Rock",
        "Contemporary R&B",
        "Contemporary Rap",
        "Contemporary Reggae",
        "Contemporary Singer/Songwriter",
        "Continental Jazz",
        "Cool",
        "Corrido",
        "Country",
        "Country & Irish",
        "Country Blues",
        "Country Boogie",
        "Country Comedy",
        "Country Gospel",
        "Country Rap",
        "Country Soul",
        "Country-Folk",
        "Country-Pop",
        "Country-Rock",
        "Coupé-Décalé",
        "Cowboy",
        "Cowpunk",
        "Creative Orchestra",
        "Creole",
        "Cretan",
        "Croatian",
        "Crossover Jazz",
        "Cuatro",
        "Cuban Jazz",
        "Cuban Pop",
        "Cuban Traditions",
        "Cumbia",
        "Czech",
        "Dagestani",
        "Dance",
        "Dance Bands",
        "Dance-Pop",
        "Dance-Rock",
        "Dancehall",
        "Danish",
        "Danzon",
        "Dark Ambient",
        "Darkwave",
        "Death Metal",
        "Deathcore",
        "Deep Funk",
        "Deep Funk Revival",
        "Deep House",
        "Deep Soul",
        "Delta Blues",
        "Desert Blues",
        "Detroit Blues",
        "Detroit Rock",
        "Detroit Techno",
        "Dhrupad",
        "Dimotiko",
        "Dirty Blues",
        "Dirty Rap",
        "Dirty South",
        "Disco",
        "Dixieland",
        "DJ/Toasting",
        "Djabdong",
        "Documentary",
        "Dominican Traditions",
        "Doo Wop",
        "Doom Metal",
        "Downbeat",
        "Downtempo",
        "Drama",
        "Dream Pop",
        "Drill",
        "Drill'n'bass",
        "Drinking Songs",
        "Drone",
        "Drone Metal",
        "Dub",
        "Dub Poetry",
        "Dubstep",
        "Duranguense",
        "Dutch",
        "Dutch Pop",
        "Early Acoustic Blues",
        "Early American Blues",
        "Early British Pop/Rock",
        "Early Country",
        "Early Creative",
        "Early Jazz",
        "Early Jazz Vocals",
        "Early Pop/Rock",
        "Early R&B",
        "East African",
        "East Coast Blues",
        "East Coast Rap",
        "Easter",
        "Eastern European Pop",
        "Easy Listening",
        "Easy Pop",
        "Ecuadorian",
        "EDM",
        "Educational",
        "Egyptian",
        "Electric Blues",
        "Electric Chicago Blues",
        "Electric Country Blues",
        "Electric Delta Blues",
        "Electric Harmonica Blues",
        "Electric Jazz",
        "Electric Memphis Blues",
        "Electric Texas Blues",
        "Electro",
        "Electro-Acoustic",
        "Electro-Cumbia",
        "Electro-Industrial",
        "Electro-Jazz",
        "Electro-Techno",
        "Electronic",
        "Electronic/Computer Music",
        "Electronica",
        "Electronicore",
        "Emo",
        "Emo-Pop",
        "Enka",
        "Environmental",
        "Erotica",
        "Estonian",
        "Ethiopian Pop",
        "Ethnic Comedy",
        "Ethnic Fusion",
        "Euro-Dance",
        "Euro-Disco",
        "Euro-Pop",
        "Euro-Rock",
        "European Folk",
        "European Psychedelia",
        "European Rap",
        "Europop",
        "Exercise",
        "Exotica",
        "Exotica/Lounge",
        "Experimental",
        "Experimental Ambient",
        "Experimental Big Band",
        "Experimental Club",
        "Experimental Dub",
        "Experimental Electro",
        "Experimental Electronic",
        "Experimental Jungle",
        "Experimental Rock",
        "Experimental Techno",
        "Fado",
        "Fairy Tales",
        "Fantasy",
        "Field Recordings",
        "Fight Songs",
        "Film Music",
        "Film Score",
        "Finger-Picked Guitar",
        "Finnish Folk",
        "Flamenco",
        "Flute/New Age",
        "Folk",
        "Folk Jazz",
        "Folk Revival",
        "Folk-Blues",
        "Folk-Metal",
        "Folk-Pop",
        "Folk-Rock",
        "Folk/Country Rock",
        "Folksongs",
        "Foreign Language Rock",
        "Forro",
        "Frat Rock",
        "Freakbeat",
        "Free Folk",
        "Free Funk",
        "Free Improvisation",
        "Free Jazz",
        "Freestyle",
        "French",
        "French Antilles",
        "French Chanson",
        "French Folk",
        "French Guianese",
        "French House",
        "French Pop",
        "French Rap",
        "French Rock",
        "Frevo",
        "Fuji",
        "Funk",
        "Funk Metal",
        "Funky Breaks",
        "Fusion",
        "G-Funk",
        "Gabba",
        "Gabonese",
        "Gambian",
        "Gamelan",
        "Gangsta Rap",
        "Garage",
        "Garage Punk",
        "Garage Rock",
        "Garage Rock Revival",
        "Gay Comedy",
        "Georgian",
        "Georgian Choir",
        "German",
        "German Rap",
        "Ghanaian",
        "Giddha",
        "Girl Groups",
        "Glam Rock",
        "Glitch",
        "Glitter",
        "Global Jazz",
        "Go-Go",
        "Goa Trance",
        "Golden Age",
        "Gospel",
        "Gospel Choir",
        "Goth Metal",
        "Goth Rock",
        "Gqom",
        "Greek",
        "Greek Folk",
        "Greek-Pop",
        "Grime",
        "Grindcore",
        "Grunge",
        "Grunge Revival",
        "Grupero",
        "Guadeloupe",
        "Guaguancó",
        "Guatemalan",
        "Guinea-Bissau",
        "Guinean",
        "Guitar Jazz",
        "Guitar Virtuoso",
        "Guitar/Easy Listening",
        "Guitar/New Age",
        "Gwo Ka",
        "Gypsy",
        "Hair Metal",
        "Haitian",
        "Halloween",
        "Happy Hardcore",
        "Hard Bop",
        "Hard Rock",
        "Hardcore Punk",
        "Hardcore Rap",
        "Hardcore Techno",
        "Harmonica Blues",
        "Harmony Vocal Group",
        "Harp/New Age",
        "Hawaiian",
        "Hawaiian Pop",
        "Healing",
        "Heartland Rock",
        "Heavy Metal",
        "Hebrew",
        "Hi-NRG",
        "Highlife",
        "Hip-Hop/Urban",
        "Holiday",
        "Holidays",
        "Honduran",
        "Honky Tonk",
        "Horror Rap",
        "Hot Jazz",
        "Hot Rod",
        "Hot Rod Revival",
        "House",
        "Hungarian Folk",
        "Hymns",
        "Icelandic",
        "IDM",
        "Illbient",
        "Improvisation",
        "Improvised Music",
        "Incan",
        "Indian",
        "Indian Classical",
        "Indian Pop",
        "Indian Subcontinent Traditions",
        "Indie Electronic",
        "Indie Folk",
        "Indie Pop",
        "Indie Rock",
        "Indipop",
        "Indonesian Traditions",
        "Industrial",
        "Industrial Dance",
        "Industrial Drum'n'Bass",
        "Industrial Metal",
        "Inspirational",
        "Instructional",
        "Instrumental Children's Music",
        "Instrumental Collections",
        "Instrumental Country",
        "Instrumental Gospel",
        "Instrumental Hip-Hop",
        "Instrumental Pop",
        "Instrumental Rock",
        "International",
        "International Pop",
        "International Rap",
        "Interview",
        "Inuit",
        "Iran-Classical",
        "Iranian",
        "Iraqi",
        "Irish Folk",
        "Islamic",
        "Israeli",
        "Israeli Jazz",
        "Italian Folk",
        "Italian Music",
        "Italian Pop",
        "Italian Rap",
        "Italo Disco",
        "Ivorian",
        "J-Pop",
        "Jaipongan",
        "Jam Bands",
        "Jamaican",
        "Jangle Pop",
        "Japanese Orchestral",
        "Japanese Rap",
        "Japanese Rock",
        "Japanese Traditions",
        "Javanese",
        "Jazz",
        "Jazz Blues",
        "Jazz Instrument",
        "Jazz-Funk",
        "Jazz-House",
        "Jazz-Pop",
        "Jazz-Rap",
        "Jazz-Rock",
        "Jesus Rock",
        "Jewish Folk",
        "Jewish Music",
        "Jibaro",
        "Jit",
        "Jive",
        "Joik",
        "Jug Band",
        "Juju",
        "Juke Joint Blues",
        "Juke/Footwork",
        "Jump Blues",
        "Jump Blues/Piano Blues",
        "Jungle/Drum'n'Bass",
        "Junkanoo",
        "K-Pop",
        "Kabuki",
        "Kalindula",
        "Kayokyoku",
        "Kazakhstani",
        "Kecak",
        "Kenyan",
        "Keyboard",
        "Keyboard/Synthesizer/New Age",
        "Khmer Dance",
        "Klezmer",
        "Kora",
        "Korean",
        "Korean Rap",
        "Korean Rock",
        "Kraut Rock",
        "Kulintang",
        "Kurdish",
        "Kuwaiti",
        "L.A. Punk",
        "Laika",
        "Lambada",
        "Laotian",
        "Latin",
        "Latin America",
        "Latin Big Band",
        "Latin CCM",
        "Latin Comedy",
        "Latin Dance",
        "Latin Folk",
        "Latin Freestyle",
        "Latin Gospel",
        "Latin Jazz",
        "Latin Pop",
        "Latin Psychedelia",
        "Latin Rap",
        "Latin Rock",
        "Latin Soul",
        "Latvian",
        "LDS Music",
        "Lebanese",
        "Left-Field House",
        "Left-Field Pop",
        "Left-Field Rap",
        "Library Music",
        "Liedermacher",
        "Lo-Fi",
        "Louisiana Blues",
        "Lounge",
        "Lovers Rock",
        "Lullabies",
        "M-Base",
        "Macapat Poetry",
        "Macedonian",
        "Madagascan",
        "Madchester",
        "Mainstream Jazz",
        "Makossa",
        "Malawian",
        "Malaysian",
        "Malian Music",
        "Mambo",
        "Mandopop",
        "Mantras",
        "Marabi",
        "Marches",
        "Mariachi",
        "Martinique",
        "Math Rock",
        "Mauritanian",
        "Mbalax",
        "Mbaqanga",
        "Mbira",
        "Mbube",
        "Mbuti Choral",
        "Meditation/Relaxation",
        "Mediterranean Traditions",
        "Melanesian",
        "Memphis Blues",
        "Memphis Soul",
        "Mento",
        "Merengue",
        "Merenhouse",
        "Merseybeat",
        "Metalcore",
        "Mexican Traditions",
        "Mexican-Cumbia",
        "Microhouse",
        "Micronesian",
        "Microsound",
        "Microtonal",
        "Middle Eastern Pop",
        "Middle Eastern Traditions",
        "Midwest Rap",
        "Military",
        "Mini Jazz",
        "Minimal Techno",
        "Minimalism",
        "Minstrel",
        "Miscellaneous (Classical)",
        "Mixed Media",
        "Mod",
        "Mod Revival",
        "Modal Music",
        "Modern Acoustic Blues",
        "Modern Big Band",
        "Modern Blues",
        "Modern Composition",
        "Modern Creative",
        "Modern Delta Blues",
        "Modern Electric Blues",
        "Modern Electric Chicago Blues",
        "Modern Electric Texas Blues",
        "Modern Free",
        "Modern Jazz",
        "Modern Jazz Vocals",
        "Modern Son",
        "Moldavian",
        "Mongolian",
        "Mood Music",
        "Moravian",
        "Morna",
        "Morning Radio",
        "Moroccan",
        "Motown",
        "Movie Themes",
        "Mozambiquan",
        "MPB",
        "Mugam",
        "Musette",
        "Music Comedy",
        "Music Hall",
        "Musical Comedy",
        "Musical Theater",
        "Musicals",
        "Musique Actuelle",
        "Musique Concrète",
        "Myanmarian",
        "Mystical Minimalism",
        "Namibian",
        "Narcocorridos",
        "Nashville Sound/Countrypolitan",
        "Native American",
        "Native South American",
        "Nature",
        "Ndombolo",
        "Neo-Bop",
        "Neo-Classical",
        "Neo-Classical Metal",
        "Neo-Disco",
        "Neo-Electro",
        "Neo-Glam",
        "Neo-Prog",
        "Neo-Psychedelia",
        "Neo-Soul",
        "Neo-Traditional Folk",
        "Neo-Traditionalist Country",
        "Nepalese",
        "New Acoustic",
        "New Age",
        "New Age Tone Poems",
        "New Jack Swing",
        "New Mexcio",
        "New Orleans Blues",
        "New Orleans Brass Bands",
        "New Orleans Jazz",
        "New Orleans Jazz Revival",
        "New Orleans R&B",
        "New Orleans/Classic Jazz",
        "New Romantic",
        "New Traditionalist",
        "New Wave",
        "New Wave of British Heavy Metal",
        "New Wave/Post-Punk Revival",
        "New York Blues",
        "New York Punk",
        "New York Salsa",
        "New Zealand",
        "New Zealand Rock",
        "Newbeat",
        "Nicaraguan",
        "Nigerian",
        "Nisiotika",
        "No Wave",
        "Noh",
        "Noise",
        "Noise Pop",
        "Noise-Rock",
        "Nordic Traditions",
        "Norteno",
        "North African",
        "North American Traditions",
        "North/East Asian Traditions",
        "Northern Soul",
        "Norwegian",
        "Norwegian Folk",
        "Nouvelle Chanson",
        "Novelty",
        "Novelty Ragtime",
        "Nu Breaks",
        "Nü Metal",
        "Nueva Cancion",
        "Nueva Trova",
        "Nursery Rhymes",
        "Nyahbinghi",
        "Obscuro",
        "Observational Humor",
        "Occasion-Based Effects",
        "Oceanic Traditions",
        "Oi!",
        "Okinawan Pop",
        "Okinawan Traditional",
        "Old-School Rap",
        "Old-Timey",
        "Omutibo",
        "Onda Grupera",
        "Opera",
        "Orchestral",
        "Orchestral Jazz",
        "Orchestral/Easy Listening",
        "Organ/Easy Listening",
        "Original Score",
        "Outlaw Country",
        "Pachanga",
        "Pacific Islands",
        "Paisley Underground",
        "Pakistani",
        "Palestinian",
        "Palm-Wine",
        "Panamanian",
        "Panflute/Easy Listening",
        "Papua New Guinea",
        "Paraguayan",
        "Party Rap",
        "Party Soca",
        "Persian",
        "Peruvian",
        "Peruvian Folk",
        "Philippine",
        "Philly Soul",
        "Piano Blues",
        "Piano Jazz",
        "Piano/Easy Listening",
        "Piano/New Age",
        "Pibroch",
        "Piedmont Blues",
        "Pipe Bands",
        "Plena",
        "Plunderphonics",
        "Poetry",
        "Polish",
        "Political Comedy",
        "Political Folk",
        "Political Rap",
        "Political Reggae",
        "Polka",
        "Polynesian",
        "Pop",
        "Pop Idol",
        "Pop Punk",
        "Pop-Metal",
        "Pop-Rap",
        "Pop-Soul",
        "Pop/Rock",
        "Portuguese",
        "Post-Bop",
        "Post-Disco",
        "Post-Grunge",
        "Post-Hardcore",
        "Post-Metal",
        "Post-Minimalism",
        "Post-Punk",
        "Post-Rock",
        "Power Metal",
        "Power Pop",
        "Praise & Worship",
        "Prank Calls",
        "Pre-War Blues",
        "Pre-War Country Blues",
        "Pre-War Gospel Blues",
        "Process-Generated",
        "Prog-Rock",
        "Progressive Alternative",
        "Progressive Big Band",
        "Progressive Bluegrass",
        "Progressive Country",
        "Progressive Electronic",
        "Progressive Folk",
        "Progressive House",
        "Progressive Jazz",
        "Progressive Metal",
        "Progressive Trance",
        "Protest Songs",
        "Proto-Punk",
        "Psychedelic",
        "Psychedelic Pop",
        "Psychedelic Soul",
        "Psychedelic/Garage",
        "Psychobilly",
        "Psytrance",
        "Pub Rock",
        "Puerto Rican Traditions",
        "Punk",
        "Punk Blues",
        "Punk Metal",
        "Punk Revival",
        "Punk/New Wave",
        "Punta",
        "Pygmy",
        "Qawwali",
        "Quadrille",
        "Quebecois",
        "Quechua",
        "Queercore",
        "Quiet Storm",
        "R&B",
        "R&B Instrumental",
        "Radio Plays",
        "Radio Shows",
        "Radio Works",
        "Raga",
        "Ragga",
        "Ragtime",
        "Rai",
        "Rakugo",
        "Ranchera",
        "Rap",
        "Rap-Metal",
        "Rap-Rock",
        "Rapso",
        "Rave",
        "Red Dirt",
        "Reggae",
        "Reggae Gospel",
        "Reggae-Pop",
        "Reggaeton",
        "Reggaeton/Latin Rap",
        "Regional Blues",
        "Relaxation",
        "Rembetika",
        "Retro Swing",
        "Retro-Rock",
        "Retro-Soul",
        "Riot Grrrl",
        "Ritual Music",
        "Rock & Roll",
        "Rock & Roll/Roots",
        "Rock en Español",
        "Rockabilly",
        "Rockabilly Revival",
        "Rocksteady",
        "Rodeo",
        "Romanian",
        "Roots Reggae",
        "Roots Rock",
        "Rumba",
        "Russian Folk",
        "Russian Traditions",
        "Sacred Traditions",
        "Sadcore",
        "Salsa",
        "Salvadoran",
        "Samba",
        "Sami",
        "Samoan",
        "Sardinian",
        "Satire",
        "Saudi Arabian",
        "Saxophone Jazz",
        "Scandinavian",
        "Scandinavian Metal",
        "Scandinavian Pop",
        "Schlager",
        "Scottish Country Dance",
        "Scottish Folk",
        "Screamo",
        "Scriptures",
        "Sea Shanties",
        "Séga",
        "Self-Help & Development",
        "Senegalese Music",
        "Serbian",
        "Sha'abi",
        "Sharki",
        "Shibuya-Kei",
        "Shinto",
        "Shock Jock",
        "Shoegaze",
        "Show Tunes",
        "Show/Musical",
        "Siamese",
        "Siberian",
        "Sierra Leonian",
        "Sing-Alongs",
        "Singer/Songwriter",
        "Ska",
        "Ska Revival",
        "Ska-Punk",
        "Skatepunk",
        "Sketch Comedy",
        "Skiffle",
        "Slack-Key Guitar",
        "Slide Guitar Blues",
        "Slovakian",
        "Slovenian",
        "Slowcore",
        "Sludge Metal",
        "Smooth Jazz",
        "Smooth Reggae",
        "Smooth Soul",
        "Soca",
        "Social Media Pop",
        "Society Dance Band",
        "Soft Rock",
        "Solo Instrumental",
        "Solomon Islands",
        "Somalian",
        "Son",
        "Sonero",
        "Song Parody",
        "Songster",
        "Sonidero",
        "Sophisti-Pop",
        "Soukous",
        "Soul",
        "Soul Jazz",
        "Soul Jazz/Groove",
        "Soul-Blues",
        "Sound Art",
        "Sound Collage",
        "Sound Effects",
        "Sound Sculpture",
        "Sound System",
        "Soundtracks",
        "South African Folk",
        "South African Pop",
        "South African Pop/Rock",
        "South African Rock",
        "South American Traditions",
        "South/Eastern European Traditions",
        "Southeast Asian Traditions",
        "Southern African",
        "Southern Gospel",
        "Southern Rap",
        "Southern Rock",
        "Southern Soul",
        "Space",
        "Space Age Pop",
        "Space Rock",
        "Spanish Folk",
        "Speeches",
        "Speed/Thrash Metal",
        "Spiritual",
        "Spiritual Jazz",
        "Spirituals",
        "Spoken Comedy",
        "Spoken Word",
        "Sports Anthems",
        "Spouge",
        "Spy Music",
        "Square Dance",
        "St. Louis Blues",
        "Standards",
        "Standup Comedy",
        "Steel Band",
        "Stoner Metal",
        "Stories",
        "Straight-Ahead Jazz",
        "Straight-Edge",
        "Stride",
        "String Bands",
        "Structured Improvisation",
        "Sudanese",
        "Sufi",
        "Sumatran",
        "Sunshine Pop",
        "Surf",
        "Surf Revival",
        "Swahili",
        "Swamp Blues",
        "Swamp Pop",
        "Swedish Folk",
        "Swedish Pop/Rock",
        "Sweet Bands",
        "Swing",
        "Swiss Folk",
        "Symphonic Black Metal",
        "Symphonic Metal",
        "Symphony",
        "Synth Pop",
        "Synthwave",
        "Syrian",
        "Taarab",
        "Tahitian",
        "Tajik",
        "Tango",
        "Tanzanian",
        "Tape Music",
        "Tech-House",
        "Technical Death Metal",
        "Techno",
        "Techno Bass",
        "Techno-Dub",
        "Techno-Tribal",
        "Teen Idols",
        "Teen Pop",
        "Tejano",
        "Television Music",
        "Tex-Mex",
        "Texas Blues",
        "Texas Rap",
        "Thai",
        "Thai Pop",
        "Thanksgiving",
        "Third Stream",
        "Third Wave Ska Revival",
        "Throat Singing",
        "Tibetan",
        "Timba",
        "Tin Pan Alley Pop",
        "Tongan",
        "Torch Songs",
        "Township Jazz",
        "Township Jive",
        "Trad Jazz",
        "Traditional Bluegrass",
        "Traditional Blues",
        "Traditional Celtic",
        "Traditional Chinese",
        "Traditional Country",
        "Traditional European Folk",
        "Traditional Folk",
        "Traditional Gospel",
        "Traditional Irish Folk",
        "Traditional Japanese",
        "Traditional Korean",
        "Traditional Middle Eastern Folk",
        "Traditional Native American",
        "Traditional Pop",
        "Traditional Scottish Folk",
        "Trance",
        "Transylvanian",
        "Trap (EDM)",
        "Trap (Latin)",
        "Trap (Rap)",
        "Tribal House",
        "Tribute Albums",
        "Trinidadian",
        "Trip-Hop",
        "Trombone Jazz",
        "Tropical",
        "Tropicalia",
        "Trot",
        "Trova",
        "Truck Driving Country",
        "Trumpet Jazz",
        "Trumpet/Easy Listening",
        "Turkish",
        "Turkish Psychedelia",
        "Turntablism",
        "Tuvan",
        "TV Music",
        "TV Soundtracks",
        "Twee Pop",
        "Tyrolean",
        "Ugandan",
        "UK Drill",
        "UK Garage",
        "Ukrainian",
        "Underground Rap",
        "Uptown Soul",
        "Urban",
        "Urban Blues",
        "Urban Cowboy",
        "Urban Folk",
        "Urbano",
        "Uruguayan",
        "Uzbekistani",
        "Vallenato",
        "Vaporware",
        "Vaudeville",
        "Vaudeville Blues",
        "Vaudou",
        "Venezuelan",
        "Vibraphone/Marimba Jazz",
        "Video Game Music",
        "Vietnamese",
        "Visual Kei",
        "Vocal",
        "Vocal Jazz",
        "Vocal Music",
        "Vocal Pop",
        "Vocalese",
        "Volksmusik",
        "Waltz",
        "Wedding Collections",
        "Welsh",
        "West African",
        "West Coast Blues",
        "West Coast Jazz",
        "West Coast Rap",
        "Western European Traditions",
        "Western Swing",
        "Western Swing Revival",
        "Witch House",
        "Work Song",
        "Work Songs",
        "Worldbeat",
        "Yé-yé",
        "Yemenite",
        "Yodel",
        "Yodeling",
        "Yoruban",
        "Yugoslavian",
        "Zairean",
        "Zambian",
        "Zimbabwean",
        "Zouk",
        "Zulu",
        "Zydeco")



# def show_table_differences():

#   ''' pick up the columns present in the table '''
#   columns = get_columns('alib')

#   if table_exists('alib_rollback'):
#       for column in columns:

#           field_to_compare = column[0]
#           # print(f"Changes in {column[0]}:")
#           # query = f"select alib.*, alib_rollback.* from alib inner join alib_rollback ON alib.__path = alib_rollback.__path where 'alib.{column[0]}' != 'alib_rollback.{column[0]}'"
#           dbcursor.execute(f"select alib.__path, 'alib.{field_to_compare}', 'alib_rollback.{field_to_compare}' from alib inner join alib_rollback ON alib.__path = alib_rollback.__path where ('alib.{field_to_compare}' != 'alib_rollback.{field_to_compare}');")
#           differences = dbcursor.fetchall()
#           diffcount = len(differences)
#           print(diffcount)
#           input()
#           for difference in differences:
#               print(difference[0], difference[1], difference[2])



def texttags_in_alib(taglist):
    ''' compare existing tags in alib table against list of text tags and eliminate those that are not present in alib '''
    dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib');")
    dbtags = dbcursor.fetchall()
    ''' generate a list of the first element of each tuple in the list of tuples that is dbtags '''
    dblist = list(zip(*dbtags))[0]
    ''' build list of matching tagnames in dblist '''
    return([tag for tag in taglist if tag in dblist])

    

def kill_badtags():
    ''' iterate over unwanted tags and set any non NULL values to NULL '''

    ''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
    dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
    badtags = dbcursor.fetchall()
    if len(badtags) > 0:
        badtags.sort()

        opening_tally = tally_mods()
        print(f"\nRemoving spurious tags:")

        for tagname in badtags:

            if not tagname[0].startswith('__'):
                ''' make an exception for __albumgain as it's ever present in mp3 and always null, so bypass it as it'd waste a cycle.  all other tags starting with '__' are created by tagfromdb3.py and are in effect _static_ data '''
                ''' append quotes to tag names in case any have a space in the field name '''
                tag = '"' + tagname[0] + '"'
                dbcursor.execute(f'''CREATE INDEX IF NOT EXISTS ix_spurious ON alib({tag}) WHERE {tag} IS NOT NULL''')
                dbcursor.execute(f'''SELECT COUNT({tag}) FROM alib''')
                tally = dbcursor.fetchone()[0]
                print(f"- {tag}, {tally}")
                dbcursor.execute(f"UPDATE alib SET {tag} = NULL WHERE {tag} IS NOT NULL")
                dbcursor.execute(f"DROP INDEX IF EXISTS ix_spurious")
                conn.commit() # it should be possible to move this out of the for loop, but then just check that trigger is working correctly

    closing_tally = tally_mods()
    print(f"|\n{closing_tally - opening_tally} tags were removed")
    return(closing_tally - opening_tally)



def nullify_empty_tags():

    ''' set all fields to NULL where they are otherwise empty but not NULL '''
    opening_tally = tally_mods()
    columns = get_columns('alib')
    print("\nSetting all fields to NULL where they are otherwise empty but not NULL")
    for column in columns:
        # skip over all tags starting with '__' 
        if not column[0].startswith('__'):
            field_to_check = '[' + column[0] + ']'
            print(f"Checking: {field_to_check}")
            dbcursor.execute(f"CREATE INDEX IF NOT EXISTS ix_nullify ON alib ({field_to_check}) WHERE TRIM({field_to_check}) = '';")
            dbcursor.execute(f"UPDATE alib SET {field_to_check} = NULL WHERE TRIM({field_to_check}) = '';")
            dbcursor.execute(f"DROP INDEX IF EXISTS ix_nullify;")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def trim_and_remove_crlf():
    ''' Identify and remove spurious CRs, LFs and SPACES in all known text fields '''
    ''' list all known text tags you might want to process against '''
    all_text_tags = ["_releasecomment", "album", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", "discsubtitle", 
    "engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "movement", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", 
    "musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "part", "performer", "personnel", "producer", "recordinglocation", "releasetype", 
    "remixer", "style", "subtitle", "theme", "title", "upc", "version", "work", "writer"]

    ''' narrow it down to the list that's actually present in alib table - based on what's been imported '''
    text_tags = texttags_in_alib(all_text_tags)
    print(f"\nTrimming and removing spurious CRs, LFs in:")
    opening_tally = tally_mods()

    for text_tag in text_tags:
        dbcursor.execute(f"CREATE INDEX IF NOT EXISTS ix_crlf ON alib (replace(replace({text_tag}, char(10), ''), char(13), '') ) WHERE {text_tag} IS NOT NULL AND {text_tag} != replace(replace({text_tag}, char(10), ''), char(13), '');")
        dbcursor.execute(f"CREATE INDEX IF NOT EXISTS ix_crlf1 ON alib (trim({text_tag})) WHERE {text_tag} IS NOT NULL;")

        print(f"- {text_tag}")

        ''' trim crlf '''
        # dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(10), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(10), ''));")
        # dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(13), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(13), ''));")
        # dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, char(10), '') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, char(10), '');")
        # dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, char(13), '') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, char(13), '');")
        dbcursor.execute(f"UPDATE alib SET {text_tag} = replace(replace({text_tag}, char(10), ''), char(13), '') WHERE {text_tag} IS NOT NULL AND {text_tag} != replace(replace({text_tag}, char(10), ''), char(13), '');")


        ''' trim spaces between delimiters '''
        dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, ' \\','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, ' \\','\\');")
        dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, '\\ ','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, '\\ ','\\');")

        ''' finally trim the end result '''
        dbcursor.execute(f"UPDATE alib SET {text_tag} = trim({text_tag}) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim({text_tag});")

        dbcursor.execute(f"DROP INDEX IF EXISTS crlf")
        dbcursor.execute(f"DROP INDEX IF EXISTS crlf1")

    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def set_apostrophe():

    all_text_tags = ["_releasecomment", "album", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", "discsubtitle", 
    "engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "movement", "part", "performer", "personnel", "producer", "recordinglocation", "releasetype", 
    "remixer", "style", "subtitle", "theme", "title", "upc", "version", "work", "writer"]

    wrong_apostrophe1 =r'’'
    wrong_apostrophe2 =  r' ́'
    right_apostrophe = "\'"

    ''' narrow it down to the list that's actually present in alib table - based on what's been imported '''
    text_tags = texttags_in_alib(all_text_tags)
    opening_tally = tally_mods()

    for text_tag in text_tags:
        dbcursor.execute(f"CREATE INDEX IF NOT EXISTS ix_apostrophe ON alib ({text_tag}) WHERE {text_tag} IS NOT NULL;")
        print(f"\nStandardising apostrophes: replacing instances of '{wrong_apostrophe1}' and '{wrong_apostrophe2}' with '{right_apostrophe}' for tag: {text_tag}")

        ''' replace wrong apostrophes '''
        dbcursor.execute(
            f"UPDATE alib SET {text_tag} = replace({text_tag}, (?), (?) ) WHERE {text_tag} IS NOT NULL AND {text_tag} != replace({text_tag}, (?), (?) );", (wrong_apostrophe1, right_apostrophe, wrong_apostrophe1, right_apostrophe))

        dbcursor.execute(
            f"UPDATE alib SET {text_tag} = replace({text_tag}, (?), (?) ) WHERE {text_tag} IS NOT NULL AND {text_tag} != replace({text_tag}, (?), (?) );", (wrong_apostrophe2, right_apostrophe, wrong_apostrophe2, right_apostrophe))


        dbcursor.execute(f"DROP INDEX IF EXISTS ix_apostrophe;")

    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def square_brackets_to_subtitle():
    '''  select all records with '[' in title, split-off text everything folowing '[' and write it out to subtitle '''
    opening_tally = tally_mods()
    print(f"\nUpdating titles to remove any text enclosed in square brackets from TITLE and appending same to SUBTITLE tag")
    # dbcursor.execute("UPDATE alib SET title = IIF(TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) = '', title, TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1))), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%';")
    dbcursor.execute("UPDATE alib SET title = TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%' AND TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) != '';")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def title_feat_to_artist():
    ''' Move all instances of Feat and With in track TITLE to ARTIST tag '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')


    print("\nStripping feat. from TITLE tag and incorporating into delimited ARTIST string...\n")

    ''' Select all records that have another undelimited performer name in the TITLE field '''
    dbcursor.execute('''SELECT title,
                               artist,
                               rowid
                          FROM alib
                         WHERE title LIKE '%(Feat. %)%' OR 
                               title LIKE '%[Feat. %]%' OR 
                               title LIKE '%(Feat: %)%' OR 
                               title LIKE '%[Feat: %]%' OR 
                               title LIKE '%(Feat %)%' OR 
                               title LIKE '%[Feat %]%' OR 
                               title LIKE '%(Featuring. %)%' OR 
                               title LIKE '%[Featuring. %]%' OR 
                               title LIKE '%(Featuring: %)%' OR 
                               title LIKE '%[Featuring: %]%' OR 
                               title LIKE '%(Featuring %)%' OR 
                               title LIKE '%[Featuring %]%' OR 
                               title LIKE '% Featuring. %' OR 
                               title LIKE '% Featuring: %' OR 
                               title LIKE '% Featuring %' OR 
                               title LIKE '% Feat. %' OR 
                               title LIKE '% Feat: %' OR 
                               title LIKE '% Feat %' OR 
                               title LIKE '% Ft. %' OR 
                               title LIKE '% ft. %' OR 
                               title LIKE '% With: %';''')

    records_to_process = dbcursor.fetchall()
    
    feats = ['Feat ', 'Feat:', 'Feat.', 'Feat-', 'Feat -', 'Featuring ', 'Featuring:', 'Featuring.', 'Featuring-',  'Featuring -', 'Ft.', 'ft. ', 'With:' ]

    ''' now process each in sequence '''
    for record in records_to_process:

        ''' loop through records  and process each string '''
        row_title = record[0] # get title field contents
        row_artist = record[1] # get artist field contents
        table_record = record[2] # get rowid

        ''' test for bracket enclosed contents, strip brackets'''
        if '[' in row_title or ']' in row_title or '(' in row_title or ')' or '{' in row_title or '}' in row_title:


            ''' strip bracket components from base'''
            brackets = ['[', ']', '(', ')', '{', '}']
            for bracket in brackets:
                row_title = row_title.replace(bracket, '')
                

        ''' now break down string into base and substring, testing for each instance of feat '''
        for feat in feats:
    
            print(f"'{feat}'")            
            
            try:
                '''check if  this instance of feat is in the string '''
                split_point = row_title.lower().index(feat.lower())
                feat_len = len(feat)

                ''' if it is, split the string into base and substring, removing this instance of feat and trimming both to get rid of extranous whitespace '''
                base = row_title[0:split_point -1 ].strip()
                sub = row_title[split_point + feat_len:].strip()

                ''' finally derive the new artist entry by concatenate the string in a way that puddletag knows how to handle the delimiter '''
                if row_artist is None:
                    new_artist = sub
                else:
                    new_artist = row_artist + '\\\\' + sub

                print(f"{format(table_record,'07d')}, String: '{row_title}' Base: '{base}' Sub: '{sub}' New artist: '{new_artist}'")
                dbcursor.execute('''UPDATE alib set title = (?), artist = (?) WHERE rowid = (?);''', (base, new_artist, table_record))

            except ValueError:

                ''' if no match exit the loop and continue to next match ... this implementation is just to stop code crashing when feat is not present in row_title '''
                exit

        print('\n')        
                    

    print(f"|\n{tally_mods() - opening_tally} changes were processed")  



def feat_artist_to_artist():
    ''' Move all instances of Feat and With to ARTIST tag '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')

    print("\nStripping feat. from ARTIST tag and incorporating into delimited ARTIST string...\n")

    ''' Select all records that have another undelimited performer name in the artist field '''
    dbcursor.execute('''SELECT artist,
                               rowid 
                          FROM alib
                         WHERE artist LIKE '%(Feat. %)%' OR 
                               artist LIKE '%[Feat. %]%' OR 
                               artist LIKE '%(Feat: %)%' OR 
                               artist LIKE '%[Feat: %]%' OR 
                               artist LIKE '%(Feat %)%' OR 
                               artist LIKE '%[Feat %]%' OR
                               artist LIKE '%(Featuring. %)%' OR 
                               artist LIKE '%[Featuring. %]%' OR 
                               artist LIKE '%(Featuring: %)%' OR 
                               artist LIKE '%[Featuring: %]%' OR 
                               artist LIKE '%(Featuring %)%' OR 
                               artist LIKE '%[Featuring %]%' OR
                               artist LIKE '% Featuring. %' OR 
                               artist LIKE '% Featuring: %' OR 
                               artist LIKE '% Featuring %' OR 
                               artist LIKE '% Feat. %' OR 
                               artist LIKE '% Feat: %' OR 
                               artist LIKE '% Feat %' OR 
                               artist LIKE '% With: %';''')


    records_to_process = dbcursor.fetchall()
    
    feats = ['Feat ', 'Feat:', 'Feat.', 'Feat-', 'Feat -', 'Featuring ', 'Featuring:', 'Featuring.', 'Featuring-',  'Featuring -', 'With:']

    ''' now process each in sequece '''
    for record in records_to_process:

        ''' loop through records  and process each string '''
        row_artist = record[0] # get field contents
        table_record = record[1] # get rowid

        ''' test for bracket enclosed contents, strip brackets'''
        if '[' in row_artist or ']' in row_artist or '(' in row_artist or ')' or '{' in row_artist or '}' in row_artist:


            ''' strip bracket components from base'''
            brackets = ['[', ']', '(', ')', '{', '}']
            for bracket in brackets:
                row_artist = row_artist.replace(bracket, '')
                

        ''' now break down string into base and substring, testing for each instance of feat '''
        for feat in feats:
    
            print(f"'{feat}'")            
            
            try:
                '''check if  this instance of feat is in the string '''
                split_point = row_artist.lower().index(feat.lower())
                feat_len = len(feat)

                ''' if it is, split the string into base and substring, removing this instance of feat and trimming both to get rid of extranous whitespace '''
                base = row_artist[0:split_point -1 ].strip()
                sub = row_artist[split_point + feat_len:].strip()
                ''' finally concatenate the string in a way that puddletag knows how to handle the delimiter '''
                newbase = base + '\\\\' + sub

                print(f"{format(table_record,'07d')}, String: '{row_artist}' Base: '{base}' Sub: '{sub}' Newbase: '{newbase}'")
                dbcursor.execute('''UPDATE alib set artist = (?) WHERE rowid = (?);''', (newbase, table_record))

            except ValueError:

                ''' if no match exit the loop and continue to next match ... this iimplementation is just to stop code crashing when feat is not present in row_artist '''
                exit

        print('\n')        
                    

    dbcursor.execute('''DROP INDEX IF EXISTS artists''')
    print(f"|\n{tally_mods() - opening_tally} changes were processed")  



def merge_recording_locations():
    # ''' append "recording location" to recordinglocation if recordinglocation is empty sql2 is likely redundant because of killbadtags'''
    column_name = "recording location"
    tag_in_table(column_name, 'alib')
    print(f"\nIncorporating recording location into recordinglocation")
    opening_tally = tally_mods()

    if tag_in_table(column_name, 'alib'):

        sql1 = '''
        UPDATE alib SET recordinglocation = alib."recording location", "recording location" = NULL WHERE alib.recordinglocation IS NULL AND alib."recording location" IS NOT NULL;
        '''
        sql2 = '''
        UPDATE alib SET recordinglocation = recordinglocation || "\\" || alib."recording location", "recording location" = NULL WHERE alib.recordinglocation IS NOT NULL AND alib."recording location" IS NOT NULL;
        '''
        dbcursor.execute(sql1)
        dbcursor.execute(sql2)      
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def release_to_version():
    # ''' append "release" to version if version is empty. sql2 is likely redundant because of killbadtags'''
    column_name = "release"
    tag_in_table(column_name, 'alib')
    print(f"\nIncorporating 'release' into 'version' and removing 'release' metadata")
    opening_tally = tally_mods()

    if tag_in_table(column_name, 'alib'):

        sql1 = '''
        UPDATE alib SET version = alib.release, release = NULL WHERE alib.version IS NULL and alib.release IS NOT NULL;
        '''
        sql2 = '''
        UPDATE alib SET version = version || " " || alib.release, release = NULL WHERE alib.version IS NOT NULL AND alib.release IS NOT NULL AND NOT INSTR(alib.version, alib.release);
        '''
        dbcursor.execute(sql1)
        dbcursor.execute(sql2)      
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def unsyncedlyrics_to_lyrics():
    ''' append "unsyncedlyrics" to lyrics if lyrics is empty '''
    print(f"\nCopying unsyncedlyrics to lyrics where lyrics tag is empty")
    opening_tally = tally_mods()
    if tag_in_table('unsyncedlyrics', 'alib'):
        dbcursor.execute("UPDATE alib SET lyrics = unsyncedlyrics WHERE lyrics IS NULL AND unsyncedlyrics IS NOT NULL;")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def nullify_performers_matching_artists():
    ''' remove performer tags where they match or appear in artist tag '''
    opening_tally = tally_mods()
    print(f"\nRemoving performer names where they match or appear in artist tag")
    dbcursor.execute('UPDATE alib SET performer = NULL WHERE ( (lower(performer) = lower(artist) ) OR INSTR(artist, performer) > 0);')
    dbcursor.execute('UPDATE alib SET performer = NULL WHERE ( (lower(performer) = lower(albumartist) ) OR INSTR(albumartist, performer) > 0);')    
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def cleanse_genres_and_styles():
    ''' iterate over genre and style tags and remove tag entries that are not in the vetted list defined in vetted_genre_pool(), then merge genre and style tags into genre tag '''
    ''' the problem we are trying to solve here is five-fold: '''
    ''' 1) remove unvetted genres and styles from the record '''
    ''' 2) create a merged, deduplicated genre string from the combination of genre and style tags in the record '''
    ''' 3) compare both cleansed genres and cleansed styles with a sorted version of what's been read in '''
    ''' 4) write out a replacement genre entry if, and only if '''
    '''        the newly composed genre string differs from what's already in the record '''
    ''' 5) write out a replacement style entry if, and only if '''
    '''        the newly composed style string differs from what's already in the record '''

 
    # get every unique genre, style combo where either genre or style is not null
    # we do this because we will issue bulk updates based on those genre and style combos rather than process the entire alib table sequentially
    dbcursor.execute("SELECT DISTINCT genre, style FROM alib WHERE (genre IS NOT NULL OR style IS NOT NULL) ORDER BY genre, style;")
    records = dbcursor.fetchall()
    population = len(records)



    opening_tally = tally_mods()
    if population > 0:

        print(f"\nDeduplicating and removing spurious Genre & Style assignments and merging Genre & Style for {population} albums having genre and/or style metadata:\n")

        loop_iterator = 0
        loop_mods = tally_mods()

        #iterate through every record
        for record in records:

            #increment loop counter and tally number changes as baseline
            loop_iterator += 1

            # set update_trigger to FALSE.  This trigger is used to determine whether a table update is required.  It's set to TRUE when either style or genre needs an update
            update_triggered = False

            #store the baseline values which would serve as the replacement criteria where a table update is required
            baseline_genre = record[0]
            baseline_style = record[1]

            print(f'\n┌ Processing unique genre & style combination #{loop_iterator}/{population} [{loop_iterator / population:0.1%}] present in your file tags:\n├ Genre: {baseline_genre}\n├ Style: {baseline_style}')

            # generate incoming genre and style lists from record, if empty create empty lists
            if baseline_genre is not None:
                incoming_genres = delimited_string_to_list(dedupe_and_sort(baseline_genre))
                #incoming_genres.sort()
            else:
                incoming_genres = []
                
            if baseline_style is not None:
                incoming_styles = delimited_string_to_list(dedupe_and_sort(baseline_style))
                #incoming_styles.sort()
            else:
                incoming_styles = []


            ''' Now that we have derived genre and style lists, derive list of sanctioned styles seperately from sanctioned genres
            because we want to update both genres and styles and at the same time append styles to genres for writing back to record '''


            # if incoming_styles is not empty then it means that we'll be generating a validated style list against which we're going to compare incoming_styles to ascertain whether the table needs an update
            # so establish values for vetted_styles and replacement_style
            if incoming_styles:

                vetted_styles = vetted_list_intersection(incoming_styles, vetted_genre_pool())
                replacement_style = list_to_delimited_string(vetted_styles)

            else:

                vetted_styles = []
                replacement_style = None

            if incoming_styles != vetted_styles:
                        update_triggered = True


            # if incoming_genres is not empty then it means that we'll be generating a validated genre list against which we're going to compare incoming_genres to ascertain whether the table needs an update
            vetted_genres = [] if not incoming_genres else vetted_list_intersection(incoming_genres, vetted_genre_pool())

            # merge, dedupe and sort vetted_genres and vetted_styles
            vetted_genres_and_styles = sorted(set(vetted_genres + vetted_styles))

            if not vetted_genres_and_styles:
                # print(f'├ No vetted genre and style combination generated')
                replacement_genre = None

            else:
                # print(f'├ Vetted genre and style combination generated')
                replacement_genre = list_to_delimited_string(vetted_genres_and_styles)                

            if incoming_genres != vetted_genres_and_styles:

                # print(f"├── Incoming Genres DON'T MATCH merged vetted Genres & Styles...update to genre tag required")
                # if they're not the same then an update to matching records in table is warranted
                update_triggered = True


            # ok, so now we have established whether or not an update is required.  The update statement needs to take account of None values because they need to
            # translate into IS NULL statements both in comparison and assignment statments.
            if update_triggered:
                print('└── Update triggered!')

                # if there's no baseline_genre, but there is a baseline_style
                if (not baseline_genre and baseline_style):

                    dbcursor.execute(f'''UPDATE alib
                                           SET genre = (?),
                                               style = (?) 
                                         WHERE (genre IS NULL AND 
                                                style = (?) );''', (replacement_genre, replacement_style, baseline_style))

                # if there's a baseline_genre, but there's no baseline_style
                elif (baseline_genre and not baseline_style):

                    dbcursor.execute(f'''UPDATE alib
                                           SET genre = (?),
                                               style = (?) 
                                         WHERE (genre = (?) AND 
                                                style IS NULL );''', (replacement_genre, replacement_style, baseline_genre))

                # else there must be a baseline_genre and a baseline_style
                else:

                    dbcursor.execute(f'''UPDATE alib
                       SET genre = (?),
                           style = (?) 
                     WHERE (genre = (?) AND 
                            style = (?) );''', (replacement_genre, replacement_style, baseline_genre, baseline_style))


                #print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                # loop_mods = tally_mods()

                
    conn.commit() # commit changes to table
    closing_tally = tally_mods()
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def add_genres_and_styles():

    '''the purpose of this function is to enrich genre and style entries for albums by an albumartist where the albums in question have no genre or no style entries, but other albums by 
    said albumartist have genre and/or style entries that can be leveraged.  It also enriches genre and style metadata for albums that have only 'Pop/Rock' or 'Jazz' as genre metadata 
    and no style metadata.  To do so it builds up a composite of that albumartist's genres and styles and applies them to albums that have only 'Pop/Rock' or 'Jazz' as genre metadata 
    and no style metadata.

    This function should be run AFTER cleanse_genres_and_styles():

    pseudocode:
    gather distinct list of all albumartists in lib that have one or more albums without a genre or style entry
    for each albumartist:
    - get list of distinct genre and style combinations for their discography in the library
    - iterate over the list and build up genre and style lists by appending genre and style metadata where they are not empty
    - deduplicate and sort both the genre and style lists
    - merge genre and style lists into a deduplicated sorted genre+style list
    - for albums without genre and without style entries
       - write out the merged styles to any albums by that albumartist where albums by that albumartist don't have style entries
       - write out the merged genre and style list to genres where albums by that albumartist don't have genre entries
    what this doesn't cover is instances where genres are not complete for all tracks in an album 
        ... another function to come as a precursor or incorporated herein '''

    opening_tally = tally_mods()
    print(f"\n┌ Adding Genres and Styles to albums without both of genres and styles, based on amalgamation of albumartist's genres and styles from other works:\n")

    # as this is a rather data intensive iterative process, conditional indexes may help with performance
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists1 ON alib (
                            albumartist
                        )
                        WHERE (albumartist IS NOT NULL AND 
                               (genre IS NULL AND 
                                style IS NULL) );''')

    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists2 ON alib (
                            albumartist
                        )
                        WHERE (albumartist IS NOT NULL AND 
                               (genre IS NOT NULL OR 
                                style IS NOT NULL) );''')




    # now get a list of all albumartists that have albums with neither a genre nor a style entry as the initial input to work from
    # the working assumption here is that these same albumartists have other albums in the lib that do have genre and style metadata
    # we're going to aggregate and write that genre and style metadata to the albums by those albumartists where genre and style are missing

    dbcursor.execute('''SELECT DISTINCT albumartist
                          FROM alib
                         WHERE (albumartist IS NOT NULL AND 
                                (genre IS NULL AND 
                                 style IS NULL) ) 
                         ORDER BY albumartist;''')

    albumartists = dbcursor.fetchall()
    population = len(albumartists)

    if population > 0:

        loop_iterator = 0
        loop_mods = tally_mods()
        
        # loop through every album for each albumartist where either genre or style is not null
        # concatenate these genre and style values into variables for application to each album by that albumartist that has neither a genre nor a style value.
        for item in albumartists:

            # incrememt a loop var so you can determine % completion
            loop_iterator += 1
            # reset update_trigger
            update_triggered = False
            # pickup albumartist
            album_artist = item[0]

            ''' initialise empty lists to append from that albumartist's unique genre and style combinations ensuring we re-baseline concatenated genres and styles
            at every iteration of albumartist otherwise we're appending a previous loop's results to a new albumartist '''
            genre_list = []
            style_list = []
            concatenated_genres = []
            concatenated_styles = []
            vetted_genres = []
            vetted_styles = []
            vetted_genres_and_styles = []

            print(f'\n┌ Processing albumartist: {album_artist} [#{loop_iterator}/{population} ({loop_iterator / population:0.1%})] that has albums with no genre or style metadata')            
            
            # get a list of all that albumartist's genres and styles by polling all records that have one or both of genre and style tags
            dbcursor.execute('''SELECT DISTINCT genre,
                                                style
                                  FROM alib
                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                        (genre IS NOT NULL OR 
                                         style IS NOT NULL) ) 
                                 ORDER BY albumartist;''', (album_artist,))

            records = dbcursor.fetchall()
            matched_records = len(records)

            # right, now we have all unique genre and style records for this albumartist where there's either a genre or style entry, or both
            if matched_records > 0:

                print(f'├ Accumulating and concatenating genre and style entries from {matched_records} albums by {album_artist}')
                # iterate through every record, building up genre and style lists from every unique combination pertaining to the albumartist into concatenated_genres[] and concatenated_styles[]
                for record in records:

                    # store the baseline values related to the currently processed record
                    baseline_genre = record[0]
                    baseline_style = record[1]

                    # generate incoming genre and style lists from record and append incoming genre and style lists from record
                    if baseline_genre is not None:

                        # add the baseline_genre to the existing string
                        concatenated_genres.extend(delimited_string_to_list(baseline_genre))
                        # sort and eliminate duplicates to keep the list lean
                        #concatenated_genres = sorted(set(concatenated_genres))


                    if baseline_style is not None:

                        # add the baseline_style to the existing string
                        concatenated_styles.extend(delimited_string_to_list(baseline_style))
                        # sort and elimiate duplicates to keep the list lean
                        #concatenated_styles = sorted(set(concatenated_styles))


                # now you're done collecting concatenated_genre and concatenated_style metadata, process the end result, getting rid of unvetted items and eliminating duplicate entries
                if concatenated_styles:
                    # dedupe concatenated_styles by calling set and vet the outcomes against the vetted pool, returning the matched items from vetted_genre_pool()
                    vetted_styles = vetted_list_intersection(sorted(set(concatenated_styles)), vetted_genre_pool())
                    # convert vetted styles to a delimited string for writing to table
                    replacement_style = list_to_delimited_string(vetted_styles)
                    # set update trigger
                    update_triggered = True

                else:
                    vetted_styles = []
                    replacement_style = None
                    #print(f'├ No Style tags found and thus none added to albums by albumartist: {album_artist} without style tag')

                if concatenated_genres:

                    # dedupe concatenated_genres by calling set and vet the sorted outcomes against the vetted pool, returning the matched items from vetted_genre_pool()
                    vetted_genres = vetted_list_intersection(sorted(set(concatenated_genres)), vetted_genre_pool())

                else:
                    vetted_genres = []

                # right, now we've merged, sorted and deduplicated a vetted genres and vetted styles list it's time to merge vetted_genres and vetted_styles
                vetted_genres_and_styles = sorted(set(vetted_genres + vetted_styles))

                if vetted_genres_and_styles:
                    ''' replace all instances of that Style entry with unvetted Style entries removed, or set to NULL if no legitimate entry '''
                    ''' replace all instances of NULL genre entry with unvetted Genre entries removed, set to NULL if no legitimate entry '''
                    # convert vetted genres_and_styles to a delimited string for writing to table
                    replacement_genre = list_to_delimited_string(vetted_genres_and_styles)
                    # set update trigger
                    update_triggered = True

                else:

                    replacement_genre = None
                    #print(f'└ ├ No Genre tags found and thus none added to albums by albumartist: {album_artist} without genre tag\n')

                if update_triggered:

                    # now we know genre, style or both need an update, so evaluate and execute accordingly

                    if (replacement_style and replacement_genre): # i.e both need changing

                        # print(f'├ Replacing genre:\n└ NULL\n  └ WITH {vetted_genres_and_styles}')
                        # print(f'├ Replacing style:\n└ NULL\n  └ WITH {replacement_style}')                
                        # write out changes to all albums where genre and style tag have no data
                        dbcursor.execute('''UPDATE alib SET genre = (?), style = (?) WHERE ( albumartist = (?) COLLATE NOCASE AND (genre IS NULL AND style IS NULL));''', (replacement_genre, replacement_style, album_artist))

                    else:
                        # print(f'├ Replacing genre:\n└ NULL\n  └ WITH {vetted_genres_and_styles}')
                        # write out changes to all albums where genre tag has no data
                        dbcursor.execute('''UPDATE alib SET genre = (?) WHERE ( albumartist = (?) COLLATE NOCASE AND genre IS NULL);''', (replacement_genre, album_artist))


                    ######################################################################################################################################################################################################
                    # enrich 'Pop/Rock' and 'Jazz' only entries for the same albumartist.  allmusic.com has become lazy with their metadata, 
                    # often assigning only Pop/Rock or 'Jazz' to an album so this code adds to genre where an album has only 'Pop/Rock' or 'Jazz'as assigned genre and there are other albums by the same
                    # albumartist in the library that have richer genre and style metadata.  This could poison a few albums with incorrect genre and style assignments [where artist cross genres in their 
                    # discography], however, there should be more correct than incorrect results and incorrect results can be noted when browsing music or encountering anomlies in genre based playlists
                    # and the incorrect genre entries manually removed with a tagger.
                    #
                    # now update all records related to album_artist that have ony 'Pop/Rock' or 'Jazz' as genre entry and no style entry
                    # create a list for 'Pop/Rock' only albums and another for 'Jazz' only albums
                    ######################################################################################################################################################################################################

                    ##################################################################
                    # check for existence of Pop/Rock only entries for this albumartist
                    ##################################################################

                    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists3 ON alib (
                                            albumartist
                                        )
                                        WHERE (albumartist IS NOT NULL COLLATE NOCASE AND 
                                               (genre = 'Pop/Rock' AND 
                                                style IS NULL) );''')

                    dbcursor.execute('''SELECT genre                                           
                                          FROM alib
                                         WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                (genre = 'Pop/Rock' AND 
                                                 style IS NULL) );''', (album_artist,))

                    sub_records = dbcursor.fetchall()
                    # if there are matching records and we have a vetted_genres_and_styles entry to augment with
                    if len(sub_records) > 0 and vetted_genres_and_styles:


                        # define augmented_genre to incorporate ['Pop/Rock'] and whatever is already in vetted_genres_and_styles
                        augmented_genre = list_to_delimited_string(sorted(set(vetted_genres_and_styles + ['Pop/Rock'])))
                        print(f"├ Replacing all instances of genre:\n└ 'Pop/Rock' with:\n  └ {augmented_genre} for albumartist: {album_artist}")


                        if replacement_style:
                            # if there's a style value write both genre and style tags
                            dbcursor.execute('''UPDATE alib
                                                   SET genre = (?),
                                                       style = (?) 
                                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                        (genre = 'Pop/Rock' AND 
                                                         style IS NULL) );''', (augmented_genre, replacement_style, album_artist))
                            # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                            # # increment loop_mods to take account of the changes just proceassed
                            # loop_mods = tally_mods()

                        else:

                            dbcursor.execute('''UPDATE alib
                                                   SET genre = (?)
                                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                        (genre = 'Pop/Rock' AND 
                                                         style IS NULL) );''', (augmented_genre, album_artist))
                            # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                            # increment loop_mods to take account of the changes just processed
                            # loop_mods = tally_mods()

                    ###############################################################################################
                    # now check for existence of Pop only entities for this albumartist and augment with 'Pop/Rock'
                    ###############################################################################################
                    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists4 ON alib (
                                            albumartist
                                        )
                                        WHERE (albumartist IS NOT NULL COLLATE NOCASE AND 
                                               (genre = 'Pop' AND 
                                                style IS NULL) );''')

                    dbcursor.execute('''SELECT genre
                                          FROM alib
                                         WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                (genre = 'Pop' AND 
                                                 style IS NULL) );''', (album_artist,))

                    sub_records = dbcursor.fetchall()
                    # if there are matching records and we have a vetted_genres_and_styles entry to augment with
                    if len(sub_records) > 0:

                        if vetted_genres_and_styles:

                            # define augmented_genre to incorporate ['Pop', 'Pop/Rock'] and whatever is already in vetted_genres_and_styles
                            augmented_genre = list_to_delimited_string(sorted(set(vetted_genres_and_styles + ['Pop', 'Pop/Rock'])))
                        else:

                            # otherwise just augment with ['Pop', 'Pop/Rock']
                            augmented_genre = list_to_delimited_string(sorted(['Pop', 'Pop/Rock']))                    


                        print(f"├ Replacing all instances of genre:\n└ 'Pop' with:\n  └ {augmented_genre} for albumartist: {album_artist}")

                        if replacement_style:
                        # if there's a style value write both genre and style tags

                            dbcursor.execute('''UPDATE alib
                                                   SET genre = (?),
                                                       style = (?) 
                                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                        (genre = 'Pop' AND 
                                                         style IS NULL) );''', (augmented_genre, replacement_style, album_artist))
                            # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                            # increment loop_mods to take account of the changes just processed
                            # loop_mods = tally_mods()


                        else:
                        # if there's no style value write genre tags only
                            dbcursor.execute('''UPDATE alib
                                                   SET genre = (?) 
                                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                        (genre = 'Pop' AND 
                                                         style IS NULL) );''', (augmented_genre, album_artist))
                            # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                            # increment loop_mods to take account of the changes just processed
                            # loop_mods = tally_mods()


                    ##################################################################
                    # check for existence of Jazz only entities for this albumartist
                    ##################################################################
                    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albartists5 ON alib (
                                            albumartist
                                        )
                                        WHERE (albumartist IS NOT NULL COLLATE NOCASE AND 
                                               (genre = 'Jazz' AND 
                                                style IS NULL) );''')


                    dbcursor.execute('''SELECT genre
                                          FROM alib
                                         WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                (genre = 'Jazz' AND 
                                                 style IS NULL) );''', (album_artist,))

                    sub_records = dbcursor.fetchall()
                    if len(sub_records) > 0:

                        if vetted_genres_and_styles:

                            # define augmented_genre to incorporate ['Pop', 'Pop/Rock'] and whatever is already in vetted_genres_and_styles
                            augmented_genre = list_to_delimited_string(sorted(set(vetted_genres_and_styles + ['Jazz'])))
                            print(f"├ Replacing all instances of genre:\n└ 'Jazz' with:\n  └ {augmented_genre} for albumartist: {album_artist}")

                            if replacement_style:
                            # if there's a style value write both genre and style tags
                            
                                dbcursor.execute('''UPDATE alib
                                                       SET genre = (?),
                                                           style = (?) 
                                                     WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                            (genre = 'Jazz' AND 
                                                             style IS NULL) );''', (augmented_genre, replacement_style, album_artist))
                                # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                                # increment loop_mods to take account of the changes just processed
                                # loop_mods = tally_mods()

                            
                        else:
                            dbcursor.execute('''UPDATE alib
                                                   SET genre = (?)
                                                 WHERE (albumartist = (?) COLLATE NOCASE AND 
                                                        (genre = 'Jazz' AND 
                                                         style IS NULL) );''', (augmented_genre, album_artist))
                            # print(f"    └── {tally_mods() - loop_mods} records were modified\n")
                            # increment loop_mods to take account of the changes just proceassed
                            # loop_mods = tally_mods()

                else:
                    print(f'├ No genre or style tags found in library for albums by {album_artist}\n')                


    conn.commit()
    dbcursor.execute('DROP INDEX IF EXISTS albartists1;')
    dbcursor.execute('DROP INDEX IF EXISTS albartists2;')
    dbcursor.execute('DROP INDEX IF EXISTS albartists3;')
    dbcursor.execute('DROP INDEX IF EXISTS albartists4;')
    dbcursor.execute('DROP INDEX IF EXISTS albartists5;')
    closing_tally = tally_mods()
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def title_keywords_to_subtitle():

    ''' strip subtitle keywords from track titles and write them to subtitle tag '''

    keywords = [
    '(Remastered %)%',
    '[Remastered %]%',
    '(remastered %)%',
    '[remastered %]%',
    '(Acoustic %)%',
    '[Acoustic %]%',
    '(acoustic %)%',
    '[acoustic %]%',
    '(acoustic)%',
    '(Acoustic)%',
    '(Single Version)%',
    '(Album Version)%',
    '(LP Version)%',
    '(%remix%)%',
    '(%demo)%',
    '(explicit)%',
    '(instrumental%)%']

    ''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')

    print('\n')
    dbcursor.execute(f'''CREATE INDEX IF NOT EXISTS ix_titles_subtitles ON alib(title, subtitle) WHERE title IS NOT NULL;''')
    opening_tally = tally_mods()
    for keyword in keywords:

        print(f"Stripping {keyword} from track titles and appending to SUBTITLE tag...")
        # first update SUBTITLE where SUBTITLE IS NOT NULL
        dbcursor.execute('''UPDATE alib
                               SET subtitle = subtitle || '\\\\' || trim(substr(title, instr(title, ?) ) ),
                                   title = trim(substr(title, 1, instr(title, ?) - 1) ) 
                             WHERE (title LIKE ?) AND 
                                   subtitle IS NOT NULL;''', (keyword, keyword, keyword))
        # now update titles and subtitles where SUBTITLE IS NULL
        dbcursor.execute('''UPDATE alib
                               SET title = trim(substr(title, 1, instr(title, ?) - 1) ),
                                   subtitle = substr(title, instr(title, ?) ) 
                             WHERE (title LIKE ?) AND 
                                   subtitle IS NULL;''', (keyword, keyword, keyword))
    dbcursor.execute(f"DROP INDEX IF EXISTS ix_titles_subtitles")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def live_in_subtitle_means_live():
    ''' Ensure that any tracks that have Live in the SUBTITLE tag are designated LIVE = 1 '''
    opening_tally = tally_mods()
    print("\nEnsuring any tracks that have Live in the SUBTITLE tag are designated LIVE = 1")
    dbcursor.execute('''UPDATE alib
                           SET live = '1'
                         WHERE LOWER(subtitle) LIKE '%[live%' OR 
                               LOWER(subtitle) LIKE '%(live%' OR 
                               LOWER(subtitle) LIKE '% live %' AND 
                               live != '1';''')
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def live_means_live_in_subtitle():
    ''' Ensure any tracks that have LIVE = 1 also have [Live] in the SUBTITLE'''
    print("\nEnsuring any tracks that have LIVE = 1 also have [Live] in the SUBTITLE")
    opening_tally = tally_mods()
    dbcursor.execute('''UPDATE alib
                           SET subtitle = IIF(subtitle IS NULL OR 
                                              TRIM(subtitle) = '', '[Live]', subtitle || '\\\\[Live]') 
                         WHERE live = '1' AND 
                               NOT (instr(lower(subtitle), ' live ') ) AND 
                               NOT (instr(lower(subtitle), '[live') ) AND 
                               NOT (instr(lower(subtitle), '(live') );''')

    print(f"|\n{tally_mods() - opening_tally} changes were processed")


# def tag_live_tracks():

#     ''' Removing variations of 'Live ' from track TITLE, ensure [Live] in the SUBTITLE and Live=1'''
#     print("\nEnsuring any tracks that have LIVE = 1 also have [Live] in the SUBTITLE")
#     opening_tally = tally_mods()



#     live_instances = [
#     '(Live In',
#     '[Live In',
#     '(Live in',
#     '[Live in',
#     '(live in',
#     '[live in',
#     '(Live At',
#     '[Live At',
#     '(Live at',
#     '[Live at',
#     '(live at',
#     '[live at']

#     ''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
#     dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')

#     print('\n')
#     dbcursor.execute(f"CREATE INDEX IF NOT EXISTS ix_titles_subtitles ON alib(title, subtitle)")
#     opening_tally = tally_mods()
#     for live_instance in live_instances:

#         print(f"Stripping {live_instance} from track titles...")
#         dbcursor.execute(f"UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), subtitle = substr(title, instr(title, ?)) WHERE (title LIKE ? AND subtitle IS NULL);", (live_instance, live_instance, '%'+live_instance+'%'))
#         dbcursor.execute(f"UPDATE alib SET subtitle = subtitle || '\\\\' || trim(substr(title, instr(title, ?))), title = trim(substr(title, 1, instr(title, ?) - 1) ) WHERE (title LIKE ? AND subtitle IS NOT NULL);", (live_instance, live_instance, '%'+live_instance+'%'))

#     dbcursor.execute(f"DROP INDEX IF EXISTS ix_titles_subtitles")
#     print(f"|\n{tally_mods() - opening_tally} records were modified")



def strip_live_from_titles():

    ''' iterate each record and remove live from track title, mark the track as live and append [Live] to subtitle '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')

    records_to_process = [''] # initiate a list of one item to satisfy a while list is not empty
    iterator = 0
    exhausted_queries = 0
    mismatched_brackets = 0
    print(f"\nStripping ([Live]) from track titles...")

    # while records_to_process and not exhausted_queries: # PEP 8 recommended method for testing whether or not a list is empty.  exhausted_queries is a trigger that's activated when it's time to exit the while loop

    ''' Select all records with (live%)% or [live%]% in title '''
    dbcursor.execute('''SELECT title,
                               subtitle,
                               live,
                               rowid
                          FROM alib
                         WHERE title LIKE '%(live%)%' OR 
                               title LIKE '%[live%]%';''')

    records_to_process = dbcursor.fetchall()
    record_count = len(records_to_process)

    # ''' test whether we've hit that point where the query keeps returning the same records with mismatching brackets, in which case it's time to exit the while loop '''
    # if mismatched_brackets == record_count:

    #     exhausted_queries = 1

    print(f"Asessing {record_count} records...")


    ''' now process each in sequece '''
    for record in records_to_process:

        ''' loop through records to test whether they're all mismatched brackets'''
        row_title = record[0]

        # ''' if the entry contains a matching opening and closing bracket pair process it, otherwise skip over it as it isn't a record that should be processed '''
        # if ('[' in row_title and ']' in row_title) or ('(' in row_title and ')' in row_title):

        ''' we've not exhausted query results, so continue processing '''
        row_subtitle = record[1]  # record's subtitle field
        row_islive = record[2] # record's live field
        row_to_process = record[3] # rowid of record being processed


        ''' test for which matching bracket pairs and set up the bracket variables '''
        if '[' in row_title and ']' in row_title:

            opening_bracket = row_title.index('[')
            closing_bracket =  row_title.index(']') + 1

        elif '(' in row_title and ')' in row_title:

                opening_bracket = row_title.index('(')
                closing_bracket =  row_title.index(')') + 1

        ''' generate substring and title values from the existing title'''
        sub = row_title[opening_bracket:closing_bracket]
        base = row_title.replace(sub, '').strip()

        ''' set live value '''
        islive = '1' if 'live' in sub.lower() else None

        ''' concatenate row subtitle if there's a pre-existing subtitle '''
        row_subtitle = sub if row_subtitle is None else (row_subtitle + ' ' + sub).strip()

        ''' write out the changed title, subtitle and live fields based on the values just derived '''
        dbcursor.execute('''UPDATE alib set title = (?), subtitle = (?) WHERE rowid = (?);''', (base, row_subtitle, row_to_process))
        if (row_islive is None or row_islive == '0') and islive == 1:
            dbcursor.execute('''UPDATE alib set live = (?) WHERE rowid = (?);''', (islive, row_to_process))

        # else:

        #     ''' seeing as the record was a wash increment the number of mismatches.  when this number equals the record_count of records matching the select statement it's time to trigger exhausted_queries to break the while loop '''
        #     mismatched_brackets += 1
        #     print("Encountered mismatched bracket pair # {mismatched_brackets}, skipping record")

        # ''' test whether we've hit that point where the query keeps returning the same records with mismatching brackets, in which case it's time to exit the while loop '''
        # if mismatched_brackets == record_count:

        #     exhausted_queries = 1

    dbcursor.execute(f"DROP INDEX IF EXISTS titles")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def tags_to_dedupe():
    ''' list all known text tags you might want to process against -- this needs to become a function call so there's a common definition throughout the app
    genre and style tags re excluded as they're deduped when validated.
    This function should ideally be called after anything dealing with track titles, subtitles etc.'''

    return(["_releasecomment", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", 
        "engineer", "ensemble", "isrc", "label", "lyricist", "mixer", "mood", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", 
        "musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "performer", "personnel", "producer", "recordinglocation", "releasetype", 
        "remixer", "subtitle", "theme", "upc", "version", "writer"])



def dedupe_tags():
    ''' remove duplicate tag entries in text fields present in alib that may contain duplicate entries
    genre and style tags re excluded as they're deduped when validated.
    This function should ideally be called after anything dealing with track titles, subtitles etc.'''

    ''' get list of text tags actually present in the alib table, based on what's been imported into alib '''
    text_tags = texttags_in_alib(tags_to_dedupe())
    print(f"\nDeduping tags:")
    opening_tally = tally_mods()

    for text_tag in text_tags:
        
        query = (f"CREATE INDEX IF NOT EXISTS ix_dedupe_tag ON alib({text_tag}) WHERE {text_tag} IS NOT NULL;")

        dbcursor.execute(query)
        print(f"- {text_tag}")

        ''' get list of matching records '''
        ''' as you cannot pass variables as field names to a SELECT statement build the query string dynamically then run it '''
        query = f"SELECT rowid, {text_tag} FROM alib WHERE {text_tag} IS NOT NULL;"
        dbcursor.execute(query)

        ''' now process each matching record '''
        records = dbcursor.fetchall()
        records_returned = len(records) > 0
        if records_returned:
              
            for record in records:
                
                stored_value = record[1] # record value for comparison
                if '\\\\' in stored_value:
                    ''' first get the stored contents sorted and reconstituted without removing any items '''
                    split_value = stored_value.split("\\\\")
                    split_value.sort()
                    sorted_stored_value =  '\\\\'.join([str(item) for item in split_value])


                    ''' now depupe the stored value, sort the result and reconstitute the resulting string '''
                    deduped_value = list(set(stored_value.split("\\\\")))
                    deduped_value.sort()
                    final_value = '\\\\'.join([str(item) for item in deduped_value])
                    
                    '''now compare the sorted original string against the sorted deduped string and write back only those that are not the same '''
                    if final_value != sorted_stored_value:

                        ''' write out {final_value} to {text_tag}  '''
                        row_to_process = record[0]
                        
                       
                        query = f"UPDATE alib SET {text_tag} = (?) WHERE rowid = (?);", (final_value, row_to_process)
                        print(query)
                        dbcursor.execute(*query)

    dbcursor.execute("DROP INDEX IF EXISTS dedupe_tag")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def kill_singular_discnumber():
    ''' get rid of discnumber when all tracks in __dirpath have discnumber = 1.  I'm doing this the lazy way because I've not spent enough time figuring out the CTE update query in SQL.  This is a temporary workaround to be replaced with a CTE update query '''
    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    # dbcursor.execute('''WITH GET_SINGLE_DISCS AS ( SELECT __dirpath AS cte_value FROM ( SELECT DISTINCT __dirpath, discnumber FROM alib WHERE discnumber IS NOT NULL AND lower(__dirname) NOT LIKE '%cd%' AND lower(__dirname) NOT LIKE '%cd%') GROUP BY __dirpath HAVING count( * ) = 1 ORDER BY __dirpath ) SELECT cte_value FROM GET_SINGLE_DISCS;''')

    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_dirpaths_discnumbers ON alib (
                            __dirpath,
                            discnumber
                        );''')

    dbcursor.execute('''WITH GET_SINGLE_DISCS AS (
                                            SELECT __dirpath AS cte_value,
                                                   discnumber
                                              FROM (
                                                       SELECT DISTINCT __dirpath,
                                                                       discnumber
                                                         FROM alib
                                                        WHERE --discnumber IS NOT NULL AND 
                                                              (__dirname NOT LIKE '%cd%' AND 
                                                               __dirname NOT LIKE 'CD_' AND 
                                                               __dirname NOT LIKE 'CD %' AND                                                                
                                                               __dirname NOT LIKE 'D_' AND 
                                                               __dirname NOT LIKE 'DISC_' AND 
                                                               __dirname NOT LIKE 'DISC _' AND 
                                                               __dirpath NOT LIKE '%/Michael Jackson - HIStory Past, Present and Future, Book I%' AND 
                                                               __dirpath NOT LIKE '%Depeche Mode - Singles Box%' AND 
                                                               __dirpath NOT LIKE '%Disc%' AND 
                                                               __dirpath NOT LIKE '%/Lambchop – Tour Box/%' AND 
                                                               __dirpath NOT LIKE '%/Pearl Jam Evolution - Gold Box Set/%' AND 
                                                               __dirpath NOT LIKE '%4CD Box/%' AND 
                                                               __dirpath NOT LIKE '%Boxset/CD%' AND 
                                                               __dirpath NOT LIKE '%Live/d%' AND 
                                                               __dirpath NOT LIKE '%Unearthed/Unearthed%' AND 
                                                               __dirpath NOT LIKE '%/Robin Trower - Original Album Series, Vol. 2/%' AND 
                                                               __dirpath NOT LIKE '%/The Cult - Love (Omnibus Edition, 4xCD, 2009)/%' AND 
                                                               __dirpath NOT LIKE '%/The Cult - Rare Cult - The Demo Sessions (5xCD, Boxset) [2002]/%' AND 
                                                               __dirpath NOT LIKE '%/The Doors - Perception Boxset%' AND 
                                                               __dirpath NOT LIKE '%/qnap/qnap2/T/T1/The Flower Kings/2018 Bonus%' AND 
                                                               __dirpath NOT LIKE '%/VA/%') 
                                                   )
                                             GROUP BY __dirpath
                                            HAVING count( * ) = 1
                                             ORDER BY __dirpath
                                        )
                                        SELECT cte_value
                                          FROM GET_SINGLE_DISCS
                                         WHERE discnumber = '1' OR discnumber = '01';''')

    queryresults  = dbcursor.fetchall()
    print(f"\n")
    for query in queryresults:
        var = query[0]
        print(f"Removing discnumber = '1' from {var}.")
        dbcursor.execute("UPDATE alib SET discnumber = NULL where __dirpath = ?", (var,))

    dbcursor.execute('''DROP INDEX IF EXISTS dirpaths_discnumbers;''')
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def strip_live_from_album_name():
    ''' Strip all occurences of '(live)' from end of album name '''
    print(f"\nStripping all occurences of '(live)' from end of album name, ensuring album is marked live and updating subtitle where required")

    opening_tally = tally_mods()
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albums ON alib(album) WHERE album IS NOT NULL''')

    ''' set live flag '''
    dbcursor.execute('''UPDATE alib
                           SET live = '1'
                         WHERE lower(substr(album, -6) ) IN ('(live)', '[live]') AND 
                               live IS NULL OR 
                               live != '1';''')
    ''' enrich subtitle '''
    dbcursor.execute('''UPDATE alib
                           SET subtitle = iif(subtitle = '' OR 
                                              subtitle IS NULL, substr(album, -6), subtitle || " " || substr(album, -6) ) 
                         WHERE lower(substr(album, -6) ) IN ('(live)', '[live]') AND 
                               subtitle NOT LIKE '%[live]%' AND 
                               subtitle NOT LIKE '%(live)%';''')
    ''' strip live from album title '''
    dbcursor.execute('''UPDATE alib
                           SET album = trim(substr(album, 1, length(album) - 7) ) 
                         WHERE lower(substr(album, -6) ) IN ('(live)', '[live]');''')

    print(f"|\n{tally_mods() - opening_tally} changes were processed")
    


def merge_album_version():
    ''' merge album name and version fields into album name '''
    print(f"\nMerging album name and version fields into album name where version tag does not already appear in album name")
    opening_tally = tally_mods()
    dbcursor.execute('''UPDATE alib SET album = album || ' ' || version WHERE version IS NOT NULL AND NOT INSTR(lower(album), lower(version));''')
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def merge_genre_style():
    ''' merge genre and style fields into genre field '''
    print(f"\nMerging genre and style tags into genre tag where style tag does not already appear in genre tag")
    opening_tally = tally_mods()
    dbcursor.execute(f"UPDATE alib SET genre = genre || '\\' || style WHERE style IS NOT NULL AND NOT INSTR(genre, style);")
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def split_album_version():
    ''' split album name and version fields, reverting album tag to album name '''
    print(f"\nRemoving VERSION tag from ABUM tag")
    opening_tally = tally_mods()
    dbcursor.execute('''UPDATE alib
                           SET album = substring(album, 1, INSTR(album, version) - 2) 
                         WHERE version IS NOT NULL AND 
                               INSTR(album, version);''')
    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def set_compilation_flag():
    ''' set compilation = '1' when __dirname starts with 'VA -' and '0' otherwise '''
    print(f"\nSetting COMPILATION = '1' / '0' depending on whether __dirname starts with 'VA -' or 'Various Artists - '")
    opening_tally = tally_mods()
    dbcursor.execute('''
                        UPDATE alib
                           SET compilation = '1'
                         WHERE (compilation IS NULL OR 
                                compilation != '1' AND 

                                (substring(__dirname, 1, 4) = 'VA -' OR
                                substring(__dirname, 1, 17) = 'Various Artists - ' ) AND 
                                (albumartist IS NULL OR
                                albumartist = 'Various Artists'));''')

    dbcursor.execute('''
                        UPDATE alib
                           SET compilation = '0'
                         WHERE (compilation IS NULL OR 
                                compilation != '0' AND 
                                (substring(__dirname, 1, 4) != 'VA -' OR 
                                substring(__dirname, 1, 17) != 'Various Artists - ' ) AND 
                                albumartist IS NOT NULL);''')

    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def nullify_albumartist_in_va():
    ''' remove 'Various Artists' value from ALBUMARTIST tag '''
    print(f"\nRemoving 'Various Artists' from ALBUMARTIST and ENSEMBLE tags")
    opening_tally = tally_mods()
    dbcursor.execute('''
                        UPDATE alib
                           SET albumartist = NULL
                         WHERE lower(albumartist) = 'various artists';''')

    dbcursor.execute('''
                        UPDATE alib
                           SET ensemble = NULL
                         WHERE lower(ensemble) = 'various artists';''')

    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def capitalise_releasetype():
    print(f"\nSetting 'First Letter Caps' for all instances of releasetype")
    opening_tally = tally_mods()
    dbcursor.execute('''SELECT DISTINCT releasetype FROM alib WHERE releasetype IS NOT NULL;''')
    releasetypes = dbcursor.fetchall()
    for release in releasetypes:
    
        flc = firstlettercaps(release[0])
        print(f'{release[0]} > {flc}')
        # SQLite WHERE clause is case sensistive so this should not repeatedly upddate records every time it is run
        dbcursor.execute('''UPDATE alib SET releasetype = (?) WHERE releasetype = (?) AND releasetype != (?);''', (flc, release[0], flc))

    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def add_releasetype():
    print(f"\nAdding releasetype tag where none is present")
    opening_tally = tally_mods()

    # set Singles
    print('Detecting and setting Singles...')
    dbcursor.execute('''WITH cte AS (
                            SELECT DISTINCT __dirpath,
                                            count( * ) AS track_count,
                                            releasetype,
                                            genre
                              FROM alib
                             WHERE releasetype IS NULL AND 
                                   genre NOT LIKE '%Classical%' AND 
                                   genre NOT LIKE '%Jazz%' 
                             GROUP BY __dirpath
                             ORDER BY track_count,
                                      __dirpath
                        )
                        UPDATE alib
                           SET releasetype = 'Single'
                         WHERE __dirpath IN (
                            SELECT __dirpath
                              FROM cte
                             WHERE track_count <= 3 AND 
                                   releasetype IS NULL
                        );''')

    # set EPs
    print('Detecting and setting EPs...')
    dbcursor.execute('''WITH cte AS (
                            SELECT DISTINCT __dirpath,
                                            count( * ) AS track_count,
                                            releasetype,
                                            genre
                              FROM alib
                             WHERE releasetype IS NULL AND 
                                   genre NOT LIKE '%Classical%' AND 
                                   genre NOT LIKE '%Jazz%' 
                             GROUP BY __dirpath
                             ORDER BY track_count,
                                      __dirpath
                        )
                        UPDATE alib
                           SET releasetype = 'Ep'
                         WHERE __dirpath IN (
                            SELECT __dirpath
                              FROM cte
                             WHERE track_count > 3 AND 
                                   track_count <= 6
                        );''')

    # set Soundtracks
    print('Detecting and setting Soundtracks...')
    dbcursor.execute('''UPDATE alib
                           SET releasetype = 'Soundtrack'
                         WHERE __dirpath LIKE '%/OST%' AND 
                               releasetype IS NULL;''')

    # set Albums
    print('Detecting and setting Albums...')
    dbcursor.execute('''WITH cte AS (
                            SELECT DISTINCT __dirpath,
                                            count( * ) AS track_count,
                                            releasetype
                              FROM alib
                             WHERE releasetype IS NULL
                             GROUP BY __dirpath
                             ORDER BY track_count,
                                      __dirpath
                        )
                        UPDATE alib
                           SET releasetype = 'Album'
                         WHERE __dirpath IN (
                            SELECT __dirpath
                              FROM cte
                             WHERE track_count > 6
                        );''')

    print(f"|\n{tally_mods() - opening_tally} changes were processed")



def add_tagminder_uuid():
    ''' this uuid v4 is to be an immutable once generated.  The idea behind it is every file will have an UUID added once which makes future tagging updates impervious to
     file name and/or location changes because updates will be based on UUID rather than __path '''
    print(f"\nAdding a file UUID tag where none exists - this makes future tagging operations impervious to file location/name")
    opening_tally = tally_mods()
    # Get all records lacking a tagminder_uuid tag entry
    dbcursor.execute('''SELECT rowid, __dirname, __filename FROM alib WHERE tagminder_uuid IS NULL or trim(tagminder_uuid) = '' order by __path;''')
    records_to_process = dbcursor.fetchall()

    ''' now process each in sequence '''
    for record in records_to_process:

        row_id = record[0] # get rowid
        row_dirname = record[1] # get __dirname
        row_filename = record[2] # get filename

        # generate a uuid v4 value
        uuidval = str(uuid.uuid4())
        #print(f'Adding UUID {uuidval} to {record[1]}/{record[2]}')
        # Add the generated UUID to the row entry
        dbcursor.execute('''UPDATE alib set tagminder_uuid = (?) WHERE rowid = (?);''', (uuidval, record[0]))

    print(f"|\n{tally_mods() - opening_tally} changes were processed")


def establish_alib_contributors():
    ''' build list of unique contibutors by gathering all mbid's found in alib - checking against artist and albumartist and composer fields '''

    print("\nBuilding a list of unique contibutors (composers, artists & albumartists) that have a single MBID in alib by gathering all mbid's found in alib and checking against artist and albumartist fields\nCheck all namesakes_* tables in database to manually investigate namesakes")


    dbcursor.execute('''DROP TABLE IF EXISTS role_albumartist;''')
    dbcursor.execute('''DROP TABLE IF EXISTS role_artist;''')
    dbcursor.execute('''DROP TABLE IF EXISTS namesakes_albumartist;''')
    dbcursor.execute('''DROP INDEX IF EXISTS role_albumartists;''')
    dbcursor.execute('''DROP TABLE IF EXISTS contributor_with_mbid;''')
    dbcursor.execute('''DROP TABLE IF EXISTS namesakes_artist;''')
    dbcursor.execute('''DROP TABLE IF EXISTS role_composer;''')

    dbcursor.execute('''DROP INDEX IF EXISTS role_albumartists;''')
    dbcursor.execute('''DROP INDEX IF EXISTS role_artists;''')
    dbcursor.execute('''DROP INDEX IF EXISTS role_composers;''')
    dbcursor.execute('''DROP INDEX IF EXISTS contributors_with_mbid;''')

    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_albumartists ON alib(albumartist) WHERE albumartist IS NOT NULL;''')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_artists ON alib(artist) WHERE artist IS NOT NULL;''')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_composers ON alib(composer) WHERE composer IS NOT NULL;''')


    # create contributors table from all albumartists, artists and composers where the abumartist, artist and composer names only appear once alongside an mbid
    dbcursor.execute('''CREATE TABLE role_albumartist AS SELECT *
                                                          FROM (
                                                               WITH GET_PERFORMERS AS (
                                                                       SELECT albumartist AS contributor,
                                                                              musicbrainz_albumartistid AS mbid
                                                                         FROM (
                                                                                  SELECT DISTINCT albumartist,
                                                                                                  musicbrainz_albumartistid
                                                                                    FROM alib
                                                                                   WHERE albumartist NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_albumartistid IS NOT NULL AND 
                                                                                         musicbrainz_albumartistid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) = 1
                                                                   )
                                                                   SELECT contributor,
                                                                          mbid 
                                                                     FROM GET_PERFORMERS
                                                                    ORDER BY contributor
                                                               );'''
                    )

    dbcursor.execute('''CREATE TABLE role_artist AS SELECT *
                                                          FROM (
                                                               WITH GET_PERFORMERS AS (
                                                                       SELECT artist AS contributor,
                                                                              musicbrainz_artistid AS mbid
                                                                         FROM (
                                                                                  SELECT DISTINCT artist,
                                                                                                  musicbrainz_artistid
                                                                                    FROM alib
                                                                                   WHERE artist NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_artistid IS NOT NULL AND 
                                                                                         musicbrainz_artistid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) = 1
                                                                   )
                                                                   SELECT contributor,
                                                                          mbid
                                                                     FROM GET_PERFORMERS
                                                                    ORDER BY contributor
                                                               );'''
                    )


    dbcursor.execute('''CREATE TABLE role_composer AS SELECT *
                                                          FROM (
                                                               WITH GET_PERFORMERS AS (
                                                                       SELECT composer AS contributor,
                                                                              musicbrainz_composerid AS mbid
                                                                         FROM (
                                                                                  SELECT DISTINCT composer,
                                                                                                  musicbrainz_composerid
                                                                                    FROM alib
                                                                                   WHERE composer NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_composerid IS NOT NULL AND 
                                                                                         musicbrainz_composerid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) = 1
                                                                   )
                                                                   SELECT contributor,
                                                                          mbid
                                                                     FROM GET_PERFORMERS
                                                                    ORDER BY contributor
                                                               );'''
                    )


    # create namesakes table from all albumartists, artists and composers where the name appears > 1 alongside an mbid
    # these tables are for users to manually investigate the underlying albums and assign mbid's based on the correct namesake
    dbcursor.execute('''CREATE TABLE namesakes_albumartist AS SELECT *
                                                          FROM (
                                                               WITH GET_NAMESAKES AS (
                                                                       SELECT albumartist AS contributor,
                                                                              musicbrainz_albumartistid
                                                                         FROM (
                                                                                  SELECT DISTINCT albumartist,
                                                                                                  musicbrainz_albumartistid
                                                                                    FROM alib
                                                                                   WHERE albumartist NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_albumartistid IS NOT NULL AND 
                                                                                         musicbrainz_albumartistid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) > 1
                                                                   )
                                                                   SELECT contributor
                                                                          
                                                                     FROM GET_NAMESAKES
                                                                    ORDER BY contributor
                                                               );'''
                    )

    # create namesakes table from all artists where the artist name appears > 1 alongside an mbid
    dbcursor.execute('''CREATE TABLE namesakes_artist AS SELECT *
                                                          FROM (
                                                               WITH GET_NAMESAKES AS (
                                                                       SELECT artist AS contributor,
                                                                              musicbrainz_artistid
                                                                         FROM (
                                                                                  SELECT DISTINCT artist,
                                                                                                  musicbrainz_artistid
                                                                                    FROM alib
                                                                                   WHERE artist NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_artistid IS NOT NULL AND 
                                                                                         musicbrainz_artistid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) > 1
                                                                   )
                                                                   SELECT contributor
                                                                          
                                                                     FROM GET_NAMESAKES
                                                                    ORDER BY contributor
                                                               );'''
                    )
                                           
# create namesakes table from all composers where the composer name appears > 1 alongside an mbid
    dbcursor.execute('''CREATE TABLE namesakes_composer AS SELECT *
                                                          FROM (
                                                               WITH GET_NAMESAKES AS (
                                                                       SELECT composer AS contributor,
                                                                              musicbrainz_composerid
                                                                         FROM (
                                                                                  SELECT DISTINCT composer,
                                                                                                  musicbrainz_composerid
                                                                                    FROM alib
                                                                                   WHERE composer NOT LIKE '%\\%' AND 
                                                                                         musicbrainz_composerid IS NOT NULL AND 
                                                                                         musicbrainz_composerid NOT LIKE '%\\%'
                                                                              )
                                                                        GROUP BY contributor
                                                                       HAVING count( * ) > 1
                                                                   )
                                                                   SELECT contributor
                                                                          
                                                                     FROM GET_NAMESAKES
                                                                    ORDER BY contributor
                                                               );'''
                    )
    


    # now bring them together in a single table of distinct contributors and associated MBIDs
    dbcursor.execute('''CREATE TABLE contributor_with_mbid AS SELECT contributor,
                                                                     mbid
                                                                FROM role_albumartist
                        UNION
                        SELECT contributor,
                               mbid
                          FROM role_artist
                        UNION
                        SELECT contributor,
                               mbid
                          FROM role_composer
                         ORDER BY contributor;'''
                    )

    # finally, write out any of the contributors in alib that don't have a match in mb_disambiguated to contributors_not_in_mbrainz
    # these are artists you may want to add to the MusicBrainz database
    # NOT THIS IS A COMPROMISE SOLUTON FOR THE MOMENT IN THAT WE REALLY SHOULD BE LEVERAGING PYTHON TO SPLIT ALL DELIMITED ENTRIES TO GET A FULL POPULATION OR WE NEED TO FIND AN EFFICIENT SQL QUERY TO SPLIT AND UNION ALL
    dbcursor.execute('''DROP TABLE IF EXISTS contributors_not_in_mbrainz;''')


    dbcursor.execute('''DROP TABLE IF EXISTS contributors_not_in_mbrainz;''')
    dbcursor.execute('''DROP TABLE IF EXISTS alib_contributors;''')
    dbcursor.execute('''CREATE TABLE alib_contributors AS SELECT DISTINCT artist
                                                            FROM alib
                                                           WHERE NOT instr(artist, '\\') AND 
                                                                 trim(artist) IS NOT NULL AND 
                                                                 trim(artist) != ''
                        UNION
                        SELECT DISTINCT albumartist AS contributor
                          FROM alib
                         WHERE NOT instr(albumartist, '\\') AND 
                               trim(albumartist) IS NOT NULL AND 
                               trim(albumartist) != ''
                        UNION
                        SELECT DISTINCT composer AS contributor
                          FROM alib
                         WHERE NOT instr(composer, '\\') AND 
                               trim(composer) IS NOT NULL AND 
                               trim(composer) != ''
                        UNION
                        SELECT DISTINCT lyricist AS contributor
                          FROM alib
                         WHERE NOT instr(lyricist, '\\') AND 
                               trim(lyricist) IS NOT NULL AND 
                               trim(lyricist) != ''
                        UNION
                        SELECT DISTINCT writer AS contributor
                          FROM alib
                         WHERE NOT instr(writer, '\\') AND 
                               trim(writer) IS NOT NULL AND 
                               trim(writer) != ''
                        UNION
                        SELECT DISTINCT engineer AS contributor
                          FROM alib
                         WHERE NOT instr(engineer, '\\') AND 
                               trim(engineer) IS NOT NULL AND 
                               trim(engineer) != ''
                        UNION
                        SELECT DISTINCT producer AS contributor
                          FROM alib
                         WHERE NOT instr(producer, '\\') AND 
                               trim(producer) IS NOT NULL AND 
                               trim(producer) != ''
                         ORDER BY contributor;''')


    # get contributors in alib not in mb_disambiguated
    dbcursor.execute('''SELECT contributor
                          FROM alib_contributors
                         WHERE contributor NOT IN (
                                   SELECT entity
                                     FROM mb_disambiguated
                               );''')





def add_mb_entities():
    ''' if mb_disambiguated table exists adds musicbrainz identifiers to artists, albumartists & composers (we're adding musicbrainz_composerid, musicbrainz_engineerid and musicbrainz_producerid of our own volition for future app use) '''

    # check if the mb_disambiguated table exists.  If it's present then populate mbid's from it where records are currently without.  mb_disambiguated contains only names from mb_master that occor only once in that table
    
    if table_exists('mb_disambiguated'):

        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lalbumartists ON alib(LOWER(albumartist)) WHERE albumartist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lartists ON alib(LOWER(artist)) WHERE artist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lcomposers ON alib(LOWER(composer)) WHERE composer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lengineers ON alib(LOWER(engineer)) WHERE engineer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lproducers ON alib(LOWER(producer)) WHERE producer IS NOT NULL;''')

        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lmb_disambiguated on mb_disambiguated(LOWER(entity));''')
            
        print(f"\nAdding musicbrainz identifiers to artists & albumartists by referencing MusicBrainz MBID table")
        opening_tally = tally_mods()


        # in all cases here we want to ensure that the existing mbid value in alib or thevalue written to alib corresponds with the relevant artist match.
        # we match lowecase as case differences would mean unnecessary mismatches
        # 
        #    update t1
        #       set id = t2.id
        #      from t2
        #     where t2.name = t1.name;


        # albumartist updates
        print('Updating albumartists')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_albumartistid = mb_disambiguated.mbid
                              FROM mb_disambiguated
                             WHERE (lower(mb_disambiguated.entity) = lower(alib.albumartist) AND 
                                    alib.musicbrainz_albumartistid IS NULL );''')

        # artist updates
        print('Updating artists')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_artistid = mb_disambiguated.mbid
                              FROM mb_disambiguated
                             WHERE (lower(mb_disambiguated.entity) = lower(alib.artist) AND 
                                   alib.musicbrainz_artistid IS NULL );''')

        # composer updates
        print('Updating composers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_composerid = mb_disambiguated.mbid
                              FROM mb_disambiguated
                             WHERE (lower(mb_disambiguated.entity) = lower(alib.composer) AND 
                                   alib.musicbrainz_composerid IS NULL );''')

        # engineer updates
        print('Updating engineers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_engineerid = mb_disambiguated.mbid
                              FROM mb_disambiguated
                             WHERE (lower(mb_disambiguated.entity) = lower(alib.engineer) AND 
                                   alib.musicbrainz_engineerid IS NULL );''')

        # producer updates
        print('Updating producers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_producerid = mb_disambiguated.mbid
                              FROM mb_disambiguated
                             WHERE (lower(mb_disambiguated.entity) = lower(alib.producer) AND 
                                   alib.musicbrainz_producerid IS NULL );''')


        print(f"|\n{tally_mods() - opening_tally} changes were processed")

    else:
        # seeing as there's no master table to work from, let's enrich using mbid's already present in alib table
        # build list of unique contibutors by gathering all mbid's found in alib - checking against artist, albumartist and composer fields
        establish_alib_contributors()

        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lrole_albumartists ON alib(LOWER(albumartist)) WHERE albumartist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lrole_artists ON alib(LOWER(artist)) WHERE artist IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lrole_composers ON alib(LOWER(composer)) WHERE composer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lrole_engineers ON alib(LOWER(engineer)) WHERE engineer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lrole_producers ON alib(LOWER(producer)) WHERE producer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lcontributors on contributor_with_mbid(LOWER(contributor)) WHERE contributor IS NOT NULL;''')
            
        print(f"\nAdding musicbrainz identifiers to artists & albumartists")
        opening_tally = tally_mods()

        # in this scenario we cannot compare mbid's because it'd potentially cause namesakes that have previously been resolved to be reassigned and again conflated
        # thus we do not include the alib mbid != contributor_with_mbid mbid condition in the where clause

        # albumartist updates
        print('Updating albumartists')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_albumartistid = contributor_with_mbid.mbid
                              FROM contributor_with_mbid
                             WHERE (lower(contributor_with_mbid.artist) = lower(alib.albumartist) AND 
                                   alib.musicbrainz_albumartistid IS NULL);''')
           
        # artist updates
        print('Updating artists')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_artistid = contributor_with_mbid.mbid
                              FROM contributor_with_mbid
                             WHERE (lower(contributor_with_mbid.artist) = lower(alib.artist) AND 
                                   alib.musicbrainz_artistid IS NULL);''')

        # composer updates
        print('Updating composers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_composerid = contributor_with_mbid.mbid
                              FROM contributor_with_mbid
                             WHERE (lower(contributor_with_mbid.artist) = lower(alib.composer) AND 
                                   alib.musicbrainz_composerid IS NULL);''')

        # engineer updates
        print('Updating engineers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_engineerid = contributor_with_mbid.mbid
                              FROM contributor_with_mbid
                             WHERE (lower(contributor_with_mbid.artist) = lower(alib.engineer) AND 
                                   alib.musicbrainz_engineerid IS NULL);''')
        # producer updates
        print('Updating producers')
        dbcursor.execute('''UPDATE alib
                               SET musicbrainz_producerid = contributor_with_mbid.mbid
                              FROM contributor_with_mbid
                             WHERE (lower(contributor_with_mbid.artist) = lower(alib.producer) AND 
                                   alib.musicbrainz_producerid IS NULL);''')

        print(f"|\n{tally_mods() - opening_tally} changes were processed")


def add_multiartist_mb_entities():
    ''' if mb_disambiguated table exists adds musicbrainz identifiers to artists, albumartists & composers (we're adding musicbrainz_composerid of our own volition for future app use) '''

    # check if the mb_disambiguated table exists.  If it's present then populate mbid's from it where records are currently without
    
    if table_exists('mb_disambiguated'):

        # I believe these indexes are duplicated under add_mb_entities() using a different naming convention
        # dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lalbumartists ON alib(LOWER(albumartist)) WHERE albumartist IS NOT NULL;''')
        # dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lartists ON alib(LOWER(artist)) WHERE artist IS NOT NULL;''')
        # dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lcomposers ON alib(LOWER(composer)) WHERE composer IS NOT NULL;''')
        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_lmb_artists on mb_disambiguated(LOWER(entity));''')




        # firstly update all MBIDs in table alib that are readily matchable to a contributor, correcting any legacy mismatch of mbids that might be present in alib
        # (e.g. where a tagger has previously selected the wrong artist for whatever reason)
        # only write changes if the current mbid value does not correspond with the mbid value in mb_disambiguated which is pulled directly from the MusicBrainz official dataset

        add_mb_entities()  # update individual MBIDs


        # now that we've added individual mbid's above, now create a union of all unique combinations of artists, albumartists and composers and index it
        dbcursor.execute('''DROP TABLE IF EXISTS multi_contributors;''')
        dbcursor.execute('''CREATE TABLE multi_contributors AS SELECT lower(artist) AS contributor,
                                                                      musicbrainz_artistid AS mbid
                                                                 FROM alib
                                                                WHERE artist IS NOT NULL

                            UNION

                            SELECT lower(albumartist) AS contributor,
                                   musicbrainz_albumartistid AS mbid
                              FROM alib
                             WHERE albumartist IS NOT NULL

                            UNION

                            SELECT lower(composer) AS contributor,
                                   musicbrainz_composerid AS mbid
                              FROM alib
                             WHERE composer IS NOT NULL

                            UNION

                            SELECT lower(engineer) AS contributor,
                                   musicbrainz_engineerid AS mbid
                              FROM alib
                             WHERE engineer IS NOT NULL

                            UNION

                            SELECT lower(producer) AS contributor,
                                   musicbrainz_producerid AS mbid
                              FROM alib
                             WHERE producer IS NOT NULL

                             ORDER BY contributor;''')

        dbcursor.execute('''CREATE INDEX IF NOT EXISTS ix_multi_contributor_index ON multi_contributors (
                                contributor
                            );''')


        # select from multi_contributors only instances where the number of contributor names and number of mbid's are mismatched for repprocessing
        # NOTE: This means that incorrect associations will not be universally corrected - you may want to turn this off for one mass update at some point

        dbcursor.execute('''SELECT DISTINCT contributor,
                                            mbid,
                                            (LENGTH(contributor) - LENGTH(REPLACE(contributor, '\\', '') ) ) / LENGTH('\\') AS acount,
                                            (LENGTH(mbid) - LENGTH(REPLACE(mbid, '\\', '') ) ) / LENGTH('\\') AS idcount
                              FROM multi_contributors
                             WHERE acount > 1 AND 
                                   acount != idcount ORDER BY contributor;''')


        records = dbcursor.fetchall()
        population = len(records)
        if population > 0:

            # Create an mbid_updates table
            #
            # NNNNNNB - Review logic to ensure it's ok to persist table as opposed to augment it
            #

            dbcursor.execute('''CREATE TABLE IF NOT EXISTS mbid_updates (contributor text, mbid text);''')

            print(f'{population} distinct multi-artist entries to process')
            #iterate through every record
            for record in records:

                #store the baseline values which would serve as the replacement criteria where a table update is required
                baseline_contributor = record[0]
                baseline_mbid = record[1]
                derived_mbid = []

                # generate a list from the delimited contributors string retrieved from the Sqlite table
                contributors = delimited_string_to_list(baseline_contributor)

                # now retrieve mbid for the individual contributor and append it to derived_mbid, building up a list with each mbid match found
                for contributor in contributors:

                    dbcursor.execute('''SELECT mbid from mb_disambiguated where LOWER(mb_disambiguated.entity) == (?);''', (contributor,))

                    retrieved_mbid = dbcursor.fetchone()
                    # check if the select statement returned a match, and if it did, append it to the derived_mbid list
                    if retrieved_mbid is not None:
                        derived_mbid.append(retrieved_mbid[0])
        
                # now turn the list into a delimited string
                derived_mbid = list_to_delimited_string(derived_mbid)

                # if derived_mbid differs from baseline mbid write out a record to table mbid_updates which will then form the basis from which to update matching records in alib

                if derived_mbid != baseline_mbid:

                    dbcursor.execute('''INSERT INTO mbid_updates (
                                                                     contributor,
                                                                     mbid
                                                                 )
                                                                 VALUES (
                                                                     (?),
                                                                     (?)
                                                                 );''', (baseline_contributor, derived_mbid))


                # now we have an updated pairing of contributors and mbids where there's more than one contributor write updates to alib where the contributors match artist, albumartist and composer


                # # process artists
                dbcursor.execute('''UPDATE alib
                                       SET musicbrainz_artistid = mbid_updates.mbid
                                      FROM mbid_updates
                                     WHERE mbid_updates.contributor = lower(alib.artist);''')

                # process albumartists
                dbcursor.execute('''UPDATE alib
                                       SET musicbrainz_albumartistid = mbid_updates.mbid
                                      FROM mbid_updates
                                     WHERE mbid_updates.contributor = lower(alib.albumartist);''')


                # process composers
                dbcursor.execute('''UPDATE alib
                                       SET musicbrainz_composerid = mbid_updates.mbid
                                      FROM mbid_updates
                                     WHERE mbid_updates.contributor = lower(alib.composer);''')

                # process engineers
                dbcursor.execute('''UPDATE alib
                                       SET musicbrainz_engineerid = mbid_updates.mbid
                                      FROM mbid_updates
                                     WHERE mbid_updates.contributor = lower(alib.engineer);''')

                # process producers
                dbcursor.execute('''UPDATE alib
                                       SET musicbrainz_producerid = mbid_updates.mbid
                                      FROM mbid_updates
                                     WHERE mbid_updates.contributor = lower(alib.producer);''')


# def vet_contributor(entity): # entity can be any contributor tagname: artist, albumartist, composer, engineer, producer etc.

#     # get all distinct entries relating to the entity
#     dbcursor.execute('''SELECT DISTINCT ?
#                               FROM alib
#                              WHERE ? IS NOT NULL
#                              ORDER BY lower(?)''', (entity, entity, entity))
#     matching_entities = dbcursor.fetchall()

#     if len(matching_entities) > 0:

#         #if we have entries, process them in turn
#         for item in matching_entities:

#             #get entity entry and get a lowercase version of same
#             stored_entity = item[0]
#             l_stored_entity = stored_entity.lower()

#             print(f"Checking whether {stored_entity} exists in local copy of MusicBrainz contributors")
#             dbcursor.execute('''SELECT artist
#                                   FROM mb_disambiguated
#                                  WHERE (lower(mb_disambiguated.entity) = ?), (l_stored_entity))''')
#             result = dbcursor.fetchall()
#             if len(result) > 0

                







def standardise_album_tags(tag):
    ''' this function takes a tagname as parameter, runs a query against alib using the tagname as criteria then, finds __dirnames where contents across files in __dirname are inconsisent, merges
    all strings from each matching record into a consolidated string, dedupes and sorts it and writes the result back to all records associated with the __dirname where any record's contents does not 
    correspond with the derived value.

    Its purpose is to ensure consistent genre, style, moood and theme metadata across all tracks in an album.
    Ideally it should only be called after vetting genres and styles, but in the grand scheme of things the order doesn't matter as it'll yield the identical outcome after both have run '''

    tagname = tag

    print(f'Standardising {tagname} consistency for all albums in alib...')

    # first get a list of all __dirpath's that have more than one unique value associated with the tag being processed
    # at some point investigate why passing tagname through using (?) results in a null result search
    dbcursor.execute(f'SELECT __dirpath FROM (SELECT DISTINCT __dirpath, {tagname} FROM alib) GROUP BY __dirpath HAVING count( * ) > 1 ORDER BY __dirpath;')

    affected_dirpaths = dbcursor.fetchall()

    if affected_dirpaths is not None:

        # Now process the contents of each of the affected_dirpath's in turn
        # get all distinct tag entries associated with that __dirpath from the alib table
        # concatenate them into a long list
        # dedupe and sort the resulting string
        # write back the deduped, sorted concatenated result to all records associated with that __dirpath where the existing value doesn't match that we've derived

        for dirpath in affected_dirpaths:

            # reset concatenated_list to ensure we're not carring over any remnants of previous iterations
            concatenated_list = []
            # pickup the query results one at a time and process the associated {tagname} values
            dp = dirpath[0]
            print(f"Processing: {dirpath[0]}")

            dbcursor.execute(f"SELECT DISTINCT {tagname} FROM alib WHERE __dirpath = (?);", (dp,))
            distinct_tag_entries = dbcursor.fetchall()
            if distinct_tag_entries is not None:

                # build up the concatenated list, making sure to not to incorporate None
                for distinct_tag in distinct_tag_entries:

                    if distinct_tag[0] is not None:
                        concatenated_list.append(distinct_tag[0])
            # now convert it to a string, dedupe and sort it
            concatenated_string = dedupe_and_sort(list_to_delimited_string(concatenated_list))
            # write back the updte to alib
            dbcursor.execute(f"UPDATE alib SET {tagname} = (?) WHERE __dirpath = (?);", (concatenated_string, dp))
            print(f"Setting {tagname} to: {concatenated_string}\n")


def tag_mixed_res_albums():
    ''' Add [Mixed Res] to end of version tag where file bit depth and/or sample rates differ - ensuring we don't add the string if it's already present '''

    print(f"\nIdentifying all albums where bit depth and/or sample rates differs between files and appending ' [Mixed Res]'' to end of all version tags if not already present")

    dbcursor.execute('''WITH cte AS (
                            SELECT __dirpath
                              FROM (
                                       SELECT DISTINCT __dirpath,
                                                       __bitspersample,
                                                       __frequency_num
                                         FROM alib
                                   )
                             GROUP BY __dirpath
                            HAVING count( * ) > 1
                             ORDER BY __dirpath
                        )
                        UPDATE alib
                           SET version = IIF(version IS NULL, '', trim(version)) || ' [Mixed Res]'
                         WHERE __dirpath IN cte AND (
                            version IS NULL OR version NOT LIKE '%[Mixed Res]%');''')


def tag_non_redbook():
    ''' Add album res to end of all version tags where file bit depth and/or sample rates > 16/44.1 respectively - ensuring we don't add the string if it's already present or is a [Mixed Res] album '''

    ''' first deal with mixed resolution albums '''
    tag_mixed_res_albums()


    ''' Now deal with any > redbook '''
    print(f"\nAdding bit_depth and sampling rate to end of all version tags where file bit depth and/or sample rates > 16/44.1 respectively whilst ensuring we don't add the string if it's already present or the album is a [Mixed Res] album")

    # update version tag with album resolution metadata if it's not already present in the version tag
    # SQLite3 instr() returns NULL if a field value is NULL so leverage '' as a value

    dbcursor.execute('''
                        WITH cte AS (
                            SELECT __dirpath
                              FROM (
                                       SELECT DISTINCT __dirpath,
                                                       album,
                                                       version,
                                                       __bitspersample,
                                                       __frequency_num
                                         FROM alib
                                   )
                             WHERE ( (__bitspersample > '16' OR 
                                      __frequency_num > '44.1') AND 
                                     version IS NULL) OR 
                                   ( (__bitspersample > '16' OR 
                                      __frequency_num > '44.1') AND 
                                     instr(lower(version), 'khz]') = 0 AND 
                                     instr(lower(version), '[mixed res]') = 0) 
                        )
                        UPDATE alib
                           SET version = iif(version IS NULL, '[' || __bitspersample || __frequency || ']', version || ' [' || __bitspersample || __frequency || ']') 
                         WHERE __dirpath IN cte;''')

def find_duplicate_flac_albums():
    ''' this is based on records in the alib table as opposed to file based metadata imported using md5sum.  The code relies on the md5sum embedded in properly encoded FLAC files - it basically takes them, creates a concatenated string
    from the sorted md5sum of al tracks in a folder and compares that against the same for all other folders.  If the strings match you have a 100% match of the audio stream and thus duplicate album, irrespective of tags.
    '''

    print(f'\nChecking for FLAC files that do not have an md5sum in the tag header')

    dbcursor.execute("DROP TABLE IF EXISTS nonstandard_FLACS;")
    dbcursor.execute("CREATE TABLE IF NOT EXISTS nonstandard_FLACS AS SELECT DISTINCT __dirpath FROM alib WHERE (__filetype = 'FLAC' AND (__md5sig = '' OR __md5sig = '0' OR __md5sig is null)) ORDER BY __path;")
    dbcursor.execute("SELECT __dirpath from nonstandard_FLACS")
    invalid_flac_albums = dbcursor.fetchall()
    if invalid_flac_albums:

        print(f"|\nInvalid FLAC albums present.  Be careful not to delete albums with invalid ('0' or empty) __md5sig.\nSee table 'nonstandard_FLACS' for a list of folders containing nonstandard FLAC files that should be re-encoded")
        

    print(f"\nSearching for duplicated flac albums based on __md5sig")
    duplicated_flac_albums = 0

    '''Create table in which to store concatenated __md5sig for all __dirnames '''

    dbcursor.execute('''DROP TABLE IF EXISTS __dirpath_content_concat__md5sig;''')

    dbcursor.execute('''CREATE TABLE __dirpath_content_concat__md5sig (
                        __dirpath      TEXT,
                        concat__md5sig TEXT);''')

    '''populate table with __dirpath and concatenated __md5sig of all files associated with __dirpath (note order by __md5sig to ensure concatenated __md5sig is consistently generated irrespective of physical record sequence). '''

    dbcursor.execute('''INSERT INTO __dirpath_content_concat__md5sig (
                                                                         __dirpath,
                                                                         concat__md5sig
                                                                     )
                                                                     SELECT __dirpath,
                                                                            group_concat(__md5sig, " | ") 
                                                                       FROM (
                                                                                SELECT __dirpath,
                                                                                       __md5sig
                                                                                  FROM alib
                                                                                 ORDER BY __dirpath,
                                                                                          __md5sig
                                                                            )
                                                                      GROUP BY __dirpath;''')


    ''' create table in which to store all __dirnames with identical FLAC contents (i.e. the __md5sig of each FLAC in folder is concatenated and compared) '''

    dbcursor.execute('''DROP TABLE IF EXISTS __dirpaths_with_same_content;''')

    dbcursor.execute('''CREATE TABLE __dirpaths_with_same_content (
                        killdir        TEXT,
                        __dirpath      TEXT,
                        concat__md5sig TEXT
                    );''')


    ''' now write the duplicate records into a separate table listing all __dirname's that have identical FLAC contents '''

    dbcursor.execute('''INSERT INTO __dirpaths_with_same_content (
                                                                     __dirpath, 
                                                                     concat__md5sig
                                                                 )
                                                                 SELECT __dirpath,
                                                                        concat__md5sig
                                                                   FROM __dirpath_content_concat__md5sig
                                                                  WHERE concat__md5sig IN (
                                                                            SELECT concat__md5sig
                                                                              FROM __dirpath_content_concat__md5sig
                                                                             GROUP BY concat__md5sig
                                                                            HAVING count( * ) > 1
                                                                        )
                                                                  ORDER BY concat__md5sig,
                                                                           __dirpath;''')



    ''' create table for listing directories in which FLAC files should be deleted as they're duplicates '''

    dbcursor.execute('''DROP TABLE IF EXISTS __dirpaths_with_FLACs_to_kill;''')

    dbcursor.execute('''CREATE TABLE __dirpaths_with_FLACs_to_kill (
                                                                        __dirpath      TEXT,
                                                                        concat__md5sig TEXT
                                                                    );''')

    ''' populate table listing directories in which FLAC files should be deleted as they're duplicates '''

    dbcursor.execute('''INSERT INTO __dirpaths_with_FLACs_to_kill (
                                                                      __dirpath,
                                                                      concat__md5sig
                                                                  )
                                                                  SELECT __dirpath,
                                                                         concat__md5sig
                                                                    FROM __dirpaths_with_same_content
                                                                   WHERE rowid NOT IN (
                                                                             SELECT min(rowid) 
                                                                               FROM __dirpaths_with_same_content
                                                                              GROUP BY concat__md5sig
                                                                         );''')


    dbcursor.execute('''SELECT COUNT(*) FROM __dirpaths_with_same_content''')
    duplicated_flac_albums = dbcursor.fetchone()
    if duplicated_flac_albums[0] == 0:
        ''' sqlite returns null from a sum operation if the field values are null, so test for it, because if the script is run iteratively that'll be the case where alib has been readied for export '''
        print(f"|\n0 duplicated FLAC albums present")
    else:
        print(f"|\n{duplicated_flac_albums[0]} duplicated FLAC albums present - see table __dirpaths_with_same_content for a listing")



def generate_string_grouper_input():

#    build up table of contributors made up of artist, performers, albumartists and composers, engineers, producers,labels & recordinglocation
#    at this stage we're avoiding dealing with delimited artists and artists with comma space
#    we'll come back to these in the future.

#    This process culminates in the creation of a table called string_grouper_input which is
#    then referenced by string-grouper to generate a list of likely artist name duplicates
#    for investigation by the user.

#    Following investigation, the user generates a CSV comprising current name and replacement 
#    name, which is then imported into the disambiguation table.  This table is leveraged by 
#    Python code which reads the disambiguation table into a Python dictionary and pulls all 
#    artist, performer, albumartist, composer, egineer, producer and label records from alib into a Pandas dataframe, 
#    applies the changes set out in the disambiguation table and writes the changes back to 
#    the alib table.

#    In practice, one would run this routine to build up string_grouper_input, feed it through 
#    string-grouper, populate the disambiguation table with additional records based on the human curated 
#    results of string-grouper and then run the Python routine that makes the update a few times 
#    over until such time that the user is satisfied dimininshing returns have set in such that
#    it no longer warrants further pursuit.


# during this routine a number of tables are created/leveraged
# ct is a temp table
   
    print('Generating distinct_contributors table which holds distinct list of artist, performers, albumartists, composers, writers, lyricists, engineers and producers present in your tags')
    # right, what we're doing here is creating a list of all contributors across artist, albumrtist, composer, lyricist, writer, performer, engineer and producer
    # method:  
    # get distinct records from each table, union all into a temp table
    # write all entries containing '\\' to a secondary temp table
    # read secondary temp table entries into a list
    # split list using Python function
    # add all entries from the primary temp table to the split list
    # convert list to a set to elimiate duplicate list items
    # sort it
    # write it out to sqlite table called all_contributors
    # feed that into the end result.

    dbcursor.execute("DROP TABLE IF EXISTS ct")
    dbcursor.execute("DROP TABLE IF EXISTS ct_singles")
    dbcursor.execute("DROP TABLE IF EXISTS ct_delimited")
    dbcursor.execute("DROP TABLE IF EXISTS distinct_contributors")

    dbcursor.execute("CREATE TEMP TABLE IF NOT EXISTS ct (contributor TEXT)")
    dbcursor.execute("CREATE INDEX ix_ct on ct(contributor)")
    dbcursor.execute("CREATE TEMP TABLE IF NOT EXISTS ct_singles (contributor TEXT)")
    dbcursor.execute("CREATE INDEX ix_ct_singles on ct_singles(contributor)")
    dbcursor.execute("CREATE TEMP TABLE IF NOT EXISTS ct_delimited (contributor TEXT)")
    dbcursor.execute("CREATE INDEX ix_ct_delimited on ct_delimited(contributor)")

    # grab all 
    # -artist
    # -albumartist
    # -performer
    # -composer
    # -lyricist
    # -writer
    # -engineer
    # -producer
    # 
    # names and insert into ct table
    dbcursor.execute('''INSERT INTO
                          ct (contributor)
                        SELECT DISTINCT
                          artist
                        FROM
                          alib
                        WHERE
                          artist IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT
                          albumartist
                        FROM
                          alib
                        WHERE
                          albumartist IS NOT NULL
                        UNION ALL
                        -- SELECT DISTINCT
                        --   performer
                        -- FROM
                        --   alib
                        -- WHERE
                        --   performer IS NOT NULL
                        -- UNION ALL
                        SELECT DISTINCT
                          composer
                        FROM
                          alib
                        WHERE
                          composer IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT
                          lyricist
                        FROM
                          alib
                        WHERE
                          lyricist IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT
                          writer
                        FROM
                          alib
                        WHERE
                          writer IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT
                          engineer
                        FROM
                          alib
                        WHERE
                          engineer IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT
                          producer
                        FROM
                          alib
                        WHERE
                          producer IS NOT NULL;''')

    dbcursor.execute('''INSERT INTO ct_singles 
                        SELECT DISTINCT
                          contributor
                        FROM
                          ct
                        WHERE
                          contributor NOT LIKE '%\\%'
                        ORDER BY
                          contributor;''')

    dbcursor.execute('''INSERT INTO ct_delimited 
                        SELECT DISTINCT
                          contributor
                        FROM
                          ct
                        WHERE
                          contributor LIKE '%\\%'
                        ORDER BY
                          contributor;''')


    # get all records from delimited_contributors
    dbcursor.execute('''SELECT contributor
                        FROM
                          ct_delimited;''')


    # now grab the lot into a list
    delim_records = [row[0] for row in dbcursor.fetchall()]

    # split every string into discrete words
    split_list = [item for sublist in delim_records for item in sublist.split('\\')]
    print(f"Splitting contributor fields with delimited text gives rise to {len(split_list)} rows")
    # deduplicate and sort split_list
    unique_sorted_split_list = sorted(set(split_list))
    # release mem
    split_list.clear()

    # get all records from ct_singles
    dbcursor.execute('''SELECT contributor
                        FROM
                          ct_singles;''')
    # now grab the lot into a list
    singles_records = [row[0] for row in dbcursor.fetchall()]
    
    merged_list = sorted(set(unique_sorted_split_list + singles_records))

    # create a dict including lowercase of every record
    data_to_insert = [(item, item.lower(), 0) for item in merged_list]



    # Ensure the all_contributors table exists
    dbcursor.execute("CREATE TABLE IF NOT EXISTS distinct_contributors (contributor TEXT PRIMARY KEY, lcontributor TEXT, processed INTEGER, mbid TEXT)")

    # Batch insert all unique, sorted entries from the merged list into a table for processing via string_grouper
    # this table is used externally by the user to feed lists to string grouper and find possible namesakes, misspelled arist names, text case differences. irregular spacing, 'the ' etc.
    # the script to reference is grouper-taglib.py

    # Use executemany for efficient bulk insertion
    dbcursor.executemany('''INSERT OR IGNORE INTO distinct_contributors (contributor, lcontributor, processed, mbid)
                            VALUES (?, ?, ?, NULL)''', data_to_insert)


    # index all_contributors
    dbcursor.execute('''CREATE INDEX ix_distinct_contributors ON distinct_contributors(contributor)''')
    dbcursor.execute('''CREATE INDEX ix_ldistinct_contributors ON distinct_contributors(lcontributor)''')    


    # create table only if it doesn't exist - if it's there preserve prior work
    dbcursor.execute("CREATE TABLE IF NOT EXISTS _INF_contributors_with_commas (contributor TEXT UNIQUE, delimit NULL, lcontributor TEXT, ampersands INTEGER, commas INTEGER)")
    # pull in only those contributors with commas that do not already appear in mb_disambiguated or _REF_matched_on_allmusic (because in both cases they're clearly already correct)
    dbcursor.execute('''INSERT INTO
                          _INF_contributors_with_commas
                        SELECT
                          contributor,
                          NULL AS delimit,
                          lcontributor,
                          (
                            LENGTH (contributor) - LENGTH (REPLACE (contributor, '&', ''))
                          ) / LENGTH ('&') AS ampersands,
                          (
                            LENGTH (contributor) - LENGTH (REPLACE (contributor, ',', ''))
                          ) / LENGTH (',') AS commas
                        FROM
                          distinct_contributors
                        WHERE
                          (
                            (
                              contributor LIKE '%,%'
                              AND lcontributor NOT IN (
                                SELECT
                                  lentity
                                FROM
                                  mb_disambiguated
                              )
                              AND contributor NOT IN (
                                SELECT
                                  contributor
                                FROM
                                  _REF_matched_on_allmusic
                              )
                            )
                          ) ON CONFLICT DO NOTHING;''')

    # create table only if it doesn't exist - if it's there preserve prior work
    dbcursor.execute("CREATE TABLE IF NOT EXISTS _INF_contributors_with_ampersand (contributor TEXT UNIQUE, delimit NULL, lcontributor TEXT, ampersands INTEGER, commas INTEGER)")
    # pull in only those contributors with commas that do not already appear in mb_disambiguated (because they're clearly correct)
    dbcursor.execute('''INSERT INTO
                          _INF_contributors_with_ampersand
                        SELECT
                          contributor,
                          NULL AS delimit,
                          lcontributor,
                          (
                            LENGTH (contributor) - LENGTH (REPLACE (contributor, '&', ''))
                          ) / LENGTH ('&') AS ampersands,
                          (
                            LENGTH (contributor) - LENGTH (REPLACE (contributor, ',', ''))
                          ) / LENGTH (',') AS commas
                        FROM
                          distinct_contributors
                        WHERE
                          contributor LIKE '%&%'
                          AND lcontributor NOT IN (
                            SELECT
                              lentity
                            FROM
                              mb_disambiguated
                          )
                          AND contributor NOT IN (
                            SELECT
                              contributor
                            FROM
                              _REF_matched_on_allmusic
                          ) ON CONFLICT DO NOTHING;''')


    dbcursor.execute('''select count(*) from distinct_contributors;''')
    records_generated = dbcursor.fetchall()[0][0]
    print(f'Your library contains {records_generated} unique artists, albumartists composers, lyricists, witers, performers, engineers & producers')

    # now generate string grouper output
    df = pd.read_sql_query(f'SELECT rowid, contributor, FALSE FROM distinct_contributors;', conn) # import all distinct contributors derived from alib

    # match strings using string grouper capability
    matches = match_strings(df['contributor'], min_similarity=0.85) # tweak similarity threshold for your level of precision ... lower score = more records in results
    # Look at only the non-exact matches:
    similarities = matches[matches['left_contributor'] != matches['right_contributor']].head(None)
    #similarities = matches[matches['similarity'] < 0.9]

    # write out to a csv file
    similarities.to_csv('string_grouper_output.csv', index=False, sep = '|')

    # write out to a sqlite table
    similarities.to_sql('string_grouper_output', conn, index=True, if_exists='replace')

    # so what we now have is string_grouper's take on likely matches, based on speficied similarity threshold
    # as we have processed records in the past and accepted them as true (1) or false (0), we should delete all matching records from disambiguation table
    # do this by deleting records in string_grouper that match records in disambuguation table












    # # generate list of distinct labels --- this needs to follow above logic and then get repeated for recordinglocations
    # # probably best dealt with as a seperate table
    # # grab all single entry label names and insert into string_grouper_labels table
    # dbcursor.execute('''CREATE TABLE string_grouper_labels AS SELECT DISTINCT label AS contributor
    #                                                             FROM alib
    #                                                            WHERE label IS NOT NULL AND 
    #                                                                  label NOT LIKE '%\\%' AND 
    #                                                                  label NOT LIKE '%, %'
    #                                                            ORDER BY label;''')


    # dbcursor.execute('''select count(*) from string_grouper_labels;''')

    # records_generated = dbcursor.fetchall()[0]
    # print(f'Your library contains {records_generated} unique labels')

    # # grab all single entry recordinglocation names and insert into string_grouper_recordinglocations table
    # dbcursor.execute('''CREATE TABLE string_grouper_recordinglocations AS SELECT DISTINCT recordinglocation AS contributor
    #                                                                         FROM alib
    #                                                                        WHERE recordinglocation IS NOT NULL AND 
    #                                                                              recordinglocation NOT LIKE '%\\%' AND 
    #                                                                              recordinglocation NOT LIKE '%, %'
    #                                                                        ORDER BY recordinglocation;''')

    # dbcursor.execute('''select count(*) from string_grouper_recordinglocations''')

    # records_generated = dbcursor.fetchall()[0]
    # print(f'Your library contains {records_generated} unique recordinglocations')






# ''' Here we begin utilising Pandas DFs capabilities to make mass updates made to artist, performer, composer and albumartist data'''
# def convert_dfrow(row, delim: str = r"\\"):
#     result = []

#     for item in row:
#         if not pd.isna(item):
#             item = delim.join(disambiguation_dict.get(x, x) for x in item.split(delim))

#         result.append(item)

#     return result    

def compare_large_dataframes(df1, df2):

    # Example usage:
    # Assuming df1 and df2 are your DataFrames
    # differing_records will contain only the rows in df2 that differ from df1
    # differing_records = compare_dataframes(df1, df2)

    '''This method uses the merge function with the indicator parameter set to True. It then filters the resulting DataFrame to keep only the rows present in df2 but not in df1.
    Depending on your specific use case, this approach might offer better performance, especially for large datasets.
    However, it's always a good idea to test with your actual data to determine which method works best for your situation.'''


    # Check if the DataFrames have the same shape
    if df1.shape != df2.shape:
        raise ValueError("DataFrames must have the same number of rows and columns")

    # Merge the DataFrames and keep only the rows that are different
    merged_df = pd.merge(df1, df2, how='outer', indicator=True).query('_merge == "right_only"').drop('_merge', axis=1)

    return merged_df


def compare_dataframes(df1, df2, index_col):
    """
    Memory-efficient comparison of two dataframes, returning only different records from df2.
    
    Parameters:
    df1 (pandas.DataFrame): Original dataframe
    df2 (pandas.DataFrame): Modified dataframe
    index_col (str): Name of the index column
    
    Returns:
    pandas.DataFrame: Records from df2 that differ from df1
    dict: Performance metrics including memory usage and execution time
    """
    # import time
    # from sys import getsizeof
    
    # start_time = time.time()
    
    # Ensure both dataframes have the same columns
    if not df1.columns.equals(df2.columns):
        raise ValueError("DataFrames must have identical columns")
    
    # Set index for comparison
    df1 = df1.set_index(index_col)
    df2 = df2.set_index(index_col)
    
    # Get indices present in df2 but not in df1 (new records)
    new_records_idx = df2.index.difference(df1.index)
    
    # For records present in both dataframes, compare values efficiently
    common_idx = df2.index.intersection(df1.index)
    
    # Compare only common records using numpy operations
    df1_common = df1.loc[common_idx]
    df2_common = df2.loc[common_idx]
    
    # Create boolean mask for changed rows using numpy operations
    # This is more memory efficient than DataFrame operations
    changed_mask = np.any(
        df1_common.values != df2_common.values, 
        axis=1
    )
    
    # Get indices of changed records
    changed_idx = common_idx[changed_mask]
    
    # Combine changed and new record indices
    different_idx = changed_idx.union(new_records_idx)

    # Get final result
    result = df2.loc[different_idx].reset_index()

    return result


def disambiguate_contributors():
    '''  Function that leverages Pandas DataFrames to apply metadata changes to artist, performer, albumartist composer, engineer and producer fields in a dataframe 
    representing these datapoints in the alib table and then writing back the changes to a table (disambiguation_updates) in the database.  This 
    table is then used to update records in alib based on matching rowid.'''

    # if disambiguation source table is not present there's nothing to do...
    if not table_exists('cleansed_contributors'):

        print('No contributor dismbiguation data present, nothing to disambiguate')
        return


    # first define an inner function convert_dfrow to transform every row in every in-scope column in the df
    ''' Here we begin utilising Pandas DFs capabilities to make mass updates made to artist, albumartist, composer, lyricist, writer, engineer, performer and producer data'''

    def convert_dfrow(row, delim: str = r"\\"):

        '''
        convert_dfrow takes a row from a DataFrame as input and processes each item in the row. Here's a breakdown of what it does:

        Parameters:
        row: The input row, typically a list or Series representing a row from a DataFrame.
        delim: The delimiter used to split and join items in the row. Default value is "\\", which is a regular expression representing a backslash. This delimiter is used to split and join strings within the row.
        Functionality:

        The function initializes an empty list result to store the processed items.
        It iterates over each item in the input row.

        For each item, it checks if the item is not null (pd.isna(item) checks if the item is NaN using pandas library). If the item is not null, it splits the item using the specified delimiter (delim) and applies a transformation
        using a dictionary (disambiguation_dict) to each element obtained after the split.  If the dictionary (disambiguation_dict) contains a key corresponding to the split element, it replaces the split element with the corresponding
        value from the dictionary. This transformation occurs using a generator expression (disambiguation_dict.get(x, x) for x in item.split(delim)). This essentially maps each element to its disambiguated version if available in the dictionary.
        
        The transformed item is then appended to the result list.
        Finally, the function returns the list result containing the processed items from the row.
        In summary, this function takes a DataFrame row, splits each item using a specified delimiter, applies a transformation to each split element based on a dictionary lookup, and returns the list of processed items.
        '''

        result = []

        for item in row:
            if not pd.isna(item):
                item = delim.join(disambiguation_dict.get(x, x) for x in item.split(delim))

            result.append(item)

        return result


    # get records from disambiguation table
    # dbcursor.execute('''SELECT current_val, replacement_val FROM cleansed_contributors WHERE status = FALSE ;''')  # retrieve only disambiguation records not previously processed
    # this means the user needs to ensure they reset to FALSE if they want something reprocessed.  There's an argument to say this isn't necessary - reprocess the lot because it's only dirty records in alib that will be affected.
    dbcursor.execute('''SELECT current_val, replacement_val FROM cleansed_contributors;''')

    disambiguation_records = dbcursor.fetchall()
    disambiguation_count = len(disambiguation_records)

    # if there are unprocessed records in disambiguation table
    if disambiguation_count > 0:

        # convert list of disambiguation records into a dict
        disambiguation_dict = {key: value for key, value in disambiguation_records}

        # load all artist, performer, albumartist and composer records in alib into a df then process the dataframe
        df1 = pd.read_sql_query('SELECT rowid AS alib_rowid, artist, albumartist, composer, writer, lyricist, engineer, producer from alib order by __path', conn)

        # make a copy against which to apply changes which we'll subsequently compare with df1 to isolate changes
        df2 = df1.copy()
        print(f'Stats relating to records imported from alib table for processing:\n')

        # transform the columns of interest by calling innner function convert_dfrow
        df2[['artist', 'albumartist', 'composer', 'engineer', 'lyricist', 'producer', 'writer']] = df2[['artist', 'albumartist', 'composer', 'engineer', 'lyricist', 'producer', 'writer']].apply(convert_dfrow)
        df2.info(verbose=True)

   
        # df3 = compare_large_dataframes(df1, df2)
        # print(f'\nStats relating to records changed through disambiguation and homoginisation process:\n')
        # df3.info(verbose=True)

        df3 = compare_dataframes(df1, df2, 'alib_rowid')
        df3.info(verbose=True)

        # now write changes back to db

        if df3.empty is False:

            # Save df3 to a Sqlite table, replacing it if necessary
            df3.to_sql('disambiguation_updates', conn, if_exists='replace', index=False)

            # process the updates back to alib

            # replaced from example from Keith Metcalfe on SQLite forum.  Essentially same as above but using aliases to improve readability
            dbcursor.execute('''UPDATE 
                                  alib AS target 
                                SET 
                                  artist = source.artist, 
                                  albumartist = source.albumartist, 
                                  composer = source.composer,
                                  engineer = source.engineer,
                                  lyricist = source.lyricist,
                                  producer = source.producer,
                                  writer = source.writer
                                FROM 
                                  disambiguation_updates AS source 
                                WHERE 
                                  source.alib_rowid == target.rowid;''')

            print(f'\nArtist, performer, album, albumartist, composer, writer, lyricist, engineer & producer records relating to {df3.shape[0]} records have been disambiguated and homogenised.')

            # update disambiguation table to mark changes to alib_updated.  This should be called only after the database update
            dbcursor.execute('''UPDATE cleansed_contributors set status = TRUE WHERE status = FALSE ;''')

            #strictly speaking disambiguation_updates table is no longer required, so drop it.  If you want to want to see what was replaced, retain it.
            # dbcursor.execute('''DROP TABLE disambiguation_updates''')
            conn.commit()


    else:
        print('No remaining name corrections to process')


############################################################


#################################################################



def unpad_tracks():

    dbcursor.execute('''UPDATE alib
                           SET track = CAST (CAST (track AS INTEGER) AS TEXT) 
                         WHERE track IS NOT NULL AND 
                               track != CAST (CAST (track AS INTEGER) AS TEXT);''')


def unpad_discnumbers():

    dbcursor.execute('''UPDATE alib
                           SET discnumber = CAST (CAST (discnumber AS INTEGER) AS TEXT) 
                         WHERE discnumber IS NOT NULL AND 
                               discnumber != CAST (CAST (discnumber AS INTEGER) AS TEXT);''')


def set_title_caps():

    dbcursor.execute('''SELECT DISTINCT title
                          FROM alib
                         WHERE title IS NOT NULL
                         ORDER BY title;''')
    titles = dbcursor.fetchall()
    if len(titles) > 0:
        for title in titles:

            stored_title = title[0]
            capitalised_title = rymify(stored_title)

            if stored_title != capitalised_title:

                dbcursor.execute('''UPDATE alib
                                       SET title = (?) 
                                     WHERE title = (?);''', (capitalised_title, stored_title))

def set_album_caps():

    dbcursor.execute('''SELECT DISTINCT album
                          FROM alib
                         WHERE album IS NOT NULL
                         ORDER BY lower(albumartist), lower(album);''')
    albums = dbcursor.fetchall()
    if len(albums) > 0:
        for album in albums:

            stored_album = album[0]
            capitalised_album = rymify(stored_album)
            # print(f'stored album: "{stored_album}", capitalised album "{capitalised_album}", mistmatched: {stored_album != capitalised_album}')
            # input()

            if stored_album != capitalised_album:

                print(f"Current album name: '{stored_album}' \nmismatched\nRevised album name: '{capitalised_album}'")

                dbcursor.execute('''UPDATE alib
                                       SET album = (?) 
                                     WHERE album = (?);''', (capitalised_album, stored_album))

def set_composer_caps():

    dbcursor.execute('''SELECT DISTINCT composer
                          FROM alib
                         WHERE composer IS NOT NULL
                         ORDER BY lower(albumartist), lower(album);''')
    composers = dbcursor.fetchall()
    if len(composers) > 0:
        for composer in composers:

            stored_composer = composer[0]
            capitalised_composer = title_case(replace_demimiters(stored_composer))


            if stored_composer != capitalised_composer:

                print(f"Current composer name: '{stored_composer}' \nmismatched\nRevised composer name: '{capitalised_composer}'")

                dbcursor.execute('''UPDATE alib
                                       SET composer = (?) 
                                     WHERE composer = (?);''', (capitalised_composer, stored_composer))

# def set_artist_caps():

#     dbcursor.execute('''SELECT DISTINCT artist
#                           FROM alib
#                          WHERE artist IS NOT NULL
#                          ORDER BY lower(artist), lower(album);''')
#     artists = dbcursor.fetchall()
#     if len(artists) > 0:
#         for artist in artists:

#             stored_artist = artist[0]
#             l_stored_artist = stored_artist.lower()


#             dbcursor.execute('''SELECT artist
#                                   FROM mb_disambiguated
#                                  WHERE lower(artist) == (?);''', (l_stored_artist,))

#             matched_artist = dbcursor.fetchall()


#             capitalised_artist = title_case(replace_demimiters(stored_artist, 'artist'))


#             if stored_artist != capitalised_artist:

#                 print(f"Current artist name: '{stored_artist}' \nmismatched\nRevised artist name: '{capitalised_artist}'")

#                 dbcursor.execute('''UPDATE alib
#                                        SET artist = (?) 
#                                      WHERE artist = (?);''', (capitalised_artist, stored_artist))


def set_albumartist_caps():

    # this function compares albumartist names against mb_disambiguated and if the text case in alib differs it prefers the text case in mb_disambiguated
    # if no match exists, it compares it against a transformed albumartist calling on title_case(replace_demimiters(artist name))
    # it also creates and populates a table of albumartists not in mb_disambiguated for the user to peruse and check on to ensure they're correct
    # if they're not correct the user can correct them by adding their amendment to cleansed_contributors, which is used elewhere to update mb_entities as well as all entries in alib

    dbcursor.execute('''SELECT DISTINCT albumartist
                          FROM alib
                         WHERE albumartist IS NOT NULL
                         ORDER BY lower(albumartist), lower(album);''')
    albumartists = dbcursor.fetchall()
    if len(albumartists) > 0:
        for albumartist in albumartists:

            # grab first entry
            stored_albumartist = albumartist[0]
            # store lowercase for reference
            l_stored_albumartist = stored_albumartist.lower()

            # here we need some code that checks for mbrainz table, and if exists lookup stored_albumartist.  if found leave name unchanged, if not do capitalisation.
            # Obv means mbrainz capitalisation must agree with what we see in allmusic via cleansed_contributors table
            # so where there are differences we should update the musicbrainz table if that's important to you.
            # So some code should be written to id albumartists in current day alib where their text case differs from mbrainz and then mbrainz master mb_entities must be updated accordingly
            # Note, mb_entities and it's derived tables are already always checked against cleansed_contributors when the script is run, so explicit changes that have been made via cleansed_contributors
            # will always be reflected in mb_entities every time the script is run
            # if you are 100% sure your artist names appear exactly as you want them to in alib, you might choose to update mb_entities from distinct contributors in alib, albeit choosing not to code it at this juncture

            dbcursor.execute('''SELECT artist
                                  FROM mb_disambiguated
                                 WHERE lower(artist) == (?);''', (l_stored_albumartist,))

            matched_artist = dbcursor.fetchall()

            # if there's no match, the artist doesn't exist in musicbrainz data, so transform it:
            if len(matched_artist) == 0:
                capitalised_albumartist = title_case(replace_demimiters(stored_albumartist,'albumartist'))

                if stored_albumartist != capitalised_albumartist:

                    print(f"Current albumartist name: '{stored_albumartist}' \nmismatched\nRevised albumartist name: '{capitalised_albumartist}'")

                    dbcursor.execute('''UPDATE alib
                                           SET albumartist = (?) 
                                         WHERE albumartist = (?);''', (capitalised_albumartist, stored_albumartist))


            else:
                # if there is a match check that the text case is the same ... if not prefer mbrainz text case
                mb_artist = matched_artist[0][0]
                if stored_albumartist != mb_artist:

                    print(f"Current albumartist name: '{stored_albumartist}' \nmismatched with\nRevised albumartist name: '{mb_artist}'")
                    dbcursor.execute('''UPDATE alib SET albumartist = ? WHERE albumartist = ?''', (mb_artist, stored_albumartist))


# def contributors_not_in_mb():

#     dbcursor.execute('''DROP TABLE IF EXISTS contributors_not_in_mbrainz''')

#                     # as there is no match write the name to contributors_not_in_mbrainz

#                     UPDATE contributors_not_in_mbrainz 








def rename_tunes():
    ''' rename all tunes in alib table leveraging the metadta in alib.  Relies on compilation = 0 to detect VA and OST albums
    DO NOT DEPLOY THIS LIGHTLY YET '''


    # get list of all tunes and metadata relevant to naming files
    dbcursor.execute('''SELECT rowid,
                               __path,
                               __filename,
                               __dirpath,
                               __filename_no_ext,
                               __ext,
                               __dirname,
                               discnumber,
                               track,
                               title,
                               subtitle,
                               artist,
                               albumartist,
                               compilation
                          FROM alib
                         ORDER BY __path;''')

    tunes = dbcursor.fetchall()

    print("\nRenaming files based on metadata...")

    for tune in tunes:

        # grab mtadata related to the track in question, ensure all file related metadata is read in as literals
        tune_rowid = tune[0]
        tune_path = tune[1]
        tune_filename = tune[2]
        tune_dirpath = tune[3]
        tune_filename_no_ext = tune[4]
        tune_ext = tune[5]
        tune_dirname = tune[6]
        tune_discnumber = tune[7]
        tune_track = tune[8]
        tune_title = tune[9]
        tune_subtitle = tune[10]
        tune_artist = tune[11]
        tune_albumartist = tune[12]
        tune_compilation = tune[13]

        target_filename = None

        # derive new filename

        if tune_discnumber is not None: # add discnumber

            target_filename = pad_text(str(int(tune_discnumber)))

        if tune_track is not None: # add track number

            if target_filename is not None:

                target_filename = target_filename + '-' + pad_text(str(int(tune_track)))

            else:

                target_filename = pad_text(str(int(tune_track)))

        if tune_compilation == '1' and tune_artist is not None: # if compilation add track artist

            if target_filename is not None:

                target_filename = target_filename + ' - ' + tune_artist

            else:

                target_filename = tune_artist

        if tune_title is not None: # add track title

            if target_filename is not None:

                target_filename = target_filename + ' - ' + tune_title

            else:

                target_filename = tune_title


        if target_filename is not None:

            if tune_ext is not None:

                target_filename = target_filename + '.' + tune_ext

            target_filename = target_filename.replace(' )', ')') # remove extraneous spaces between closing bracket if any
            target_filename = sanitize_filename(target_filename) # strip out any bad characters
            new_tune_path = tune_dirpath + r'/' + trim_whitespace(target_filename)



        if target_filename != tune_filename: # only rename if necessary

            print(f'Old filename: {tune_filename}\nNew filename: {target_filename}')


            # attempt to rename the file

            if os.path.exists(new_tune_path):

                print(f'Cannot rename file: {tune_filename} to: {target_filename} because {new_tune_path} exists.')
            else:
                
                try:
                    os.rename(tune_path, new_tune_path)
                    # if the rename was successful update the alib table to reflect the new filenames, keeping old file extension as file type will not have changed.
                    dbcursor.execute('''UPDATE alib
                                           SET __filename = (?),
                                               __filename_no_ext = substr( (?), 1, instr( (?), __ext) - 2),
                                               __path = (?) 
                                         WHERE rowid = (?);''', (tune_filename, tune_filename, tune_filename, new_tune_path, tune_rowid))

                except OSError as e:
                    print(f"{e}\n", file=sys.stderr)



def rename_dirs():
    ''' rename all dirs holding tunes in alib table leveraging the metadata in alib.  Relies on compilation = 0 to detect VA and OST albums
    DO NOT DEPLOY THIS LIGHTLY YET '''


    # get list of all dirs and metadata relevant to naming them.  Important to order by deepest part of tree descending so we work from furthest branch back to root folder to ensure we're not
    # trying to rename orphaned children after a parent has been renamed

        # counts the number of occurences of '/' in __dirpath:
        # SELECT DISTINCT (LENGTH(__dirpath) - LENGTH(REPLACE(__dirpath, '/', '') ) ) / LENGTH('/') AS counter,
        #                 __dirpath
        #   FROM alib
        #  ORDER BY counter DESC,
        #           __dirpath;

    dbcursor.execute('''SELECT DISTINCT ((LENGTH(__dirpath) - LENGTH(REPLACE(__dirpath, '/', '') ) ) / 1) AS counter,
                                        __dirpath,
                                        __dirname,
                                        albumartist,
                                        album,
                                        version,
                                        compilation,
                                        __bitspersample,
                                        __frequency_num,
                                        __frequency,
                                        discnumber
                          FROM alib
                         ORDER BY counter DESC,
                                  __dirpath;''')

    releases = dbcursor.fetchall()

    print("\nRenaming directories based on metadata...")

    for release in releases:

        # grab metadata related to the directory in question, ensure all directory related metadata is read in as literals.  We're not interested in counter, so don't bother retrieving it

        release_dirpath = release[1] # __dirpath
        release_dirname = release[2] # __dirname    
        release_albumartist = release[3]
        release_album = release[4]
        release_version = release[5]
        release_compilation = release[6]
        release_bitspersample = release[7]
        release_frequency_num = release[8]
        release_frequency = release[9]
        release_discnumber = release[10]

        # build up target dirname
        target_dirname = None # __dirname

        # Gather data required to check whether all tracks have the same artist value
        dbcursor.execute('''SELECT DISTINCT artist FROM alib WHERE __dirpath = (?);''', (release_dirpath,))
        artists = dbcursor.fetchall()
        artist_count = len(artists)
        # set boolean value to drive processing workflow based on whether there's only a single artist entry
        single_trackartist = artist_count == 1


        # derive new dirname, deferring to albumartist over compilation flag
        if release_albumartist is None and not single_trackartist:  # then this release is definitely a compilation

            target_dirname = 'VA'  # name for compilations
            compilation_status = '1'

        else:  # if it's not a compilation it must be an albumartist album

            compilation_status = '0'
            # get albumartist, but don't count on there being one
            if release_albumartist is not None:

                target_dirname = release_albumartist

            elif single_trackartist: # check if all tracks have the same artist value - if they do, use that rather than VA in the event there's no albumartist tag present

                # collect artist name from single entry tuple
                target_dirname = artists[0][0]

            else: # theoretically we'll never get here, unless there is no artist or albumartist assigned to the tracks

                print(f'No albumartist associated with album: {release_dirpath} which has not been flagged as compilation. Assigning "ZZZ_MISSING ALBUMARTIST" as albumartist')
                target_dirname = 'ZZZ_MISSING ALBUMARTIST'

        # check whether compilation status is correctly set based on the determintion of compilation status derived above and if not correct it.
        if release_compilation != compilation_status:

            print('Correcting Compilation tag for {release_dirpath} by setting it to {compilation_status} based on album metadata')
            dbcursor.execute('''UPDATE alib SET compilation = (?) WHERE __dirpath = (?);''', (compilation_status, release_dirpath))

        # append album name if it's present
        if release_album is not None:

            target_dirname = target_dirname + ' - ' + release_album

        # check if there's VERSION metadata and append it, if and only if it's not already in the target dirname
        if release_version:

            if release_version.lower() not in target_dirname.lower():

                target_dirname = target_dirname + release_version
                target_dirname = delete_repeated_phrase2(target_dirname, release_version)

        # determine release_resolution string
        release_resolution = ' ['  + release_bitspersample + release_frequency + ']'

        # test for [Mixed Res] albums - if tagminder finds mixed resolution files in a single folder it appends [Mixed Res] to album names so we won't want to add other resolution info on top of that
        if '[mixed res]' not in release_album.lower():  # i.e. it's not a Mixed Re album

            # if > redbook append bitrate and sampling frequency to folder name when not already present
            if (int(release_bitspersample) > 16 or float(release_frequency_num) > 44.1) and release_resolution.lower() not in target_dirname.lower(): # i.e. this is not a redbook album and release resolution is not already present in the target_dirname
                
                release_resolution = ' ['  + release_bitspersample + release_frequency + ']'

        # this is a lazy override, but a simple means of reverting to CDx if necessary whilst ensuring the compilation determination runs regardless
        if 'cd' in release_dirname.lower()[:2]:

            if release_discnumber:

                # if there's a discnumber present, leverage it
                target_dirname = 'cd' + release_discnumber

            else:
                # otherwise use whatever came after cd
                target_dirname = 'cd' + release_dirname.lower()[2:]

        elif 'disc' in release_dirname.lower()[:4]:

            if release_discnumber:
                # if there's a discnumber present, leverage it
                target_dirname = 'cd' + release_discnumber

            else:
                # otherwise use whatever came after disc
                target_dirname = 'cd' + release_dirname.lower()[4:]
            
        # ensure release_resolution and [mixed res] aren't in target path more than once e.g. due to tagger logic or workflow limitations.
        # print(f'unfiltered buildup: {target_dirname}')

        target_dirname = delete_repeated_phrase2(target_dirname, release_version)
        # print(f'eliminated release_version: {target_dirname}')

        target_dirname = delete_repeated_phrase2(target_dirname, release_resolution)
        # print(f'eliminated release_resolution: {target_dirname}')
        # input()
        target_dirname = delete_repeated_phrase2(target_dirname, '[Mixed Res]')
        # print(f'{target_dirname}')
        # input()
        target_dirname = trim_whitespace(sanitize_filename(target_dirname)) # strip illegal chars from target_dirname using same logic as applies to filenames because __dirname cannot ontains '/'

        # __path # derive via sql as it's 1:many tracks  = __dirpath + __filename, tus SQL update would read replace(__path, __dirpath, (?)) where __dirpath = (?); (target_dirpath, release_dirpath)
        # __dirpath # 1:1 replacement
        # __dirname # target_dirname
        # __parent_dir # doesn't change

        if release_dirname != target_dirname: # run rename operation only if the derived dirname differs from the current dirname

            # derive new target_dirpath
            target_dirpath = sanitize_dirname(release_dirpath.rstrip(release_dirname) + target_dirname) # Now derive target_dirpath, which comprises the old __dirpath stripped of the old __dirname and replaced by target_dirname
            # attempt to rename the directory

            if os.path.exists(target_dirpath):

                print(f'Cannot rename directory: {release_dirpath} to: {target_dirpath} because {target_dirpath} already exists.')
            else:

                try:
                    print(f'Renaming directory: {release_dirpath}\nto directory......: {target_dirname}')
                    os.rename(release_dirpath, target_dirpath)

                    # if the rename was successful update the alib table to reflect the new __dirname, __dirpath & __path
                    try:

                        dbcursor.execute('''UPDATE alib
                                               SET __path = replace(__path, __dirpath, (?) ),
                                                   __dirpath = (?),
                                                   __dirname = (?) 
                                             WHERE __dirpath = (?);''', (target_dirpath, target_dirpath, target_dirname, release_dirpath))
                    except sqlite3.Error as er:

                        print(er.sqlite_errorcode)  # Prints 275
                        print(er.sqlite_errorname)  # Prints SQLITE_CONSTRAINT_CHECK

                except OSError as e:
                    print(f"{e}\n", file=sys.stderr)



def show_stats_and_log_changes():

    ''' count number of changes that were written across the popultion of changes '''
    metadata_changes = tally_mods()
    ''' count number of records changed '''
    records_changed = changed_records()
    
    ''' sum the number of __dirpaths changed '''
    dir_count = affected_dircount()


    messagelen = len(f"{metadata_changes} updates have been processed against {records_changed} records, affecting {dir_count} albums")
    print(f"\n")
    print('─' * messagelen)
    print(f"{metadata_changes} updates have been processed against {records_changed} records, affecting {dir_count} albums")
    print('─' * messagelen)

def export_changes():
    ''' get list of all affected __dirpaths '''
    changed_dirpaths = affected_dirpaths()

    ''' write out affected __dirpaths to enable updating of time signature or further processing outside of this script '''
    if changed_dirpaths:

        changed_dirpaths.sort()
        dbcursor.execute('CREATE TABLE IF NOT EXISTS dirs_to_process (__dirpath BLOB PRIMARY KEY);')

        for dirpath in changed_dirpaths:

            dbcursor.execute(f"REPLACE INTO dirs_to_process (__dirpath) VALUES (?)", dirpath)

        conn.commit()

        data = dbcursor.execute("SELECT * FROM dirs_to_process")
        dirlist = '/tmp/dirs2process'
        with open(dirlist, 'w', newline='') as filehandle:
            writer = csv.writer(filehandle, delimiter = '|', quoting=csv.QUOTE_NONE, escapechar='\\')
            writer.writerows(data)

        ''' write changed records to changed_tags table '''
        ''' Create an export database and write out alib containing changed records with sqlmodded set to NULL for writing back to underlying file tags '''
        dbcursor.execute("CREATE INDEX IF NOT EXISTS ix_filepaths ON alib(__path)")

        #############################################temporarily blocked code###############################
        export_db = '/tmp/amg/export.db'
        # print(f"\nGenerating changed_tags table: {export_db}")
        dbcursor.execute(f"ATTACH DATABASE '{export_db}' AS alib2")
        dbcursor.execute("DROP TABLE IF EXISTS  alib2.alib")
        dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib AS SELECT * FROM alib WHERE sqlmodded IS NOT NULL ORDER BY __path")
        dbcursor.execute("UPDATE alib2.alib SET sqlmodded = NULL;")
        dbcursor.execute("DROP TABLE IF EXISTS  alib2.alib_rollback")
        dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib_rollback AS SELECT * FROM alib_rollback ORDER BY __path")
        dbcursor.execute("DROP TABLE IF EXISTS alib_rollback")
        ##############################################temporarily blocked code###############################
        conn.commit()
        dbcursor.execute("VACUUM;")        
        ##############################################temporarily blocked code###############################        
        print(f"Affected folders have been written out to text file: {dirlist}")
        print(f"\nChanged tags have been written to a database: {export_db} in table alib.\nalib contains only changed records with sqlmodded set to NULL for writing back to underlying file tags.")
        print(f"You can now directly export from this database to the underlying files using tagfromdb3.py.\n\nIf you need to rollback changes you can reinstate tags from table 'alib_rollback' in {export_db}\n")
        percent_affected = (records_changed / library_size())*100
        print(f"{'%.2f' % percent_affected}% of records in table (corresponding to tracks in library) have been modified.")
        ##############################################temporarily blocked code###############################
    else:
        print("- No changes were processed\n")



def update_tags():
    ''' function call to run mass tagging updates.  It is preferable to run update_tags prior to killing bad_tags so that data can be moved to good tags where present in non-standard tags such as 'recording location' & unsyncedlyrics
    Consider whether it'd be better to break this lot into discrete functions '''

    ''' set up initialisation counter '''
    start_tally = tally_mods()

    ''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


    # here you add whatever update and enrichment queries you want to run against the table 
    # comment out anything you don't want run

    # # transfer any unsynced lyrics to LYRICS tag
    # unsyncedlyrics_to_lyrics()

    # # merge [recording location] with RECORDINGLOCATION
    # merge_recording_locations()

    # # merge release tag to VERSION tag
    # release_to_version()

    # get rid of tags we don't want to store
    kill_badtags()

    # remove CR & LF from text tags (excluding lyrics & review tags)
    trim_and_remove_crlf()

    # # get rid of non-standard apostrophes
    # set_apostrophe()

    # # strip Feat in its various forms from track title and append to ARTIST tag
    # title_feat_to_artist()

    # # remove all instances of artist entries that contain feat or with and replace with a delimited string incorporating all performers
    # feat_artist_to_artist()

    # # disambiguate entries in artist, albumartist & composer tags leveraging the outputs of string-grouper
    disambiguate_contributors() # this only does something if there are records in the disambiguation table that have not yet been processed

    # generate sg_contributors, which is the table containing all distinct artist, performer, albumartist and composer names in your library
    # this is to be processed by string-grouper to generate similarities.csv for investigation and resolution by human endeavour.  The outputs 
    # of that endeavour then serve to append new records to the disambiguation table which is then processed via disambiguate_contributors() if enabled by user
    generate_string_grouper_input()

    # # set all empty tags ('') to NULL
    # nullify_empty_tags()

    # # set all PERFORMER tags to NULL when they match or are already present in ARTIST tag
    # nullify_performers_matching_artists()

    # # iterate through titles moving text between matching (live) or [live] to SUBTITLE tag and set LIVE=1 if not already tagged accordingly
    # strip_live_from_titles()

    # # moves known keywords in brackets to subtitle
    # title_keywords_to_subtitle()

    # # last resort moving anything left in square brackets to subtitle.  Cannot do the same with round brackets because chances are you'll be moving part of a song title
    # square_brackets_to_subtitle()

    # # strips '(live)'' from end of album name and sets LIVE=1 where this is not already the case
    # strip_live_from_album_name()

    # # ensure any tracks with 'Live' appearing in subtitle have set LIVE=1
    # live_in_subtitle_means_live()

    # # ensure any tracks with LIVE=1 also have 'Live' appearing in subtitle 
    # live_means_live_in_subtitle()

    # # set DISCNUMBER = NULL where DISCNUMBER = '1' for all tracks and folder is not part of a boxset
    # kill_singular_discnumber()

    # # set compilation = '1' when __dirname starts with 'VA -' and '0' otherwise.  Note, it does not look for and correct incorrectly flagged compilations and visa versa - consider enhancing
    # set_compilation_flag()

    # # set albumartist to NULL for all compilation albums where they are not NULL
    # nullify_albumartist_in_va()

    # # applies firstlettercaps to each entry in releasetype if not already firstlettercaps
    # capitalise_releasetype()

    # # determines releasetype for each album if not already populated
    # add_releasetype()

    # add a uuid4 tag to every record that does not have one
    # add_tagminder_uuid()

    # # # Sorts delimited text strings in tags, dedupes them and compares the result against the original tag contents.  When there's a mismatch the newly deduped, sorted string is written back to the underlying table
    # dedupe_tags()

    # # runs a query that detects duplicated albums based on the sorted md5sum of the audio stream embedded in FLAC files and writes out a few tables to ease identification and (manual) deletion tasks
    # find_duplicate_flac_albums()

    # # # remove genre and style tags that don't appear in the vetted list, merge genres and styles and sort and deduplicate both
    # cleanse_genres_and_styles()

    # # add genres where an album has no genres and a single albumartist.  Genres added will be amalgamation of the same artist's other work in your library.
    # add_genres_and_styles()

    # # standardise genres, styles, moods, themes: merges tag entries for every distinct __dirpath, dedupes and sorts them then writes them back to the __dirpath in question
    # # this meaans all tracks in __dirpath will have the same album, genre style, mood and theme tags
    # # do not run genre and style code if you use per track genres and styles
    # standardise_album_tags('album')
    # standardise_album_tags('genre')
    # standardise_album_tags('style')
    # standardise_album_tags('mood')
    # standardise_album_tags('theme')

    # # if mb_disambiguated table exists adds musicbrainz identifiers to artists, albumartists & composers (we're adding musicbrainz_composerid of our own volition for future app use)
    # # not necessary to call this anymore becaise it gets called by add_multiartist_mb_entities()
    # # ideally it should be turned into a localised function of add_multiartist_mb_entities()
    # #add_mb_entities()

    # # add mbid's for multi-entry artists
    # add_multiartist_mb_entities()

    # # set capitalistion for track titles
    # set_title_caps()


    # # set capitalistion for album names
    # set_album_caps()


    # ######################
    # # these should probably go and never be run, because the reality is all entries for contrbutors should in fact be processed against the musicbrainz mbid table once the musicbrainz names in that table have been updated with changes 
    # # from disambiguation table, which in effect means those changes reflect how the artist name appears on allmusic.com

    # # set capitalistion for composers
    # set_composer_caps()

    # # set capitalistion for artists
    # set_artist_caps()

    # # set capitalistion for albumartists
    # set_albumartist_caps()
    # ######################


    # add resolution info to VERSION tag for all albums where > 16/44.1 and/or mixed resolution albums
    # tag_non_redbook()

    # # merge ALBUM and VERSION tags to stop Logiechmediaserver, Navidrome etc. conflating multiple releases of an album into a single album.  It preserves VERSION tag to make it easy to remove VERSION from ALBUM tag in future
    # # must be run AFTER tag_non_redbook() as it doesn't append non redbook metadata
    # merge_album_version()


    # # remove leading 0's from track tags
    # unpad_tracks()

    # # remove leading 0's from discnumber tags
    # unpad_discnumbers()

    # # rename files leveraging processed metadata in the database
    # rename_tunes()

    # # rename folders containing albums leveraging processed metadata in the database
    # rename_dirs()


    ''' return case sensitivity for LIKE to SQLite default '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')



    ''' add any other update queries you want to run above this line '''

    conn.commit()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    return(tally_mods() - start_tally)




# # Define argparse configuration
# parser = argparse.ArgumentParser(description="Run specific functions for metadata processing.")
# parser.add_argument('--all', action='store_true', help="Run all functions.")
# parser.add_argument('--exclude', nargs='+', help="Exclude specified functions from execution.")
# parser.add_argument('--include', nargs='+', help="Run only specified functions.")
# args = parser.parse_args()

# # Define which functions to run based on argparse arguments
# functions_to_run = []

# if args.all:
#     functions_to_run = [func for func in globals() if callable(globals()[func]) and func.startswith('') and func != 'parser']
# elif args.exclude:
#     functions_to_run = [func for func in globals() if callable(globals()[func]) and func.startswith('') and func != 'parser' and func not in args.exclude]
# elif args.include:
#     functions_to_run = [func for func in globals() if callable(globals()[func]) and func.startswith('') and func != 'parser' and func in args.include]

# # Execute the selected functions
# for func_name in functions_to_run:
#     func = globals()[func_name]
#     func()



if __name__ == '__main__':

    cls()
    if len(sys.argv) < 2 or not exists(sys.argv[1]):
        print(f"""Usage: python {sys.argv[0]} </path/to/database> to process""")
        sys.exit()
    dbfile = sys.argv[1]
    working_dir = dirname(dbfile)
    

    conn = sqlite3.connect(dbfile)
    dbcursor = conn.cursor()
    start_time = time.time()
    establish_environment()
    update_tags()
    show_stats_and_log_changes()
    # show_table_differences()

    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"It took {'%.2f' % elapsed_time } seconds or {'%.2f' % (elapsed_time / 60) } minutes to update library")



    # print(f"Compacting database {dbfile}")
    # dbcursor.execute("VACUUM")
    dbcursor.close()
    conn.close()
    print(f"\n{'─' * 5}\nDone!\n")

''' todo: ref https://github.com/audiomuze/tags2sqlite
add:
- write out test files: all __dirpath's missing genres, composers, year/date, mbalbumartistid
- fill in blanks on albumartist, genre, style, mood, theme where some files relating to an album have a different or no value for aformentioned fields.
- add MBID table to mix and test for its existence - if it exists use that to add MBIDs rather than what's already in alib - this will produce a more comprehensive result
- are we filling in blanks on composers anywhere in here?


 '''
