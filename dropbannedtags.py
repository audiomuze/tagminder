import csv
import os
from os.path import exists, dirname
import re
import sqlite3
import sys


''' function to clear screen '''
cls = lambda: os.system('clear')

def firstlettercaps(s):
    ''' returns first letter caps for each word but respects apostrophes '''
    return re.sub(r"[A-Za-z]+('[A-Za-z]+)?", lambda mo: mo.group(0)[0].upper() + mo.group(0)[1:].lower(), s)


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



def dedupe_and_sort(list_item, delimiter):
    ''' get a list item that contains a delimited string, dedupe and sort it and pass it back '''
    distinct_items = set(x.strip() for x in list_item.split(delimiter))
    return (delimiter.join(sorted(distinct_items)))



def tally_mods():
    ''' start and stop counter that returns how many changes have been triggered at the point of call - will be >= 0 '''
    dbcursor.execute('SELECT sum(CAST (sqlmodded AS INTEGER) ) FROM alib WHERE sqlmodded IS NOT NULL;')
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
    # ''' sum number of distinct __dirpaths with changed content '''    
    # dbcursor.execute('SELECT count(DISTINCT __dirpath) FROM alib where sqlmodded IS NOT NULL;')
    # matches = dbcursor.fetchone()
    # return(matches[0])
    return(len(affected_dirpaths()))



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
    "_releasecomment",
    "acousticbrainz_mood",
    "acoustid_fingerprint",
    "acoustid_id",
    "album",
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
    "musicbrainz_discid",
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

    dbcursor.execute('drop table if exists permitted_tags;')
    dbcursor.execute('create table permitted_tags (tagname text);')

    for tag in good_tags:

        dbcursor.execute(f"INSERT INTO permitted_tags ('tagname') VALUES ('{tag}')")

    ''' ensure trigger is in place to record incremental changes until such time as tracks are written back '''
    dbcursor.execute("CREATE TRIGGER IF NOT EXISTS sqlmods AFTER UPDATE ON alib FOR EACH ROW WHEN old.sqlmodded IS NULL BEGIN UPDATE alib SET sqlmodded = iif(sqlmodded IS NULL, '1', (CAST (sqlmodded AS INTEGER) + 1) )  WHERE rowid = NEW.rowid; END;")

    ''' alib_rollback is a master copy of alib table untainted by any changes made by this script.  if a rollback table already exists we are applying further changes or imports, so leave it intact '''
    dbcursor.execute("CREATE TABLE IF NOT EXISTS alib_rollback AS SELECT * FROM alib order by __path;")

    conn.commit()



# def show_table_differences():

#   ''' pick up the columns present in the table '''
#   columns = get_columns('alib')

#   if table_exists('alib_rollback'):
#       for column in columns:

#           field_to_compare = column[0]
#           # print(f"Changes in {column[0]}:")
#           # query = f"select alib.*, alib_rollback.* from alib inner join alib_rollback on alib.__path = alib_rollback.__path where 'alib.{column[0]}' != 'alib_rollback.{column[0]}'"
#           dbcursor.execute(f"select alib.__path, 'alib.{field_to_compare}', 'alib_rollback.{field_to_compare}' from alib inner join alib_rollback on alib.__path = alib_rollback.__path where ('alib.{field_to_compare}' != 'alib_rollback.{field_to_compare}');")
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

    

# def get_badtags():
#   ''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
#   dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
#   badtags = dbcursor.fetchall()
#   if len(badtags) > 0:
#       badtags.sort()
#   return(badtags)



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
                dbcursor.execute(f"create index if not exists spurious on alib({tag}) WHERE {tag} IS NOT NULL")
                dbcursor.execute(f"select count({tag}) from alib")
                tally = dbcursor.fetchone()[0]
                print(f"- {tag}, {tally}")
                dbcursor.execute(f"UPDATE alib set {tag} = NULL WHERE {tag} IS NOT NULL")
                dbcursor.execute(f"drop index if exists spurious")
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
            dbcursor.execute(f"CREATE INDEX IF NOT EXISTS nullify ON alib ({field_to_check}) WHERE TRIM({field_to_check}) = '';")
            dbcursor.execute(f"UPDATE alib SET {field_to_check} = NULL WHERE TRIM({field_to_check}) = '';")
            dbcursor.execute(f"DROP INDEX IF EXISTS nullify;")
    print(f"|\n{tally_mods() - opening_tally} tags were modified")



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
        dbcursor.execute(f"CREATE INDEX IF NOT EXISTS crlf ON alib (replace(replace({text_tag}, char(10), ''), char(13), '') ) WHERE {text_tag} IS NOT NULL;")
        dbcursor.execute(f"CREATE INDEX IF NOT EXISTS crlf1 ON alib (trim({text_tag})) WHERE {text_tag} IS NOT NULL;")

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

        dbcursor.execute(f"drop index if exists crlf")
        dbcursor.execute(f"drop index if exists crlf1")

    print(f"|\n{tally_mods() - opening_tally} tags were modified")



def square_brackets_to_subtitle():
    '''  select all records with '[' in title, split-off text everything folowing '[' and write it out to subtitle '''
    opening_tally = tally_mods()
    print(f"\nUpdating titles to remove any text enclosed in square brackets from TITLE and appending same to SUBTITLE tag")
    # dbcursor.execute("UPDATE alib SET title = IIF(TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) = '', title, TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1))), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%';")
    dbcursor.execute("UPDATE alib SET title = TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%' AND TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) != '';")
    print(f"|\n{tally_mods() - opening_tally} tags were modified")



# def title_feat_to_artist():
#     ''' Move all instances of Feat and With to ARTIST tag '''

#     opening_tally = tally_mods()
#     feat_instances = [
#     '(Feat. %',
#     '[Feat. ',
#     '(feat. ',
#     '[feat. ',
#     '(Feat ',
#     '[Feat ',
#     '(feat ',
#     '[feat ']

#     print('\n')
#     dbcursor.execute(f"create index if not exists titles_artists on alib(title, artist)")

#     for feat_instance in feat_instances:

#         print(f"Stripping {feat_instance} from track TITLE and appending performers to ARTIST tag...")
#         # dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), artist = artist || '\\\\' || REPLACE(replace(substr(title, instr(title, ?) ), ?, ''), ')', '')  WHERE title LIKE ? AND (trim(substr(title, 1, instr(title, ?) - 1) ) != '');",  (feat_instance, feat_instance, feat_instance, '%'+feat_instance+'%', feat_instance))
#         dbcursor.execute('''UPDATE alib
#                                SET title = trim(substr(title, 1, instr(title, ?) - 1) ),
#                                    artist = artist || '\\\\' || REPLACE(replace(substr(title, instr(title, ?) ), ?, ''), ')', '') 
#                              WHERE title LIKE ? AND 
#                                    (trim(substr(title, 1, instr(title, ?) - 1) ) != '');''', (feat_instance, feat_instance, feat_instance, '%'+feat_instance+'%', feat_instance))



#     dbcursor.execute(f"drop index if exists titles_artists")
#     print(f"|\n{tally_mods() - opening_tally} tags were modified")  

def title_feat_to_artist():
    ''' Move all instances of Feat and With in track TITLE to ARTIST tag '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS titlea on alib(title)''')

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
                               title LIKE '% With: %';''')


    records_to_process = dbcursor.fetchall()
    
    feats = ['Feat ', 'Feat:', 'Feat.', 'Feat-', 'Feat -', 'Featuring ', 'Featuring:', 'Featuring.', 'Featuring-',  'Featuring -', 'With:' ]

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
                new_artist = row_artist + '\\\\' + sub

                print(f"{format(table_record,'07d')}, String: '{row_title}' Base: '{base}' Sub: '{sub}' New artist: '{new_artist}'")
                dbcursor.execute('''UPDATE alib set title = (?), artist = (?) WHERE rowid = (?);''', (base, new_artist, table_record))

            except ValueError:

                ''' if no match exit the loop and continue to next match ... this iimplementation is just to stop code crashing when feat is not present in row_title '''
                exit

        print('\n')        
                    

    dbcursor.execute('''drop index if exists artists''')
    print(f"|\n{tally_mods() - opening_tally} tags were modified")  



def feat_artist_to_artist():
    ''' Move all instances of Feat and With to ARTIST tag '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS artists on alib(artist)''')

    print("\nStripping feat. ARTIST tag and incorporating into delimited ARTIST string...\n")

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
    
    feats = ['Feat ', 'Feat:', 'Feat.', 'Feat-', 'Feat -', 'Featuring ', 'Featuring:', 'Featuring.', 'Featuring-',  'Featuring -', 'With:' ]

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
                    

    dbcursor.execute('''drop index if exists artists''')
    print(f"|\n{tally_mods() - opening_tally} tags were modified")  



def merge_recording_locations():
    # ''' append "recording location" to recordinglocation if recordinglocation is empty sql2 is likely redundant because of killbadtags'''
    column_name = "recording location"
    tag_in_table(column_name, 'alib')

    if tag_in_table(column_name, 'alib'):
        opening_tally = tally_mods()
        print(f"\nIncorporating recording location into recordinglocation")

        sql1 = '''
        UPDATE alib SET recordinglocation = alib."recording location", "recording location" = NULL WHERE alib.recordinglocation IS NULL AND alib."recording location" IS NOT NULL;
        '''
        sql2 = '''
        UPDATE alib SET recordinglocation = recordinglocation || "\\" || alib."recording location", "recording location" = NULL WHERE alib.recordinglocation IS NOT NULL AND alib."recording location" IS NOT NULL;
        '''
        dbcursor.execute(sql1)
        dbcursor.execute(sql2)      
        print(f"|\n{tally_mods() - opening_tally} tags were modified")



def release_to_version():
    # ''' append "release" to version if version is empty. sql2 is likely redundant because of killbadtags'''
    column_name = "release"
    tag_in_table(column_name, 'alib')

    if tag_in_table(column_name, 'alib'):
        opening_tally = tally_mods()
        print(f"\nIncorporating 'release' into 'version' and removing 'release' metadata")

        sql1 = '''
        UPDATE alib SET version = alib.release, release = NULL WHERE alib.version IS NULL and alib.release IS NOT NULL;
        '''
        sql2 = '''
        UPDATE alib SET version = version || " " || alib.release, release = NULL WHERE alib.version IS NOT NULL AND alib.release IS NOT NULL AND NOT INSTR(alib.version, alib.release);
        '''
        dbcursor.execute(sql1)
        dbcursor.execute(sql2)      
        print(f"|\n{tally_mods() - opening_tally} tags were modified")



def unsyncedlyrics_to_lyrics():
    ''' append "unsyncedlyrics" to lyrics if lyrics is empty '''
    if tag_in_table('unsyncedlyrics', 'alib'):
        opening_tally = tally_mods()
        print(f"\nCopying unsyncedlyrics to lyrics where lyrics tag is empty")
        dbcursor.execute("UPDATE alib SET lyrics = unsyncedlyrics WHERE lyrics IS NULL AND unsyncedlyrics IS NOT NULL;")
        print(f"|\n{tally_mods() - opening_tally} tags were modified")



def nullify_performers_matching_artists():
    ''' remove performer tags where they match or appear in artist tag '''
    opening_tally = tally_mods()
    print(f"\nRemoving performer names where they match or appear in artist tag")
    dbcursor.execute('UPDATE alib SET performer = NULL WHERE ( (lower(performer) = lower(artist) ) OR INSTR(artist, performer) > 0);')
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


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
    '(Album Version)%']

    ''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')

    print('\n')
    dbcursor.execute(f"create index if not exists titles_subtitles on alib(title, subtitle)")
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
    dbcursor.execute(f"drop index if exists titles_subtitles")
    print(f"|\n{tally_mods() - opening_tally} tags were modified")



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
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


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

    print(f"|\n{tally_mods() - opening_tally} tags were modified")


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
#     dbcursor.execute(f"create index if not exists titles_subtitles on alib(title, subtitle)")
#     opening_tally = tally_mods()
#     for live_instance in live_instances:

#         print(f"Stripping {live_instance} from track titles...")
#         dbcursor.execute(f"UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), subtitle = substr(title, instr(title, ?)) WHERE (title LIKE ? AND subtitle IS NULL);", (live_instance, live_instance, '%'+live_instance+'%'))
#         dbcursor.execute(f"UPDATE alib SET subtitle = subtitle || '\\\\' || trim(substr(title, instr(title, ?))), title = trim(substr(title, 1, instr(title, ?) - 1) ) WHERE (title LIKE ? AND subtitle IS NOT NULL);", (live_instance, live_instance, '%'+live_instance+'%'))

#     dbcursor.execute(f"drop index if exists titles_subtitles")
#     print(f"|\n{tally_mods() - opening_tally} tags were modified")



def strip_live_from_titles():

    ''' iterate each record and remove live from track title, mark the track as live and append [Live] to subtitle '''

    opening_tally = tally_mods()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS titles on alib(title)''')


    records_to_process = [''] # initiate a list of one item to satisfy a while list is not empty
    iterator = 0
    exhausted_queries = 0

    print(f"\nStripping ([Live]) from track titles...")

    while records_to_process and not exhausted_queries: # PEP 8 recommended method for testing whether or not a list is empty.  exhausted_queries is a trigger that's activated when it's time to exit the while loop

        ''' Select all records with (live and/or [live in title '''
        dbcursor.execute('''SELECT title,
                                   subtitle,
                                   live,
                                   rowid
                              FROM alib
                             WHERE title LIKE '%(live%' OR 
                                   title LIKE '%[live%';''')

        records_to_process = dbcursor.fetchall()
        record_count = len(records_to_process)
        mismatched_brackets = 0

        ''' now process each in sequece '''
        for record in records_to_process:

            ''' loop through records to test whether they're all mismatched brackets'''
            row_title = record[0]

            ''' if the entry containa matching opening and closing brackets process it, otherwise skip over itas it isn't a record that shoul be processed '''
            if ('[' in row_title and ']' in row_title) or ('(' in row_title and ')' in row_title):

                ''' we've not exhausted query results, so continue processing '''
                row_subtitle = record[1]  # record's subtitle field
                row_islive = record[2] # record's live field
                row_to_process = record[3] # rowid of record being processed

                ''' test for matching bracket pairs and set up the bracket variables '''
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
                if (row_islive is None or row_islive == '0') and islive is not None:
                    dbcursor.execute('''UPDATE alib set live = (?) WHERE rowid = (?);''', (islive, row_to_process))

            else:

                ''' seeing as the record was a wash increment the number of mismatches.  when this number equals the record_count of records matching the select statement it's time to trigger exhausted_queries to break the while loop '''
                mismatched_brackets += 1


        ''' test whether we've hit that point where the query keeps returning the same records with mismatching brackets, in which case it's time to exit the while loop '''
        if mismatched_brackets == record_count:

            exhausted_queries = 1

    dbcursor.execute(f"drop index if exists titles")
    print(f"|\n{tally_mods() - opening_tally} tags were modified")



def tags_to_dedupe():
    ''' list all known text tags you might want to process against -- this needs to become a function call so there's a common definition throughout the app '''

    return(["_releasecomment", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", 
        "engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", 
        "musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "performer", "personnel", "producer", "recordinglocation", "releasetype", 
        "remixer", "style", "subtitle", "theme", "upc", "version", "writer"])


def dedupe_fields():
    ''' remove duplicate tag entries in text fields present in alib that may contain duplicate entries '''

    ''' get list of text tags actually present in the alib table, based on what's been imported into alib '''
    text_tags = texttags_in_alib(tags_to_dedupe())
    print(f"\nDeduping tags:")
    opening_tally = tally_mods()

    for text_tag in text_tags:
        
        query = (f"CREATE INDEX IF NOT EXISTS dedupe_tag ON alib({text_tag}) WHERE {text_tag} IS NOT NULL;")

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
    print(f"|\n{tally_mods() - opening_tally} tags were deduped")


def kill_singular_discnumber():
    ''' get rid of discnumber when all tracks in __dirpath have discnumber = 1.  I'm doing this the lazy way because I've not spent enough time figuring out the CTE update query in SQL.  This is a temporary workaround to be replaced with a CTE update query '''
    opening_tally = tally_mods()    
    # dbcursor.execute('''WITH GET_SINGLE_DISCS AS ( SELECT __dirpath AS cte_value FROM ( SELECT DISTINCT __dirpath, discnumber FROM alib WHERE discnumber IS NOT NULL AND lower(__dirname) NOT LIKE '%cd%' AND lower(__dirname) NOT LIKE '%cd%') GROUP BY __dirpath HAVING count( * ) = 1 ORDER BY __dirpath ) SELECT cte_value FROM GET_SINGLE_DISCS;''')

    dbcursor.execute('''CREATE INDEX IF NOT EXISTS dirpaths_discnumbers ON alib (
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
                                                        WHERE discnumber IS NOT NULL AND 
                                                              (__dirpath NOT LIKE '%cd%' AND 
                                                               __dirpath NOT LIKE '%/Michael Jackson - HIStory Past, Present and Future, Book I%' AND 
                                                               __dirpath NOT LIKE '%Depeche Mode - Singles Box%' AND 
                                                               __dirpath NOT LIKE '%Disc 1%' AND 
                                                               __dirpath NOT LIKE '%/Lambchop – Tour Box/%' AND 
                                                               __dirpath NOT LIKE '%/Pearl Jam Evolution - Gold Box Set/%' AND 
                                                               __dirpath NOT LIKE '%4CD Box/%' AND 
                                                               __dirpath NOT LIKE '%Boxset/CD%' AND 
                                                               __dirpath NOT LIKE '%/CD%' AND 
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
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


def strip_live_from_album_name():
    ''' Strip all occurences of '(live)' from end of album name '''
    print(f"\nStripping all occurences of '(live)' from end of album name, ensuring album is marked live and updating subtitle where required")

    opening_tally = tally_mods()
    dbcursor.execute('create index if not exists albums on alib(album)')

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

    print(f"|\n{tally_mods() - opening_tally} tags were modified")
    


def merge_album_version():
    ''' merge album name and version fields into album name '''
    print(f"\nMerging album name and version fields into album name where version tag does not already appear in album name")
    opening_tally = tally_mods()
    dbcursor.execute(f"UPDATE alib SET album = album || ' ' || version WHERE version IS NOT NULL AND NOT INSTR(album, version);")
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


def split_album_version():
    ''' split album name and version fields, reverting album tag to album name '''
    print(f"\nRemoving VERSION tag from ABUM tag")
    opening_tally = tally_mods()
    dbcursor.execute('''UPDATE alib
                           SET album = substring(album, 1, INSTR(album, version) - 2) 
                         WHERE version IS NOT NULL AND 
                               INSTR(album, version);''')
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


def set_compilation_flag():
    ''' set compilation = '1' when __dirname starts with 'VA -' and '0' otherwise '''
    print(f"\nSetting COMPILATION = '1' / '0' depending on whether __dirname starts with 'VA -'")
    opening_tally = tally_mods()
    dbcursor.execute('''
                        UPDATE alib
                           SET compilation = '1'
                         WHERE (compilation IS NULL OR 
                                compilation != '1' AND 
                                substring(__dirname, 1, 4) = 'VA -' AND 
                                albumartist IS NULL);''')

    dbcursor.execute('''
                        UPDATE alib
                           SET compilation = '0'
                         WHERE (compilation IS NULL OR 
                                compilation != '0' AND 
                                substring(__dirname, 1, 4) != 'VA -' AND 
                                albumartist IS NOT NULL);''')

    print(f"|\n{tally_mods() - opening_tally} tags were modified")



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

    print(f"|\n{tally_mods() - opening_tally} tags were modified")



def capitalise_releasetype():
    print(f"\nSetting 'First Letter Caps' for all instances of releasetype")
    opening_tally = tally_mods()
    dbcursor.execute('''SELECT DISTINCT releasetype FROM alib WHERE releasetype IS NOT NULL;''')
    releasetypes = dbcursor.fetchall()
    for release in releasetypes:
        print(release[0])
        flc = firstlettercaps(release[0])
        print(release[0], flc)
        # SQLite WHERE clause is case sensistive so this should not repeatedly upddate records every time it is run
        dbcursor.execute('''UPDATE alib SET releasetype = (?) WHERE releasetype = (?) AND releasetype != (?);''', (flc, release[0], flc))
    print(f"|\n{tally_mods() - opening_tally} tags were modified")


def establish_contributors():
    ''' build list of unique contibutors by gathering all mbid's found in alib - checking against artist and albumartist fields '''

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

    dbcursor.execute('''CREATE INDEX IF NOT EXISTS role_albumartists on alib(albumartist);''')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS role_artists on alib(artist);''')
    dbcursor.execute('''CREATE INDEX IF NOT EXISTS role_composers on alib(composer);''')


    # create contributors table from all albumartists and artists where the abumartist and artist name only appear once alongside an mbid
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

    # create namesakes table from all albumartists where the abumartist name appears > 1 alongside an mbid
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
                                           

    dbcursor.execute('''CREATE TABLE role_composer AS SELECT DISTINCT composer
                                                        FROM alib
                                                       WHERE composer IS NOT NULL AND 
                                                             composer NOT LIKE '%\\%' AND 
                                                             composer != ''
                                                       ORDER BY composer;'''
                    )


    # now bring them together in a single table
    dbcursor.execute('''CREATE TABLE contributor_with_mbid AS SELECT contributor,
                                                                     mbid
                                                                FROM role_albumartist
                        UNION
                        SELECT contributor,
                               mbid
                          FROM role_artist
                         ORDER BY contributor;'''
                    )


def add_musicbrainz_identifiers():
    ''' adds musicbrainz identifiers to artists, albumartists & composers (we're adding musicbrainz_composerid of our own volition for future app use) '''

    print(f"\nAdding musicbrainz identifiers to artists & albumartists")
    opening_tally = tally_mods()

    # albumartist updates
    dbcursor.execute('''UPDATE alib
                           SET musicbrainz_albumartistid = (
                                   SELECT contributor_with_mbid.mbid
                                     FROM contributor_with_mbid
                                    WHERE contributor_with_mbid.contributor = alib.albumartist
                               )
                         WHERE EXISTS (
                                   SELECT contributor_with_mbid.mbid
                                     FROM contributor_with_mbid
                                    WHERE contributor_with_mbid.contributor = alib.albumartist
                               )
                        AND 
                               alib.musicbrainz_albumartistid IS NULL;'''
                     )       
           
       
    # artist updates
    dbcursor.execute('''UPDATE alib
                           SET musicbrainz_artistid = (
                                   SELECT contributor_with_mbid.mbid
                                     FROM contributor_with_mbid
                                    WHERE contributor_with_mbid.contributor = alib.artist
                               )
                         WHERE EXISTS (
                                   SELECT contributor_with_mbid.mbid
                                     FROM contributor_with_mbid
                                    WHERE contributor_with_mbid.contributor = alib.artist
                               )
                        AND 
                               alib.musicbrainz_artistid IS NULL;'''
                    )

    # # composer updates
    # dbcursor.execute('''UPDATE alib
    #                        SET musicbrainz_composerid = (
    #                                SELECT contributor_with_mbid.mbid
    #                                  FROM contributor_with_mbid
    #                                 WHERE contributor_with_mbid.contributor = alib.artist
    #                            )
    #                      WHERE EXISTS (
    #                                SELECT contributor_with_mbid.mbid
    #                                  FROM contributor_with_mbid
    #                                 WHERE contributor_with_mbid.contributor = alib.artist
    #                            )
    #                     AND 
    #                            alib.musicbrainz_composerid IS NULL;'''
    #                 )

    print(f"|\n{tally_mods() - opening_tally} musicbrainz identifiers added to artists & albumartists")


def find_duplicate_flac_albums():
    ''' this is based on records in the alib table as opposed to file based metadata imported using md5sum.  The code relies on the md5sum embedded in properly encoded FLAC files - it basically takes them, creates a concatenated string
    from the sorted md5sum of al tracks in a folder and compares that against the same for all other folders.  If the strings match you have a 100% match of the audio stream and thus duplicate album, irrespective of tags.
    '''

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




def show_stats_and_log_changes():

    ''' count number of records changed '''
    records_changed = changed_records()
    
    ''' sum the number of __dirpaths changed '''
    dir_count = affected_dircount()


    messagelen = len(f"Updates have been processed against {records_changed} records, affecting {dir_count} albums")
    print(f"\n")
    print('─' * messagelen)
    print(f"Updates have been processed against {records_changed} records, affecting {dir_count} albums")
    print('─' * messagelen)


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
        dirlist = working_dir + '/dirs2process'
        with open(dirlist, 'w', newline='') as filehandle:
            writer = csv.writer(filehandle, delimiter = '|', quoting=csv.QUOTE_NONE)
            writer.writerows(data)

        ''' write changed records to changed_tags table '''
        ''' Create an export database and write out alib containing changed records with sqlmodded set to NULL for writing back to underlying file tags '''
        dbcursor.execute("create index if not exists filepaths on alib(__path)")
        export_db = working_dir + '/export.db'
        # print(f"\nGenerating changed_tags table: {export_db}")
        dbcursor.execute(f"ATTACH DATABASE '{export_db}' AS alib2")
        dbcursor.execute("DROP TABLE IF EXISTS  alib2.alib")
        dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib AS SELECT * FROM alib WHERE sqlmodded IS NOT NULL ORDER BY __path")
        dbcursor.execute("UPDATE alib2.alib SET sqlmodded = NULL;")
        dbcursor.execute("DROP TABLE IF EXISTS  alib2.alib_rollback")
        dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib_rollback AS SELECT * FROM alib_rollback ORDER BY __path")
        dbcursor.execute("DROP TABLE IF EXISTS alib_rollback")
        conn.commit()
        
        print(f"Affected folders have been written out to text file: {dirlist}")
        print(f"\nChanged tags have been written to a database: {export_db} in table alib.\nalib contains only changed records with sqlmodded set to NULL for writing back to underlying file tags.")
        print(f"You can now directly export from this database to the underlying files using tagfromdb3.py.\n\nIf you need to rollback changes you can reinstate tags from table 'alib_rollback' in {export_db}\n")
        percent_affected = (records_changed / library_size())*100
        print(f"{'%.2f' % percent_affected}% of records in table (corresponding to tracks in library) have been modified.")

    else:
        print("- No changes were processed\n")



def update_tags():
    ''' function call to run mass tagging updates.  It is preferable to run update_tags prior to killing bad_tags so that data can be moved to good tags where present in non-standard tags such as 'recording location' & unsyncedlyrics
    Consider whether it'd be better to break this lot into discrete functions '''

    ''' set up initialisation counter '''
    start_tally = tally_mods()

    ''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


    ''' here you add whatever update and enrichment queries you want to run against the table '''

    # transfer any unsynced lyrics to LYRICS tag
    unsyncedlyrics_to_lyrics()

    # merge [recording location] with RECORDINGLOCATION
    merge_recording_locations()

    # merge release tag to VERSION tag
    release_to_version()

    # get rid of tags we don't want to store
    kill_badtags()

    # set all empty tags ('') to NULL
    nullify_empty_tags()

    # remove CR & LF from text tags (excluding lyrics & review tags)
    trim_and_remove_crlf()

    # strip Feat in its various forms from track title and append to ARTIST tag
    title_feat_to_artist()

    # remove all instances of artist entries that fontain feat or with and replace with a delimited string incorporating all performers
    feat_artist_to_artist()

    # set all PERFORMER tags to NULL when they match or are already present in ARTIST tag
    nullify_performers_matching_artists()

    # mark live performances as LIVE=1 if not already tagged accordingly NEEDS REVISITING - might be superceded by strip_live_from_titles().
    # tag_live_tracks()

    # iterate through titles moving text between matching (live) or [live] to SUBTITLE tag and set LIVE=1 if not already tagged accordingly
    strip_live_from_titles()

    # moves known keywords in brackets to subtitle
    title_keywords_to_subtitle()

    # last resort moving anything left in square brackets to subtitle.  Cannot do the same with round brackets because chances are you'll be moving part of a song title
    square_brackets_to_subtitle()

    # ensure any tracks with 'Live' appearing in subtitle have set LIVE=1
    live_in_subtitle_means_live()

    # ensure any tracks with LIVE=1 also have 'Live' appearing in subtitle 
    live_means_live_in_subtitle()

    # set DISCNUMBER = NULL where DISCNUMBER = '1' for all tracks and folder is not part of a boxset
    kill_singular_discnumber()

    # merge ALBUM and VERSION tags to stop Logiechmediaserver, Navidrome etc. conflating multiple releases of an album into a single album.  It preserves VERSION tag to make it easy to remove VERSION from ALBUM tag in future
    merge_album_version()

    # set compilation = '1' when __dirname starts with 'VA -' and '0' otherwise.  Note, it does not look for and correct incorrectly flagged compilations and visa versa - consider enhancing
    set_compilation_flag()

    # set albumartist to NULL for all compilation albums where they are not NULL
    nullify_albumartist_in_va()

    # Applies firstlettercaps to each entry in releasetype if not already firstlettercaps
    capitalise_releasetype()

    # Sorts delimited text strings in fields, dedupes them and compares the result against the original field contents.  When there's a mismatch the newly deduped, sorted string is written back to the underlying table
    dedupe_fields()

    # runs a query that detects duplicated albums based on the sorted md5sum of the audio stream embedded in FLAC files and writes out a few tables to ease identification and (manual) deletion tasks
    # find_duplicate_flac_albums()

    # strips '(live)'' from end of album name and sets LIVE=1 where this is not already the case
    strip_live_from_album_name()

    # build list of unique contibutors by gathering all mbid's found in alib - checking against artist and albumartist fields
    establish_contributors()

    # adds musicbrainz identifiers to artists, albumartists & in future composers (we're adding musicbrainz_composerid of our own volition for future app use)
    add_musicbrainz_identifiers()

    ''' return case sensitivity for LIKE to SQLite default '''
    dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')



    ''' add any other update queries you want to run above this line '''

    conn.commit()
    dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
    return(tally_mods() - start_tally)



if __name__ == '__main__':

    cls()
    if len(sys.argv) < 2 or not exists(sys.argv[1]):
        print(f"""Usage: python {sys.argv[0]} </path/to/database> to process""")
        sys.exit()
    dbfile = sys.argv[1]
    working_dir = dirname(dbfile)
    

    conn = sqlite3.connect(dbfile)
    dbcursor = conn.cursor()
    establish_environment()
    update_tags()
    show_stats_and_log_changes()

    # show_table_differences()

    conn.commit()
    print(f"Compacting database {dbfile}")
    dbcursor.execute("VACUUM")
    dbcursor.close()
    conn.close()
    print(f"\n{'─' * 5}\nDone!\n")

''' todo: ref https://github.com/audiomuze/tags2sqlite
add:
- write out test files: all __dirpath's missing genres, composers, year/date, mbalbumartistid


 '''
