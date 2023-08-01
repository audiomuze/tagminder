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

# 	''' pick up the columns present in the table '''
# 	columns = get_columns('alib')

# 	if table_exists('alib_rollback'):
# 		for column in columns:

# 			field_to_compare = column[0]
# 			# print(f"Changes in {column[0]}:")
# 			# query = f"select alib.*, alib_rollback.* from alib inner join alib_rollback on alib.__path = alib_rollback.__path where 'alib.{column[0]}' != 'alib_rollback.{column[0]}'"
# 			dbcursor.execute(f"select alib.__path, 'alib.{field_to_compare}', 'alib_rollback.{field_to_compare}' from alib inner join alib_rollback on alib.__path = alib_rollback.__path where ('alib.{field_to_compare}' != 'alib_rollback.{field_to_compare}');")
# 			differences = dbcursor.fetchall()
# 			diffcount = len(differences)
# 			print(diffcount)
# 			input()
# 			for difference in differences:
# 			 	print(difference[0], difference[1], difference[2])



def texttags_in_alib(taglist):
    ''' compare existing tags in alib table against list of text tags and eliminate those that are not present in alib '''
    dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib');")
    dbtags = dbcursor.fetchall()
    ''' generate a list of the first element of each tuple in the list of tuples that is dbtags '''
    dblist = list(zip(*dbtags))[0]
    ''' build list of matching tagnames in dblist '''
    return([tag for tag in taglist if tag in dblist])

	

# def get_badtags():
# 	''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
# 	dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
# 	badtags = dbcursor.fetchall()
# 	if len(badtags) > 0:
# 		badtags.sort()
# 	return(badtags)



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
	'''  select all records with '[' in title, split-off text everying folowing '[' and write it out to subtitle '''
	opening_tally = tally_mods()
	print(f"\nUpdating titles to remove any text enclosed in square brackets from TITLE and appending same to SUBTITLE tag")
	# dbcursor.execute("UPDATE alib SET title = IIF(TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) = '', title, TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1))), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%';")
	dbcursor.execute("UPDATE alib SET title = TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)), subtitle = IIF(subtitle IS NULL OR TRIM(subtitle) = '', SUBSTR(title, INSTR(title, '[')), subtitle || ' ' || SUBSTR(title, INSTR(title, '['))) WHERE title LIKE '%[%' AND TRIM(SUBSTR(title, 1, INSTR(title, '[') - 1)) != '';")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")



def feat_to_artist():
	''' Move all instances of Feat to ARTIST tag '''

	opening_tally = tally_mods()
	feat_instances = [
	'(Feat. %',
	'[Feat. ',
	'(feat. ',
	'[feat. ',
	'(Feat ',
	'[Feat ',
	'(feat ',
	'[feat ']

	print('\n')
	dbcursor.execute(f"create index if not exists titles_artists on alib(title, artist)")

	for feat_instance in feat_instances:

		print(f"Stripping {feat_instance} from track TITLE and appending performers to ARTIST tag...")
		# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), artist = artist || '\\\\' || REPLACE(replace(substr(title, instr(title, ?) ), ?, ''), ')', '')  WHERE title LIKE ? AND (trim(substr(title, 1, instr(title, ?) - 1) ) != '');",  (feat_instance, feat_instance, feat_instance, '%'+feat_instance+'%', feat_instance))
		dbcursor.execute('''UPDATE alib
							   SET title = trim(substr(title, 1, instr(title, ?) - 1) ),
							       artist = artist || '\\\\' || REPLACE(replace(substr(title, instr(title, ?) ), ?, ''), ')', '') 
							 WHERE title LIKE ? AND 
							       (trim(substr(title, 1, instr(title, ?) - 1) ) != '');''', (feat_instance, feat_instance, feat_instance, '%'+feat_instance+'%', feat_instance))



	dbcursor.execute(f"drop index if exists titles_artists")
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
	dbcursor.execute("UPDATE alib SET live = '1' WHERE LOWER(subtitle) LIKE '%live%' AND live != '1';")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")


def live_means_live_in_subtitle():
	''' Ensure any tracks that have LIVE = 1 also have [Live] in the SUBTITLE '''
	print("\nEnsuring any tracks that have LIVE = 1 also have [Live] in the SUBTITLE")
	opening_tally = tally_mods()
	dbcursor.execute("UPDATE alib SET subtitle = '[Live]' WHERE live = '1' AND subtitle IS NULL OR TRIM(subtitle) = '';")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")


def tag_live_tracks():

	live_instances = [
	'(Live In',
	'[Live In',
	'(Live in',
	'[Live in',
	'(live in',
	'[live in',
	'(Live At',
	'[Live At',
	'(Live at',
	'[Live at',
	'(live at',
	'[live at']

	''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
	dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')

	print('\n')
	dbcursor.execute(f"create index if not exists titles_subtitles on alib(title, subtitle)")
	opening_tally = tally_mods()
	for live_instance in live_instances:

		print(f"Stripping {live_instance} from track titles...")
		dbcursor.execute(f"UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), subtitle = substr(title, instr(title, ?)) WHERE (title LIKE ? AND subtitle IS NULL);", (live_instance, live_instance, '%'+live_instance+'%'))
		dbcursor.execute(f"UPDATE alib SET subtitle = subtitle || '\\\\' || trim(substr(title, instr(title, ?))), title = trim(substr(title, 1, instr(title, ?) - 1) ) WHERE (title LIKE ? AND subtitle IS NOT NULL);", (live_instance, live_instance, '%'+live_instance+'%'))

	dbcursor.execute(f"drop index if exists titles_subtitles")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")


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
	return("Not yet coded")


def merge_album_version():
	''' merge album name and version fields into album name '''
	print(f"\nMerging album name and version fields into album name")
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
						 WHERE (compilation IS NULL AND 
						        substring(__dirname, 1, 4) = 'VA -' AND 
						        albumartist IS NULL);''')

	dbcursor.execute('''
						UPDATE alib
						   SET compilation = '0'
						 WHERE (compilation IS NULL AND 
						        substring(__dirname, 1, 4) != 'VA -' AND 
						        albumartist IS NOT NULL);''')

	print(f"|\n{tally_mods() - opening_tally} tags were modified")



def nullify_va ():
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



def normalise_releasetype():
	print(f"\nSetting 'First Letter Caps' for all instances of releasetype")
	opening_tally = tally_mods()
	dbcursor.execute('''SELECT DISTINCT releasetype FROM alib WHERE releasetype IS NOT NULL;''')
	releasetypes = dbcursor.fetchall()
	for release in releasetypes:
		print(release[0])
		flc = firstlettercaps(release[0])
		print(release[0], flc)
		dbcursor.execute('''UPDATE alib SET releasetype = (?) WHERE releasetype = (?) AND releasetype != (?);''', (flc, release[0], flc))
	print(f"|\n{tally_mods() - opening_tally} tags were modified")



def update_tags():
	''' function call to run mass tagging updates.  It is preferable to run update_tags prior to killing bad_tags so that data can be moved to good tags where present in non-standard tags such as 'recording location' & unsyncedlyrics
	Consider whether it'd be better to break this lot into discrete functions '''

	''' set up initialisation counter '''
	start_tally = tally_mods()

	''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
	dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


	''' here you add whatever update and enrichment queries you want to run against the table '''

	# unsyncedlyrics_to_lyrics()
	# kill_badtags()
	# nullify_empty_tags()
	# trim_and_remove_crlf()
	# feat_to_artist()
	# merge_recording_locations()
	# release_to_version()
	# nullify_performers_matching_artists()
	# tag_live_tracks()
	# title_keywords_to_subtitle()
	# square_brackets_to_subtitle()
	# live_in_subtitle_means_live()
	# live_means_live_in_subtitle()
	# kill_singular_discnumber()
	# merge_album_version()
	# set_compilation_flag()
	# nullify_va()
	normalise_releasetype()
	# strip_live_from_album_name()

	''' return case sensitivity for LIKE to SQLite default '''
	dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


		# ''' now there may be a few '%[Live ' and '%(Live ' still hanging around, process them also '''
		# opening_tally = tally_mods()
		# print(f"\nStripping '[Live ' and '(Live ' from end of track TITLE, marking track as Live track")
		# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live ') - 1) ), subtitle = IIF(subtitle IS NULL, trim(substr(title, instr(title, '[Live '), length(title))),subtitle || '\\\\' || trim(substr(title, instr(title, '[Live '), length(title)))), live = 1 WHERE title LIKE ? ;",  ('%'+' [Live '+'%',))
		# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live ') - 1) ), subtitle = IIF(subtitle IS NULL, trim(substr(title, instr(title, '(Live '), length(title))),subtitle || '\\\\' || trim(substr(title, instr(title, '(Live '), length(title)))), live = 1 WHERE title LIKE ? ;",  ('%'+' (Live '+'%',))
		# print(f"|\n{tally_mods() - opening_tally} tags were modified")	



	''' add any other update queries you want to run above this line '''

	conn.commit()
	dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
	return(tally_mods() - start_tally)


def show_stats_and_log_changes():

	''' count number of records changed '''
	records_changed = changed_records()
	
	''' sum number of changes processed '''	
	updates_made = tally_mods()

	''' sum the number of __dirpaths changed '''
	dir_count = affected_dircount()


	''' get list of all affected __dirpaths '''
	changed_dirpaths = affected_dirpaths()

	print(f"\n")
	print('─' * 120)
	print(f"{updates_made} updates have been processed against {records_changed} files, affecting {dir_count} albums")
	print('─' * 120)

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
		# dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib_rollback AS SELECT * FROM alib_rollback ORDER BY __path")	

		conn.commit()
		
		print(f"Affected folders have been written out to text file: {dirlist}")
		print(f"\nChanged tags have been written to a database: {export_db} in table alib.\nIt contains only changed records with sqlmodded set to NULL for writing back to underlying file tags.")
		print(f"You can now directly export from this database to the underlying files using tagfromdb3.py.\n\nIf you need to rollback changes you can reinstate tags from table 'alib_rollback' in {dbfile}\n")
		percent_affected = (records_changed / library_size())*100
		print(f"{'%.2f' % percent_affected} percent of tracks in library have been modified.")

	else:
		print("- No changes were processed\n")



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
	# print(f"Compacting database {dbfile}")
	# dbcursor.execute("VACUUM")
	dbcursor.close()
	conn.close()
	print(f"\n{'─' * 5}\nDone!\n")

''' todo: ref https://github.com/audiomuze/tags2sqlite
add:
- write out test files: all __dirpath's missing genres, composers, year/date, mbalbumartistid


 '''
