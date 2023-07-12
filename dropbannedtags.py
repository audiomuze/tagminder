from os.path import exists
import csv
import sqlite3
import sys



def table_exists(table_name):
	''' test whether table exists in a database'''
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
    return([tag in dblist])


def dedupe_and_sort(list_item, delimiter):
	''' get a list item that contains a delimited string, dedupe and sort it and pass it back '''
	distinct_items = set(x.strip() for x in list_item.split(delimiter))
	return (delimiter.join(sorted(distinct_items)))


def tally_mods():
	''' start and stop counter that returns how many changes have been triggered at the point of call - will be >= 0 '''
	dbcursor.execute('SELECT sum(CAST (sqlmodded AS INTEGER) ) FROM alib WHERE sqlmodded IS NOT NULL;')
	matches = dbcursor.fetchone()
	if matches == None:
		''' sqlite returns null from a sum operation if the field values are null, so test for it, because if the script is run iteratively that'll be the case where alib has been readied for export '''
		return(0)
	return(matches[0])


def changed_records():
	''' returns how many records have been changed at the point of call - will be >= 0 '''
	dbcursor.execute('SELECT count(sqlmodded) FROM alib;')
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


def create_config():
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
	"date",
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
	"tracknumber",
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
	

def get_badtags():
	''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
	dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
	badtags = dbcursor.fetchall()
	if len(badtags) > 0:
		badtags.sort()
	return(badtags)


def kill_badtags(badtags):
	''' iterate over unwanted tags and set any non NULL values to NULL '''

	opening_tally = tally_mods()
	print(f"\nWiping spurious tags:")

	for tagname in badtags:

		if tagname[0] != "__albumgain":
			''' make an exception for __albumgain as it's ever present in mp3 and always null, so bypass it as it'd waste a cycle '''

			''' append quotes to tag names in case any have a space in the field name '''
			tag = '"' + tagname[0] + '"'
			dbcursor.execute(f"create index if not exists {tag} on alib({tag})")
			dbcursor.execute(f"select count({tag}) from alib")
			tally = dbcursor.fetchone()[0]
			print(f"- {tag}, {tally}")
			dbcursor.execute(f"UPDATE alib set {tag} = NULL WHERE {tag} IS NOT NULL")
			dbcursor.execute(f"drop index if exists {tag}")
			conn.commit() # it should be possible to move this out of the for loop, but then just check that trigger is working correctly

	closing_tally = tally_mods()
	print(f"|\n{closing_tally - opening_tally} tags were removed")
	return(closing_tally - start_tally)


def update_tags():
	''' function call to run mass tagging updates.  It is preferable to running update_tags prior to killing bad_tags so that data can be moved to good tags where present in non-standard tags such as 'recording location'
	Consider whether it'd be better to break this lot into discrete functions '''

	''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
	dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


	''' here you add whatever update and enrichment queries you want to run against the table '''
	start_tally = tally_mods()

	''' list all known text tags you might want to process against '''
	all_text_tags = ["_releasecomment", "album", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", "discsubtitle", 
	"engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "movement", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", 
	"musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "part", "performer", "personnel", "producer", "recordinglocation", "releasetype", 
	"remixer", "style", "subtitle", "theme", "title", "upc", "version", "work", "writer"]

	''' narrow it down to the list that's actually present in alib table. based on what's been imported '''
	text_tags = texttags_in_alib(all_text_tags)

	print(f"Identifying and removing spurious CRs, LFs and SPACES in:")
	opening_tally = start_tally
	for text_tag in text_tags:
		dbcursor.execute(f"create index if not exists {text_tag} on alib({text_tag})")
		print(f"- {text_tag}")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(10), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(10), ''));")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(13), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(13), ''));")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, ' \\','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, ' \\','\\');")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, '\\ ','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, '\\ ','\\');")
		dbcursor.execute(f"drop index if exists {text_tag}")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")


	''' strip extranious info from track title and write it to subtitle or other most appropriate tag '''

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

	print('\n')
	dbcursor.execute(f"create index if not exists titles_subtitles on alib(title, subtitle)")
	opening_tally = tally_mods()
	for live_instance in live_instances:

		print(f"Stripping {live_instance} from track titles...")
		dbcursor.execute(f"UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), subtitle = substr(title, instr(title, ?)), live = IIF(live != '1', '1', live) WHERE (title LIKE ? AND subtitle IS NULL);", (live_instance, live_instance, '%'+live_instance+'%'))

	dbcursor.execute(f"drop index if exists titles_subtitles")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")


	# print(f"\nStripping from track titles:")
	# ''' this transforms uppercase '(Live in...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '(Live in' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live in%' AND title NOT LIKE '%(live in%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '(Live At...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '(Live In' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live In%' AND title NOT LIKE '%(live in%' AND subtitle IS NULL);")

	# ''' this transforms lowercase '(live in...)' without affecting 'Live' appearing elsewhere in the string being assessed '''
	# print("- '(live in' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(live') - 1) ), subtitle = substr(title, instr(title, '(live') ) WHERE (title LIKE '%(live in%' AND title NOT LIKE '%(Live in%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '[Live in...]' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '[Live in' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live in%' AND title NOT LIKE '%[live in%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '(Live At...)' without affecting 'live' appearing elsewhere in the string being assessed '''	
	# print("- '[Live In' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live In%' AND title NOT LIKE '%[live in%' AND subtitle IS NULL);")

	# ''' this transforms lowercase '[live in...]' without affecting 'Live' appearing elsewhere in the string being assessed '''
	# print("- '[live in' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[live') - 1) ), subtitle = substr(title, instr(title, '[live') ) WHERE (title LIKE '%[live in%' AND title NOT LIKE '%[Live in%' AND subtitle IS NULL);")


	# ''' this transforms uppercase '(Live at...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '(Live at' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live at%' AND title NOT LIKE '%(live at%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '(Live At...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '(Live At' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live At%' AND title NOT LIKE '%(live at%' AND subtitle IS NULL);")

	# ''' this transforms lowercase '(live at...)' without affecting 'Live' appearing elsewhere in the string being assessed '''
	# print("- '(live at' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(live') - 1) ), subtitle = substr(title, instr(title, '(live') ) WHERE (title LIKE '%(live at%' AND title NOT LIKE '%(Live at%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '[Live at...]' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '[Live At' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live At%' AND title NOT LIKE '%[live at%' AND subtitle IS NULL);")

	# ''' this transforms uppercase '[Live at...]' without affecting 'live' appearing elsewhere in the string being assessed '''
	# print("- '[Live at' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live at%' AND title NOT LIKE '%[live at%' AND subtitle IS NULL);")

	# ''' this transforms lowercase '[live at...]' without affecting 'Live' appearing elsewhere in the string being assessed '''
	# print("- '[live at' from track titles")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[live') - 1) ), subtitle = substr(title, instr(title, '[live') ) WHERE (title LIKE '%[live at%' AND title NOT LIKE '%[Live at%' AND subtitle IS NULL);")

	feat_instances = [
	'(Feat. ',
	'[Feat. ',
	'(feat. ',
	'[feat. ',
	'(Feat ',
	'[Feat ',
	'(feat ',
	'[feat ']

	print('\n')
	dbcursor.execute(f"create index if not exists titles_artists on alib(title, artist)")
	opening_tally = tally_mods()
	for feat_instance in feat_instances:

		print(f"Stripping {feat_instance} from track titles and appending performers to ARTIST tag...")
		dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, ?) - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, ?) ), ?, ''), ')', '')  WHERE title LIKE ? AND (trim(substr(title, 1, instr(title, ?) - 1) ) != '');",  (feat_instance, feat_instance, feat_instance, '%'+feat_instance+'%', feat_instance))

	dbcursor.execute(f"drop index if exists titles_artists")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")	

	# print(f"\nStripping from track titles and appending performer names to artist field:")
	# ''' convert all instances of %(Feat. '''
	# print("- '(Feat. ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(Feat. ') ), '(Feat. ', ''), ')', '')  WHERE title LIKE '%(Feat. %' AND  (trim(substr(title, 1, instr(title, '(Feat. ') - 1) ) != '') AND  title NOT LIKE '%(feat. %';")

	# ''' convert all instances of %(feat. '''
	# print("- '(feat. ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(feat. ') ), '(feat. ', ''), ')', '') WHERE title LIKE '%(feat. %' AND (trim(substr(title, 1, instr(title, '(feat. ') - 1) ) != '') AND title NOT LIKE '%(Feat. %';")

	# ''' convert all instances of %(Feat '''
	# print("- '(Feat ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(Feat ') ), '(Feat ', ''), ')', '') WHERE title LIKE '%(Feat %' AND (trim(substr(title, 1, instr(title, '(Feat ') - 1) ) != '') AND title NOT LIKE '%(feat  %';")

	# ''' convert all instances of %(feat '''
	# print("- '(feat ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(feat ') ), '(feat ', ''), ')', '') WHERE title LIKE '%(feat %' AND (trim(substr(title, 1, instr(title, '(feat ') - 1) ) != '') AND title NOT LIKE '%(Feat  %';")

	# ''' convert all instances of %[Feat. '''
	# print("- '[Feat. ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[Feat. ') ), '[Feat. ', ''), ')', '')  WHERE title LIKE '%[Feat. %' AND  (trim(substr(title, 1, instr(title, '[Feat. ') - 1) ) != '') AND  title NOT LIKE '%[feat. %';")

	# ''' convert all instances of %[feat. '''
	# print("- '[feat. ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[feat. ') ), '[feat. ', ''), ')', '') WHERE title LIKE '%[feat. %' AND (trim(substr(title, 1, instr(title, '[feat. ') - 1) ) != '') AND title NOT LIKE '%[Feat. %';")

	# ''' convert all instances of %[Feat '''
	# print("- '[Feat ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[Feat ') ), '[Feat ', ''), ')', '') WHERE title LIKE '%[Feat %' AND (trim(substr(title, 1, instr(title, '[Feat ') - 1) ) != '') AND title NOT LIKE '%[feat  %';")

	# ''' convert all instances of %[feat '''
	# print("- '[Feat ' from track titles and appending performer names to artist field")
	# dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[feat ') ), '[feat ', ''), ')', '') WHERE title LIKE '%([eat %' AND (trim(substr(title, 1, instr(title, '[feat ') - 1) ) != '') AND title NOT LIKE '%[Feat  %';")


	''' merge album name and version fields into album name '''
	print(f"\nMerging album name and version fields into album name")
	opening_tally = tally_mods()
	dbcursor.execute(f"UPDATE alib SET album = album || ' ' || version WHERE version IS NOT NULL AND NOT INSTR(album, version);")
	print(f"|\n{tally_mods() - opening_tally} tags were modified")

	''' append "recording location" to recordinglocation if recordinglocation is empty '''
	
	if tag_in_table('recording location', 'alib'):
		opening_tally = tally_mods()
		print(f"\nAppending recording location to recordinglocation where recordinglocation is empty")
		x = 'alib."recording location"'
		dbcursor.execute(f"UPDATE alib SET recordinglocation = {x} WHERE (recordinglocation IS NULL AND {x} IS NOT NULL) OR (recordinglocation IS NOT NULL AND {x} IS NOT NULL AND NOT INSTR(recordinglocation, {x}) AND NOT INSTR({x}, recordinglocation));")
		print(f"|\n{tally_mods() - opening_tally} tags were modified")

	''' remove performer names where they match artist names '''
	opening_tally = tally_mods()
	print(f"\nRemoving performer names where they match artist names")
	dbcursor.execute('UPDATE alib SET performer = NULL WHERE lower(performer) = lower(artist);')
	print(f"|\n{tally_mods() - opening_tally} tags were modified")

	''' add any other update queries you want to run above this line '''
	conn.commit()
	dbcursor.execute('PRAGMA case_sensitive_like = FALSE;')
	return(tally_mods() - start_tally)


def show_stats(killed_tags):

	''' count number of records changed '''
	records_changed = changed_records()
	
	''' sum number of changes processed '''	
	fields_changed = tally_mods()

	''' get list of all affected __dirpaths '''
	changed_dirpaths = affected_dirpaths()
	changed_dircount = affected_dircount()

	print(f"\n{records_changed} files have been modified")
	print(f"{killed_tags} bad tags have been removed from files")

	if fields_changed > killed_tags:

		print(f"{fields_changed - killed_tags} tags were modified")
	else:
		print(f"No additional tags were modified")

	print(f"\n{changed_dircount} albums will be affected by writeback")

	
	''' write out affected __dirpaths to enable updating of time signature or further processing outside of this script '''
	if changed_dirpaths:

		changed_dirpaths.sort()
		dbcursor.execute('CREATE TABLE IF NOT EXISTS dirs_to_process (__dirpath BLOB PRIMARY KEY);')

		for dirpath in changed_dirpaths:

			dbcursor.execute(f"REPLACE INTO dirs_to_process (__dirpath) VALUES (?)", dirpath)

		conn.commit()


def log_changes():
	''' write changed records to changed_tags table '''
	print(f"\nGenerating changed_tags table...")

	''' Create an export database and write out alib containing changed records with sqlmodded set to NULL for writing back to underlying file tags '''
	dbcursor.execute("ATTACH DATABASE '/tmp/export.db' AS alib2")
	dbcursor.execute("DROP TABLE IF EXISTS  alib2.alib")
	dbcursor.execute("CREATE TABLE IF NOT EXISTS alib2.alib AS SELECT * FROM alib WHERE sqlmodded IS NOT NULL ORDER BY __path")
	dbcursor.execute("UPDATE alib2.alib SET sqlmodded = NULL;")

	data = dbcursor.execute("SELECT * FROM dirs_to_process")
	with open('/tmp/dirs2process', 'w', newline='') as filehandle:
	    writer = csv.writer(filehandle, delimiter = '|', quoting=csv.QUOTE_NONE)
	    writer.writerows(data)

	print("Affected folders have been written out to text file:\n/tmp/dirs2process\n")
	print(f"Changed tags have been written to a database:\n/tmp/export.db with table alib.\n^^^ this alib table contains only changed records with sqlmodded set to NULL for writing back to underlying file tags.\n")
	print(f"You can now directly export from database:\n/tmp/export.db\n\nIf you need to rollback changes you can reinstate tags from table 'alib_rollback' in:\n{dbfile}\n")

	conn.commit()
	

if __name__ == '__main__':

	if len(sys.argv) < 2 or not exists(sys.argv[1]):
	    print(f"""Usage: python {sys.argv[0]} </path/to/database> to process""")
	    sys.exit()
	dbfile = sys.argv[1]

	conn = sqlite3.connect(dbfile)
	dbcursor = conn.cursor()
	create_config()
	killed_tags = 0
	updated_tags = update_tags()
	badtags = get_badtags()
	if badtags:
		killed_tags = kill_badtags(badtags)
	print(f"\nModified {updated_tags} tags")
	show_stats(killed_tags)
	log_changes()

	# show_table_differences()

	conn.commit()
	print(f"Compacting database {dbfile}")
	dbcursor.execute("VACUUM")
	dbcursor.close()
	conn.close()
	print(f"Done!\n")

''' todo: ref https://github.com/audiomuze/tags2sqlite
add:
- write out test files: all __dirpath's missing genres, composers, year/date, mbalbumartistid


 '''
