from os.path import exists
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
	dbcursor.execute('drop table if exists permitted_tags;')
	dbcursor.execute('create table permitted_tags (tagname text);')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__accessed")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__app")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__bitrate")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__bitrate_num")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__bitspersample")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__channels")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__created")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__dirname")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__dirpath")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__ext")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_access_date")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_access_datetime")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_access_datetime_raw")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_create_date")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_create_datetime")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_create_datetime_raw")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_mod_date")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_mod_datetime")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_mod_datetime_raw")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_size")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_size_bytes")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_size_kb")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__file_size_mb")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__filename")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__filename_no_ext")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__filetype")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__frequency")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__frequency_num")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__image_mimetype")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__image_type")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__layer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__length")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__length_seconds")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__md5sig")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__mode")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__modified")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__num_images")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__parent_dir")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__path")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__size")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__tag")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__tag_read")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__vendorstring")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("__version")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("_releasecomment")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("acousticbrainz_mood")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("acoustid_fingerprint")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("acoustid_id")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("album")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("albumartist")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("amg_album_id")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("amg_boxset_url")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("amg_url")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("amgtagged")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("analysis")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("arranger")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("artist")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("asin")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("barcode")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("bootleg")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("catalog")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("catalognumber")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("compilation")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("composer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("conductor")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("country")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("date")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("discnumber")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("discogs_artist_url")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("discogs_release_url")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("discsubtitle")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("engineer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("ensemble")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("fingerprint")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("genre")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("isrc")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("label")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("live")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("lyricist")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("lyrics")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("movement")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("mixer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("mood")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_albumartistid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_albumid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_artistid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_discid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_releasegroupid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_releasetrackid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_trackid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("musicbrainz_workid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("originaldate")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("originalreleasedate")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("originalyear")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("part")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("performancedate")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("performer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("personnel")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("producer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("rating")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("recordinglocation")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("recordingstartdate")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("reflac")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("releasetype")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("remixer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("replaygain_album_gain")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("replaygain_album_peak")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("replaygain_track_gain")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("replaygain_track_peak")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("review")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("roonalbumtag")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("roonradioban")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("roontracktag")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("roonid")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("sqlmodded")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("style")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("subtitle")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("theme")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("title")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("track")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("tracknumber")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("upc")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("version")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("work")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("writer")')
	dbcursor.execute('insert into permitted_tags ( tagname ) values ("year")')

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
	

def get_badtags():
	''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
	dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
	badtags = dbcursor.fetchall()
	if len(badtags) > 0:
		badtags.sort()
	return(badtags)


def kill_badtags(badtags):
	''' iterate over unwanted tags and set any non NULL values to NULL '''

	start_tally = tally_mods()
	print("Wiping spurious tags:")

	for tagname in badtags:

		if tagname[0] != "__albumgain":
			''' make an exception for __albumgain as it's ever present in mp3 and always null, so bypass it as it'd waste a cycle '''

			tag = '"' + tagname[0] + '"'
			dbcursor.execute(f"create index if not exists {tag} on alib({tag})")
			dbcursor.execute(f"select count({tag}) from alib")
			tally = dbcursor.fetchone()[0]
			print(f"- {tag}, {tally}")
			dbcursor.execute(f"UPDATE alib set {tag} = NULL WHERE {tag} IS NOT NULL")
			dbcursor.execute(f"drop index if exists {tag}")
			conn.commit() # it should be possible to move this out of the for loop, but then just check that trigger is working correctly

	return(tally_mods() - start_tally)


def update_tags():
	''' function call to run mass tagging updates.  Consider whether it'd be better to break this lot into discrete functions '''

	''' turn on case sensitivity for LIKE so that we don't inadvertently process records we don't want to '''
	dbcursor.execute('PRAGMA case_sensitive_like = TRUE;')


	''' here you add whatever update and enrichment queries you want to run against the table '''
	start_tally = tally_mods()
	text_tags = ["_releasecomment", "album", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", "discsubtitle", "engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "movement", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", "musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "part", "performer", "personnel", "producer", "recordinglocation", "releasetype", "remixer", "style", "subtitle", "theme", "title", "upc", "version", "work", "writer"]
	for text_tag in text_tags:
		print(f"Trimming and removing spurious CRs, LFs and SPACES from {text_tag}")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(10), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(10), ''));")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(13), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(13), ''));")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, ' \\','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, ' \\','\\');")
		dbcursor.execute(f"UPDATE alib SET {text_tag} = [REPLACE]({text_tag}, '\\ ','\\') WHERE {text_tag} IS NOT NULL AND {text_tag} != [REPLACE]({text_tag}, '\\ ','\\');")

	''' merge album name and version fields into album name '''
	print("Merging album name and version fields into album name")
	dbcursor.execute(f"UPDATE alib SET album = album || ' ' || version WHERE version IS NOT NULL AND NOT INSTR(album, version);")

	''' strip extranious info from track title and write it to subtitle or other most appropriate tag '''

	''' this transforms uppercase '(Live in...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	print("Stripping '(Live in' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live in%' AND title NOT LIKE '%(live in%' AND subtitle IS NULL);")

	''' this transforms lowercase '(live in...)' without affecting 'Live' appearing elsewhere in the string being assessed '''
	print("Stripping '(live in' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(live') - 1) ), subtitle = substr(title, instr(title, '(live') ) WHERE (title LIKE '%(live in%' AND title NOT LIKE '%(Live in%' AND subtitle IS NULL);")

	''' this transforms uppercase '[Live in...]' without affecting 'live' appearing elsewhere in the string being assessed '''
	print("Stripping '[Live in' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live in%' AND title NOT LIKE '%[live in%' AND subtitle IS NULL);")

	''' this transforms lowercase '[live in...]' without affecting 'Live' appearing elsewhere in the string being assessed '''
	print("Stripping '[live in' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[live') - 1) ), subtitle = substr(title, instr(title, '[live') ) WHERE (title LIKE '%[live in%' AND title NOT LIKE '%[Live in%' AND subtitle IS NULL);")


	''' this transforms uppercase '(Live at...)' without affecting 'live' appearing elsewhere in the string being assessed '''
	print("Stripping '(Live at' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Live') - 1) ), subtitle = substr(title, instr(title, '(Live') ) WHERE (title LIKE '%(Live at%' AND title NOT LIKE '%(live at%' AND subtitle IS NULL);")

	''' this transforms lowercase '(live at...)' without affecting 'Live' appearing elsewhere in the string being assessed '''
	print("Stripping '(live at' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(live') - 1) ), subtitle = substr(title, instr(title, '(live') ) WHERE (title LIKE '%(live at%' AND title NOT LIKE '%(Live at%' AND subtitle IS NULL);")

	''' this transforms uppercase '[Live at...]' without affecting 'live' appearing elsewhere in the string being assessed '''
	print("Stripping '[Live at' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Live') - 1) ), subtitle = substr(title, instr(title, '[Live') ) WHERE (title LIKE '%[Live at%' AND title NOT LIKE '%[live at%' AND subtitle IS NULL);")

	''' this transforms lowercase '[live at...]' without affecting 'Live' appearing elsewhere in the string being assessed '''
	print("Stripping '[live at' from track titles")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[live') - 1) ), subtitle = substr(title, instr(title, '[live') ) WHERE (title LIKE '%[live at%' AND title NOT LIKE '%[Live at%' AND subtitle IS NULL);")


	''' convert all instances of %(Feat. '''
	print("Stripping '(Feat. ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(Feat. ') ), '(Feat. ', ''), ')', '')  WHERE title LIKE '%(Feat. %' AND  (trim(substr(title, 1, instr(title, '(Feat. ') - 1) ) != '') AND  title NOT LIKE '%(feat. %';")

	''' convert all instances of %(feat. '''
	print("Stripping '(feat. ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(feat. ') ), '(feat. ', ''), ')', '') WHERE title LIKE '%(feat. %' AND (trim(substr(title, 1, instr(title, '(feat. ') - 1) ) != '') AND title NOT LIKE '%(Feat. %';")

	''' convert all instances of %(Feat '''
	print("Stripping '(Feat ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(Feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(Feat ') ), '(Feat ', ''), ')', '') WHERE title LIKE '%(Feat %' AND (trim(substr(title, 1, instr(title, '(Feat ') - 1) ) != '') AND title NOT LIKE '%(feat  %';")

	''' convert all instances of %(feat '''
	print("Stripping '(feat ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '(feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '(feat ') ), '(feat ', ''), ')', '') WHERE title LIKE '%(feat %' AND (trim(substr(title, 1, instr(title, '(feat ') - 1) ) != '') AND title NOT LIKE '%(Feat  %';")

	''' convert all instances of %[Feat. '''
	print("Stripping '[Feat. ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[Feat. ') ), '[Feat. ', ''), ')', '')  WHERE title LIKE '%(Feat. %' AND  (trim(substr(title, 1, instr(title, '[Feat. ') - 1) ) != '') AND  title NOT LIKE '%[feat. %';")

	''' convert all instances of %[feat. '''
	print("Stripping '[feat. ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[feat. ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[feat. ') ), '[feat. ', ''), ')', '') WHERE title LIKE '%(feat. %' AND (trim(substr(title, 1, instr(title, '[feat. ') - 1) ) != '') AND title NOT LIKE '%[Feat. %';")

	''' convert all instances of %[Feat '''
	print("Stripping '[Feat ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[Feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[Feat ') ), '[Feat ', ''), ')', '') WHERE title LIKE '%(Feat %' AND (trim(substr(title, 1, instr(title, '[Feat ') - 1) ) != '') AND title NOT LIKE '%[feat  %';")

	''' convert all instances of %[feat '''
	print("Stripping '[Feat ' from track titles and appending performer names to artist field")
	dbcursor.execute("UPDATE alib SET title = trim(substr(title, 1, instr(title, '[feat ') - 1) ), artist = artist || '\\' || REPLACE(replace(substr(title, instr(title, '[feat ') ), '[feat ', ''), ')', '') WHERE title LIKE '%(feat %' AND (trim(substr(title, 1, instr(title, '[feat ') - 1) ) != '') AND title NOT LIKE '%[Feat  %';")


	''' remove performer names where they match artist names '''
	print("Removing performer names where they match artist names")
	dbcursor.execute('UPDATE alib SET performer = NULL WHERE lower(performer) = lower(artist);')

	''' add any other update queries you want to run above this line '''
	conn.commit()
	return(tally_mods() - start_tally)    

def log_changes():
	''' write changed records to changed_tags table '''
	print(f"\nGenerating changed_tags table...")
	dbcursor.execute('DROP TABLE IF EXISTS changed_tags;')
	dbcursor.execute('CREATE TABLE changed_tags AS SELECT * FROM alib WHERE sqlmodded IS NOT NULL ORDER BY __path;')
	dbcursor.execute('DROP TABLE IF EXISTS changed_records;')
	dbcursor.execute('CREATE TABLE IF NOT EXISTS changed_records AS SELECT * FROM changed_tags;')
	''' Export changed records for writing back to tags '''
	dbcursor.execute('UPDATE changed_tags SET sqlmodded = NULL;')
	conn.commit()


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


# def prepare_writeback():

# 	''' test whether table exists '''
# 	dbcursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='alib';")
# 	#if the count is 1, then table exists
# 	if dbcursor.fetchone()[0] == 1:
# 	if table_exists('alib'):

# 		if table_exists('changed_tags'):

# 			dbcursor.execute('drop table alib;')
# 			dbcursor.execute('alter table changed_tags rename to alib;')
# 			print(f"Changed tags have been written to a new table and it has replaced alib - you can now directly export from existing database\nIf you need to rollback you can reinstate tags from table 'alib_rollback'")

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
	print(f"\nDone!\n")
	# show_table_differences()

	conn.commit()
	# prepare_writeback()
	dbcursor.execute("VACUUM")
	dbcursor.close()
	conn.close()
