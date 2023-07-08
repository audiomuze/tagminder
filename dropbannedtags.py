from os.path import exists
import sqlite3
import sys

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
	# dbcursor.execute('DROP TRIGGER IF EXISTS sqlmods;')
	dbcursor.execute('CREATE TRIGGER IF NOT EXISTS sqlmods AFTER UPDATE ON alib FOR EACH ROW BEGIN UPDATE alib SET sqlmodded = (CAST(sqlmodded as INTEGER) + 1) WHERE rowid = NEW.rowid; END;')

	''' if a rollback table already exists we are applying further changes or imports, so leave it intact '''
	dbcursor.execute('CREATE TABLE IF NOT EXISTS alib_rollback AS SELECT * FROM alib order by __path;')	

	''' consider adding an update query to add any new records from alib to alib_rollback.  This implies always leaving alib untouched ... in which case why ever create alib_rollback '''

	conn.commit()

def get_columns(table_name):

	dbcursor.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}');")
	return(dbcursor.fetchall())

def show_table_differences():

	''' pick up the columns present in the table '''
	columns = get_columns('alib')

	if table_exists('alib_rollback'):
		for column in columns:

			field_to_compare = column[0]
			# print(f"Changes in {column[0]}:")
			# query = f"select alib.*, alib_rollback.* from alib inner join alib_rollback on alib.__path = alib_rollback.__path where 'alib.{column[0]}' != 'alib_rollback.{column[0]}'"
			dbcursor.execute(f"select alib.__path, 'alib.{field_to_compare}', 'alib_rollback.{field_to_compare}' from alib inner join alib_rollback on alib.__path = alib_rollback.__path where ('alib.{field_to_compare}' != 'alib_rollback.{field_to_compare}');")
			differences = dbcursor.fetchall()
			diffcount = len(differences)
			print(diffcount)
			input()
			for difference in differences:
			 	print(difference[0], difference[1], difference[2])
	


def tally_mods():
	''' start and stop counter that returns how many changes have been triggered at the point of call - will be >= 0 '''

	dbcursor.execute('SELECT COUNT(*) from alib where CAST(sqlmodded AS INTEGER) > 0')
	matches = dbcursor.fetchone()
	
	return (matches[0])

def table_exists(table_name):
	''' test whether table exists '''
	dbcursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}';")
	#if the count is 1, then table exists
	return (dbcursor.fetchone()[0] == 1)
	
def get_badtags():

	''' compare existing tags in alib table against permitted tags and return list of illicit tags '''
	dbcursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('alib') t1 left join permitted_tags t2 on t2.tagname = t1.name WHERE t2.tagname IS NULL;")
	badtags = dbcursor.fetchall()
	if len(badtags) > 0:
		badtags.sort()

	return(badtags)

def kill_badtags(badtags):

	start_tally = tally_mods()

	''' Nullify all instances of badtags '''
	for tagname in badtags:

		if tagname[0] != "__albumgain":
			''' we make an exception for this tag as it's ever present in mp3 and always null, so bypass it '''

			tag = '"' + tagname[0] + '"'
			dbcursor.execute(f"create index if not exists {tag} on alib({tag})")
			dbcursor.execute(f"select count({tag}) from alib")
			tally = dbcursor.fetchone()[0]
			print(f"Wiping {tag}, {tally}")
			dbcursor.execute(f"UPDATE alib set {tag} = NULL WHERE {tag} IS NOT NULL")
			dbcursor.execute(f"drop index if exists {tag}")

	conn.commit()	
	

	return(tally_mods() - start_tally)

def update_tags():
    
    start_tally = tally_mods()
    text_tags = ["_releasecomment", "album", "albumartist", "arranger", "artist", "asin", "barcode", "catalog", "catalognumber", "composer", "conductor", "country", "discsubtitle", "engineer", "ensemble", "genre", "isrc", "label", "lyricist", "mixer", "mood", "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid", "musicbrainz_discid", "musicbrainz_releasegroupid", "musicbrainz_releasetrackid", "musicbrainz_trackid", "musicbrainz_workid", "part", "performer", "personnel", "producer", "recordinglocation", "releasetype", "remixer", "style", "subtitle", "theme", "title", "upc", "version", "work", "writer"]
    
    ''' here you add whatever update and enrichment queries you want to run against the table '''
    for text_tag in text_tags:
        
        print(f"Trimming and removing spurious CRs, LFs and spaces from {text_tag}")
        dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(10), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(10), ''));")
        dbcursor.execute(f"UPDATE alib SET {text_tag} = trim([REPLACE]({text_tag}, char(13), '')) WHERE {text_tag} IS NOT NULL AND {text_tag} != trim([REPLACE]({text_tag}, char(13), ''));")


        ''' merge album name and version fields into album name '''
        # select distinct __dirpath, album, version, album|| ' ' || version as concat, replace(album|| ' ' || version, ' ' || version, '') as reconstituted from alib where version is not null order by __dirpath;
        
    print("Removing performer names where they match artist names")
    dbcursor.execute('UPDATE alib SET performer = NULL WHERE performer = artist;')
    
    ''' add any other update queries you want to run above this line '''
    conn.commit()
    return(tally_mods() - start_tally)    



def log_changes():
	''' write changed records to changed_tags table '''
	print(f"\nGenerating changed_tags table...")
	dbcursor.execute('DROP TABLE IF EXISTS changed_tags;')
	dbcursor.execute('CREATE TABLE changed_tags AS SELECT * FROM alib WHERE CAST(sqlmodded AS INTEGER) > 0 ORDER BY __path;')
	dbcursor.execute('DROP TABLE IF EXISTS changed_records;')
	dbcursor.execute('CREATE TABLE IF NOT EXISTS changed_records AS SELECT * FROM changed_tags;')
	dbcursor.execute('UPDATE changed_tags SET sqlmodded = NULL;')
	conn.commit()


def show_stats(killed_tags):

	''' count number of records changed '''
	dbcursor.execute('SELECT COUNT(*) from alib where CAST(sqlmodded AS INTEGER) > 0')
	records_changed = dbcursor.fetchone()[0]
	
	''' sum number of changes processed '''
	dbcursor.execute('SELECT SUM(CAST(sqlmodded AS INTEGER)) from alib')
	fields_changed = dbcursor.fetchone()[0]
	
	''' sqlite returns null from a sum operation if the field values are null, so test for it, because if the script is run iteratively that'll be the case where alib has been readied for export '''
	if fields_changed == None:
		fields_changed = 0
	
	''' sum number of __dirpaths with changed content '''	
	dbcursor.execute('SELECT DISTINCT __dirpath FROM alib where CAST(sqlmodded AS INTEGER) > 0')
	affected_dirpaths = dbcursor.fetchall()
	affected_dircount = len(affected_dirpaths)

	print(f"\n{records_changed} files have been modified")
	print(f"{killed_tags} bad tags have been removed from files")

	if fields_changed > killed_tags:

		print(f"{fields_changed - killed_tags} tags were modified")
	else:
		print(f"No additional tags were modified")

	print(f"\n{affected_dircount} albums will be affected by writeback")

	
	''' write out affected __dirpaths to enable updating of time signature or further processing outside of this script '''
	if affected_dirpaths:

		affected_dirpaths.sort()
		dbcursor.execute('CREATE TABLE IF NOT EXISTS dirs_to_process (__dirpath BLOB PRIMARY KEY);')

		for dirpath in affected_dirpaths:

			dbcursor.execute(f"REPLACE INTO dirs_to_process (__dirpath) VALUES (?)", dirpath)


def prepare_writeback():

	''' test whether table exists '''
	# dbcursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='alib';")
	# #if the count is 1, then table exists
	# if dbcursor.fetchone()[0] == 1:
	if table_exists('alib'):

		if table_exists('changed_tags'):

			dbcursor.execute('drop table alib;')
			dbcursor.execute('alter table changed_tags rename to alib;')
			print(f"Changed tags have been written to a new table and it has replaced alib - you can now directly export from existing database\nIf you need to rollback you can reinstate tags from table 'alib_rollback'")



if __name__ == '__main__':

	if len(sys.argv) < 2 or not exists(sys.argv[1]):
	    print(f"""Usage: python {sys.argv[0]} </path/to/database> to process""")
	    sys.exit()
	dbfile = sys.argv[1]

	conn = sqlite3.connect(dbfile)
	dbcursor = conn.cursor()
	create_config()
	killed_tags = 0
	badtags = get_badtags()
	if badtags:
		killed_tags = kill_badtags(badtags)
	
	updated_tags = update_tags()
	print(f"\nModified {updated_tags} tags")
	show_stats(killed_tags)

	log_changes()
	print(f"\nDone!\n")
	# show_table_differences()

	conn.commit()
	prepare_writeback()
	dbcursor.execute("VACUUM")
	dbcursor.close()
	conn.close()
