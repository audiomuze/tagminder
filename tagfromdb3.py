#!/usr/bin/env python

import argparse
import os
import sqlite3
import sys
import logging
import re

try:
    from os import scandir
except ImportError:
    from scandir import scandir  # use scandir PyPI module on Python < 3.5

from operator import itemgetter


import puddlestuff
from puddlestuff import audioinfo

class compare:
	"Natural sorting class."
	def try_int(self, s):
		"Convert to integer if possible."
		try: return int(s)
		except: return s
	def natsort_key(self, s):
		"Used internally to get a tuple by which s is sorted."
		return map(self.try_int, re.findall(r'(\d+|\D+)', s))
	def natcmp(self, a, b):
		"Natural string comparison, case sensitive."
		return cmp(self.natsort_key(a), self.natsort_key(b))
	def natcasecmp(self, a, b):
		"Natural string comparison, ignores case."
		return self.natcmp(u"".join(a).lower(), u"".join(b).lower())

natcasecmp = compare().natcasecmp

def sort_field(m_text, order='Ascending', matchcase=False):
	"""Sort values, "Sort $0, order='$1', Match Case='$2'"
&Order, combo, Ascending, Descending,
Match &Case, check"""
	text = m_text
	if not matchcase:
		cmp = natcasecmp
	else:
		cmp = None
	if isinstance(text, str):
		return text
	if order == u'Ascending':
		return sorted(text)
	else:
		return sorted(text, reverse=True)

def remove_dupes(m_text, matchcase=False):
	"""Remove duplicate values, "Remove Dupes: $0, Match Case $1" Match &Case, check"""
	text = m_text
	if isinstance(text, str):
		return text


	if matchcase:
		ret = []
		append = ret.append
		[append(z) for z in text if z not in ret]
		return ret
	else:
		ret = []
		lowered = set()
		for z in text:
			if z.lower() not in lowered:
				lowered.add(z.lower())
				ret.append(z)
		return ret

def get_column_names(conn):
	columns = {}
	for row in conn.execute('PRAGMA table_info(alib)'):
		columns[row[1].lower()] = row[1]
	return columns

def removeslash(x):
	while x.endswith('/'):
		return removeslash(x[:-1])
	return x

def issubfolder(parent, child, level=1):
	dirlevels = lambda a: len(a.split('/'))
	parent, child = removeslash(parent), removeslash(child)
	# unicode is native to strings in python 3, so eliminate redundant code

	# if isinstance(parent, unicode):
	# 	sep = unicode(os.path.sep)
	# else:
	# 	sep = os.path.sep

	# re-add add sep definition
	sep = os.path.sep

	if child.startswith(parent + sep) and dirlevels(parent) < dirlevels(child):
		return True
	return False

# def getfiles(files, subfolders = False):
# 	if isinstance(files, str):
# 		files = [files]

# 	isdir = os.path.isdir
# 	join = os.path.join

# 	temp = []

# 	if not subfolders:
# 		for f in files:
# 			if not isdir(f):
# 				yield f
# 			else:
# 				dirname, subs, fnames = os.walk(f).next()
# 				for fname in fnames:
# 					yield join(dirname, fname)
# 	else:
# 		for f in files:
# 			if not isdir(f):
# 				yield f
# 			else:                
# 				for dirname, subs, fnames in os.walk(f):
# 					for fname in fnames:
# 						yield join(dirname, fname)
# 					for sub in subs:
# 						for fname in getfiles(join(dirname, sub), subfolders):
# 							pass

#
# /OWN CODE BLOCK
#

def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)
        else:
            if entry.name.endswith('.flac') or entry.name.endswith('.ape')  or entry.name.endswith('.wv') or entry.name.endswith('.mp3'):
                """or entry.name.endswith('.ape')  or entry.name.endswith('.wv') or entry.name.endswith('.dsf') or entry.name.endswith('.mp3')"""
                yield entry
#
# OWN CODE BLOCK/
#

def execute(conn, sql, args=None):
	if args:
		try:
			log_args = (str(z) if not isinstance(z, str) else z for z in args)
			logging.debug(sql + u' ' + u';'.join(log_args))
		except:
			pass
		cursor = conn.execute(sql, args)
	else:
		logging.debug(sql)
		cursor =  conn.execute(sql)
	conn.commit()
	return cursor

def initdb(dbpath):
	conn = sqlite3.connect(dbpath)
	cursor = conn.cursor()
	execute(conn, '''CREATE TABLE IF NOT EXISTS alib (
	__path blob unique,
	__filename blob,
	__dirpath blob,
	__filename_no_ext blob,
	__ext blob,
	__accessed text,
	__app text,
	__bitrate text,
	__bitspersample text,
	__bitrate_num text,
	__frequency_num text,
	__frequency text,
	__channels text,
	__created text,
	__dirname text,
	__file_access_date text,
	__file_access_datetime text,
	__file_access_datetime_raw text,
	__file_create_date text,
	__file_create_datetime text,
	__file_create_datetime_raw text,
	__file_mod_date text,
	__file_mod_datetime text,
	__file_mod_datetime_raw text,
	__file_size text,
	__file_size_bytes text,
	__file_size_kb text,
	__file_size_mb text,
	__filetype text,
	__image_mimetype text,
	__image_type text,
	__layer text,
	__length text,
	__length_seconds text,
	__mode text,
	__modified text,
	__num_images text,
	__parent_dir text,
	__size text,
	__tag text,
	__tag_read text,
	__version text,
	__vendorstring text,
	__md5sig text,
	sqlmodded text,
	reflac text,
	discnumber text,
	track text,
	title text,	
	subtitle text,
	work text,
	part text,
	live text,
	composer text,
	arranger text,
	lyricist text,
	writer text,
	artist text,
	ensemble text,
	performer text,
	personnel text,
	conductor text,
	engineer text,
	producer text,
	mixer text,
	remixer text,	
	albumartist text,
	discsubtitle text,
	album text,
	version text,
	_releasecomment text,	
	releasetype text,
	year text,
	originaldate text,
	originalreleasedate text,
	originalyear text,
	genre text,
	style text,
	mood text,
	theme text,
	rating text,
	compilation text,
	bootleg text,
	label text,
	amgtagged text,
	amg_album_id text,
	amg_boxset_url text,
	amg_url text,
	musicbrainz_albumartistid text,
	musicbrainz_albumid text,
	musicbrainz_artistid text,
	musicbrainz_discid text,
	musicbrainz_releasegroupid text,
	musicbrainz_releasetrackid text,
	musicbrainz_trackid text,
	musicbrainz_workid text,
	lyrics text,
	unsyncedlyrics text,
	performancedate text,
	acousticbrainz_mood text,
	acoustid_fingerprint text,
	acoustid_id text,
	analysis text,
	asin text,
	barcode text,
	catalog text,
	catalognumber text,
	country text,
	discogs_artist_url text,
	discogs_release_url text,
	fingerprint text,
	isrc text,
	recordinglocation text,
	recordingstartdate text,
	replaygain_album_gain text,
	replaygain_album_peak text,
	replaygain_track_gain text,
	replaygain_track_peak text,
	review text,
	roonalbumtag text,
	roonid text,
	roonradioban text,
	roontracktag text,
	upc text,
	__albumgain text)''')

	conn.commit()
	return conn

def import_tag(tag, conn, columns):
	keys = {}
	values = {}
	for key, value in tag.items():
		# print(key, value)

		if key == '__path':
			value = tag.filepath
		else:

			if key == 'artist' or key == 'performer' or key == 'composer' or key == 'genre'  or key == 'personnel' or key == 'producer' or key == 'style' or key == 'mood' or key == 'theme':
				value = sort_field(remove_dupes(value))

			if not isinstance(value, (int, float, str)):

			# 	value = unicode(value)
			# elif not isinstance(value, str):
				value = u"\\\\".join(value)

		# try:
		# 	key.decode('ascii')
		# except UnicodeEncodeError:
		# 	logging.warning('Invalid tag found %s: %s. Not parsing field.' % (tag.filepath, key))
		# 	continue
		keys[key.lower()] = key
		values[key.lower()] = value

	if set(keys).difference(columns):
		columns = update_db_columns(conn, keys)
	
	keys = sorted(keys)
	values = [values[key] for key in keys]
	placeholder = u','.join(u'?' for z in values)
	keys = ['"%s"' % key for key in keys]
	insert = u"INSERT OR REPLACE INTO alib (%s) VALUES (%s)" % (u','.join(keys), placeholder)
	execute(conn, insert, values)
	return columns

def update_db_columns(conn, columns):
	new_columns = set(columns).difference(get_column_names(conn))
	for column in new_columns:
		logging.info(u'Creating %s column' % columns[column])
		execute(conn, u'ALTER TABLE alib ADD COLUMN "%s" text' % columns[column])
	conn.commit()
	return get_column_names(conn)
	
def import_dir(dbpath, dirpath):
	conn = initdb(dbpath)
	cursor = execute(conn, 'SELECT * from alib')
	columns = get_column_names(conn)


	""" at some point you need to modify the call to scantree to accept the passed parameter - until you do it needs to be started in the directory you want to import """
	for filepath in scantree('.'):	

			try:
				logging.info("Import started: " + filepath.path)
				tag = audioinfo.Tag(filepath.path)
				# print(tag)
				# input()
			except (Exception, e):
				logging.error("Could not import file: " + filepath.path)
				logging.exception(e)
			else:
				if tag is not None:
					try:
						columns = import_tag(tag, conn, columns)
						# print(type(filepath))
						# input()
						logging.info('Imported completed: ' + str(filepath))
					except Exception as e:
						logging.error('Error occured importing file %s' % filepath)
						logging.exception(e)
						raise
				else:
					logging.warning('Invalid file: ' + filepath)

	logging.info('Import completed')

# def clean_value_for_export(value):
# 	if not value:
# 		return value
# 	if isinstance(value, memoryview):
# 		return str(value)
# 	elif isinstance(value, str):
# 		return value
# 	elif u'\\\\' in value:
# 		return sort_field(remove_dupes(filter(None, value.split(u'\\\\'))))
# 	else:
# 		return value

def clean_value_for_export(value):
    if not value:
        return value
    if isinstance(value, memoryview):
        return str(value)
    elif '\\' in value:
        return sort_field(remove_dupes(filter(None, value.split('\\'))))
    elif isinstance(value, str):
        return value
    else:
        return value


def export_db(dbpath, dirpath):
	conn = sqlite3.connect(dbpath)
	fields = get_column_names(conn)
	cursor = execute(conn, 'SELECT %s from alib' % ",".join('"%s"' % f for f in fields))
	for values in cursor:
		values = map(clean_value_for_export, values)
		new_tag = dict((k,v) for k,v in zip(fields, values))
		filepath = new_tag['__path']
		new_values = dict(z for z in new_tag.items() if not z[0].startswith('__'))

		if not issubfolder(dirpath, filepath):
			logging.info('Skipped %s. Not in dirpath.' % filepath)
			continue
		try:
			logging.info('Updating %s' % filepath)
			tag = audioinfo.Tag(filepath)
		except (Exception, e):
			logging.exception(e)
		else:
			logging.debug(new_values)
			
			for key, value in new_values.items():
				if not value and key in tag:
					del(tag[key])
				else:
					tag[key] = value

			try:
				tag.save()
				audioinfo.setmodtime(tag.filepath, tag.accessed, tag.modified)
				logging.info('Updated tag to %s' % filepath)
			except (Exception, e):
				logging.error('Could not save tag to %s' % filepath)
				logging.exception(e)
				
	logging.info('Export complete')

def parse_args():
	parser = argparse.ArgumentParser(description='Import/Save files to sqlite database.')
	parser.add_argument('action', choices=['import', 'export'],
						help='Action to perform. Either import or export')
	parser.add_argument('dbpath', type=str,
						help='Path to sqlite database.')
	parser.add_argument('musicdir', 
				   help='path to musicdir used for import/export')
	parser.add_argument('--log', 
						help='Log level. Can be DEBUG, INFO, WARNING, ERROR. All output is printed to console.', required=False)
	
	args = parser.parse_args()
	if args.log:
		logging.basicConfig(level=args.log.upper())

	dbpath = os.path.realpath(args.dbpath)
	musicdir = os.path.realpath(args.musicdir)
	if args.action == 'import':
		import_dir(dbpath, musicdir)
	else:
		export_db(dbpath, musicdir)
		
	
if __name__ == '__main__':
	parse_args()
