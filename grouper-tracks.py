import sqlite3
import pandas as pd
import numpy as np
from string_grouper import match_strings, match_most_similar, \
    group_similar_strings, compute_pairwise_similarities, \
    StringGrouper


pd.set_option('display.max_rows', None)


#Connect to db and define table to leverage
db = sqlite3.connect('/tmp/amg/allmusic-wip.db')
table = 'sg_tracks'

# Import all entries in table to a dataframe

df = pd.read_sql_query(f'SELECT rowid, title, FALSE FROM {table};', db) # import artists, performers, albumartists and composers

matches = match_strings(df['title'], min_similarity=0.98)
# Look at only the non-exact matches:
similarities = matches[matches['left_title'] != matches['right_title']].head(None)
similarities = matches[matches['similarity'] < 0.99999]
similarities.to_csv('similar_tracks.csv', index=False, sep = '|')


'''
DROP TABLE IF EXISTS ct;

DROP TABLE IF EXISTS string_grouper;

CREATE TABLE ct AS SELECT DISTINCT artist
                     FROM alib
                    WHERE artist IS NOT NULL AND 
                          artist NOT LIKE '%\\%' AND 
                          artist NOT LIKE '%, %'
                    ORDER BY artist;


INSERT INTO ct (
                   artist
               )
               SELECT albumartist
                 FROM alib
                WHERE albumartist IS NOT NULL AND 
                      albumartist NOT LIKE '%\\%' AND 
                          albumartist NOT LIKE '%, %'
                ORDER BY albumartist;
                
INSERT INTO ct (
                   artist
               )
               SELECT performer
                 FROM alib
                WHERE performer IS NOT NULL AND 
                      performer NOT LIKE '%\\%' AND 
                          performer NOT LIKE '%, %'
                ORDER BY performer;

INSERT INTO ct (
                   artist
               )
               SELECT composer
                 FROM alib
                WHERE composer IS NOT NULL AND 
                      composer NOT LIKE '%\\%' AND 
                          composer NOT LIKE '%, %'
                ORDER BY composer;

CREATE TABLE string_grouper AS SELECT DISTINCT artist
                                 FROM ct
                                ORDER BY artist;

'''
