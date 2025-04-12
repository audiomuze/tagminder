# Introduction
Tagging music has (for me) always been a time consuming, mind-numbing, prone to human error and inconsistency, and laborious task.  Tagminder has reduced it to something that is now almost entirely automated, ensuring consistency throughout your digital music library.


tagminder is comprised of two Python scripts.  One imports audio metadata from underlying audio files into a dynamically created SQLite database and can export metadata back to the underlying files (but only if you specifically ask it to).  The other processes the imported metadata in the SQLite database in order to correct anomalies, improve consistency and enrich the metadata where possible.

It enables you to affect mass updates / changes using SQL and ultimately write those changes back to the underlying files. It leverages the [puddletag](https://github.com/puddletag/puddletag) codebase to read/write tags so you need to either install puddletag, or at least pull it from the git repo to be able to access its code, specifically puddletag/puddlestuff/audioinfo. 

Tags are read/written using the Mutagen library as used in puddletag. Requires Python 3.x. and the following Python libraries:

```
from collections import OrderedDict
import csv
import os
from os.path import exists, dirname
import re
import sqlite3
import sys
import time
import uuid
import pandas as pd
import numpy as np
from string_grouper import match_strings, match_most_similar, \
    group_similar_strings, compute_pairwise_similarities, \
    StringGrouper
```

Furthermore, it's a great tool to use when adding new music to your music collection and wanting to ensure consistency in tag treatment, contributor names, MBIDs, file naming and directory naming.  It's now at a point functionally where I tag using Picard (to pull MusicBrainz metadata), then run tagminder against the files, export its changes sight unseen and do a final review using Puddletag to pick up the few edge cases that aren't worth otherwise coding to automate.

## General philosophy and rationale

### Make sense from chaos

I have a relatively large music collection and rely on good metadata to enhance my ability to explore and listen to my music collection in useful and interesting ways. 

Taggers are great but can only take you so far. Tag sources also vary in consistency and quality, often times including issues like adding 'feat. artist' entries to track titles or artist and performer tags.  This makes it more difficult for a music server to correctly identify performers and identify a track as a performance of a particular song, and thus include the performance alongside other performances of the same song.  It also means 'feat. xx ' type entries don't give rise to metadata in a form that can be used to browse your library.

tagminder lets you automatically address these sorts of issues and does a lot of cleanup work that is difficult to do at scale or consistently using a tagger. It delivers consistent and repeatable results, whether you're handling 1,000 or 1,000,000 tracks; this is simply an impossible task prone to inconsistency and human error when tackled via a tagger.

### Preserve your prior work
tagminder takes your existing tags as a given, not trying to second guess you by replacing your metadata with externally sourced metadata, but rather it looks for common issues in your metadata and solves those automatically where it is possible to get a reliable result.  It also leverages existing metadata related to tracks and contributors already in your library to enrich albums without e.g. composer metadata or tracks/albums without any genre metadata.

### MusicBrainz aware

Music servers are increasingly leveraging MusicBrainz MBIDs when present.  tagminder seeks to add MusicBrainz MBIDs to your metadata where MBIDs are already available in your existing metadata e.g. if one performance by an artist happens to have a MBID included in its metadata, tagminder will replicate that MBID in every other performance that contains the same performer name. 

To do this, it builds a table of distinct artist/performer/composer and albumartist names that have an associated MBID in your tags and then replicates that MBID to all occurences of that artist/performer/composer in your tag metadata. If your music server is MusicBrainz aware, there's a good chance adding MBID's to your tags will prevent it from merging the work of unrelated artists in your music collection that share the same name.

If you happen to have namesakes within your metadata (i.e. same artist/performer/composer name but with different MBIDs) these artist/performer/composer MBIDs will not be replicated as there would be no way for tagminder to know which MBID to apply. After running tagminder look for _REF_namesakes_* tables in the database - any records therein represent contributors requiring manual disambiguation by adding the appropriate MBID to the matching records in the alib table.

It can also go one step further, leveraging a dump of musicbrainz contributors and MBIDs against which to validate and/or add MBID metadata when the reference table _REF_mb_disambiguated is present.  This would be the most effective way to validate and add musicbrainz identifiers to your music without having to resort to retagging and risking metadata edits you've made being overwritten by a tagger.

If you want to create your own table by downloading from the MusicBrainz database dump, _REF_mb_disambiguated contains 3 fields:
mbid - the musicbrainz identifier
entity - the contributor name
lentity - lowercase representation of entity

### Leaves your files untouched unless you explicitly choose to export changes

tagminder writes changes to a database table and logs which tracks have had metadata changes. It does not make changes to your files unless you explicitly invoke tags2db.py using its export option. All tables in the database can be viewed and edited using a SQLite database editor like [SQLiteStudio](https://github.com/pawelsalawa/sqlitestudio) or [DB Browser for SQLite](https://github.com/sqlitebrowser/sqlitebrowser).  This enables you to browse your metadata and inspect tags to see exactly what would be written to files if you chose to export your changes to the underlying files.

In addition to running the automated changes you're also able to manually edit any records using the aforementioned database editors to further enhance/correct metadata issues manually, or code and run your own SQL queries if you're so inclined.

### Backing out changes is easy

All originally-ingested records are written to a rollback table, so in the event you've made changes to your metadata you don't like, you can simply reinstate your old tags by exporting from the rollback table.

### Reducing the need for incremental file backups

If your music collection is static in terms of filename and location, you can also use the metadata database as a means of backing up and versioning metadata simply by keeping various iterations of the database.  This obviates the need to overwrite a previous backup of the underlying music files, reducing storage needs, backup times and complexity.  

Getting metadata current after restoring a dated backup of your music files is as simple as exporting the most recent database against the restored files. The added benefit is it eliminates the need to create incremental backups of your music files simply because you've augmented the metadata - just backup the database and as long as your file locations remain static you have everything you need - the audio files and their metadata.

By default tagminder generates a gen4 uuid for all files, which would be added to your tags on exporting changes.  A future update will remove dependency on static filenames and locations by instead referencing the UUID to ascertain which files to write to on exporting changes from the database.  The UUID would then be referenced rather than file path.  This would have the effect of making your metadata impervious to file move and rename operations (the code has been written, I've just not had a chance to incorporate it - will do so as I work to refactor tagminder and carry out most operations in a Polars dataframe to enhance tagminder's speed and efficiency by leveraging vectorised operations.

## Understanding the scripts

### tags2db.py

Handles the import and export from/to the underlying files and SQLite database. It is the means of getting your tags in and out of your underlying audio files. 

This is where the puddletag dependency originates. I've modified Keith's (puddletag's original author) Python 2.x tags to database code to run under Python 3. To get it to work, all that's required is that you pull a copy of [puddletag source](https://github.com/puddletag/puddletag) then copy tags2db.py into the puddletag root folder so that it has access to puddletag's code library. 

You do not need a functioning puddletag with all dependencies installed to be able to use tags2db.py, albeit in time you might find puddletag handy for some cleansing/ editing that's best left to human intervention.

### tagminder.py (currently parading as ztm.py)

Does the heavy lifting where metadata is concerned, handling the cleanup of tags in the SQL table 'alib'. A SQL trigger flags any changed records, whether they're changed by way of a SQL update or a manual edit (the trigger field 'sqlmodded' is incremented every time a tag value in a record is updated).

This enables tagminder to generate a database 'export.db' containing only changed records, enabling you to write changes only to those files that have had their metadata modified in the database by tagminder or the user.  
As a bonus tagminder creates a text file called affected_files.csv every time it is run, listing the individual files that have been upated.
A user executed bash shell script addsec2modtime.sh reads that file and adds 1 second to the last modified date of every file listed therein.  This ensures that any update scan by a music server (whether batch or real-time) is able to detect the underlying files that need rescanning as opposed to rescanning all files in your collection.



At present, Tagminder's specific capabilities are as follows:

#### General tag cleanup

- strips all spurious tags from the database so that your files only contain the sanctioned tags listed in tagminder.py (you can obviously modify to suit your needs)

- trims all text fields to remove leading and trailing spaces

- removes all spurious CR/LF occurrences in text tags (you'll be surprised how many there are). It does not process the LYRICS or REVIEW tags.

-  replaces all grave accent apostrophes with ASCII and ISO 8859 compliant apostrophe: "  '  "

- removes PERFORMER tags where they match or are already present in the ARTIST tag

- sorts and eliminates duplicate tag values in tags

#### Tag standardisation

- merges ALBUM and VERSION tags into ALBUM tag to get around Logitechmediaserver (LMS), Navidrome and other music servers merging different versions of an album into a single album.  VERSION is left intact making it simple to reverse with an UPDATE query

- adds [bit depth/sampling rate kHz], [Mixed Res] or [DSD] to end of all album names where an album is not redbook (16/44.1).  This is because very few music servers differentiate different releases properly if they share exactly the same name, and the dev's typically don't see getting this right as a priority, and if they do they completely overengineer their solution rather than use tags

- sets COMPILATION = 1 for all Various Artists albums and 0 for all others. Tests for presence or otherwise of ALBUMARTIST and whether __dirname of album begins with ‘VA -’  to make its determination.  Does the same for all albums where the __dirname begins with ‘OST - ’ (denoting Orignal Sountrack).

- removes 'Various Artists' from ALBUMARTIST tag

- writes out multiple TAGNAME=value entries rather than TAGNAME=value1\\value2 delimited tag entries, and in doing so respects the underlying file type's tagging 'specification' (if one considers the bull that's been conjured over the years to be standards)

- normalises RELEASETYPE entries to `First Letter Caps` for better presentation in music server front-ends that leverage RELEASETYPE (support for RELEASETYPE was added to Logitechmediaserver massively improving its ability to list an artist's work in a meaningful manner rather than as one long unstructured list)

- adds MusicBrainz identifiers to contributors (artists, albumartists, composers, engineers and producers) leveraging what already exists in your file tags or where a master table of MBID's exists it leverages that. Where a contributor name is associated with > 1 MBID in your tags these contributors are ignored so as not to conflate contributors.  Check for tables _INF_namesakes_* for contributors requiring manual disambiguation and confirmation

- Makes albumartist, artist, composer, engineer, producer text case consistent with their representation in the MusicBrainz ecosystem.  If they don't exist in MusicBrainz it converts them to Firstlettercaps.  Can also replace the text case of artist names in _REF_mb_disambiguated with matching names found in table _REF_contributor_matched_on_allmusic.  So if you want to change the text case of a contributor throughout your collection, just add a record to _REF_contributor_matched_on_allmusic and populate the name in the text case of your choosing - records in _REF_mb_disambiguated are always updated to reflect the text case in _REF_contributor_matched_on_allmusic prior to being applied elsewhere.

- removes zero padding from discnumber and track tags

- Substantially implements [RYM Capitalisation](https://rateyourmusic.com/wiki/RYM:Capitalization) rules for English language insofar as is possible without resorting to leveraging a LLM to understand word context.

#### Handling of ‘Live’ in album names and track titles

- removes all instances and variations of Live entries from track titles and moves or appends that to the SUBTITLE tag as appropriate and ensures that the LIVE tag is set to 1 where this is not already the case.  It does not corrupt track names where the word ‘Live’ is part of a song title

- removes (live) from end of all album names, sets LIVE = '1' where it's not already set to '1' and appends (Live) to SUBTITLE tag where this is not already the case

- ensures LIVE tag is set to 1 for all Live performances where [(live...)] and its many variations appears in TITLE or SUBTITLE tags

#### Handling of Feat. in track title and artist tags

- removes most instances and variations of Feat. entries from ARTIST and TITLE tags and appends \\ delimited performer names to the ARTIST tag

#### Identifying duplicated FLAC audio content

- identifies all duplicated albums based on records in the alib table. The code assumes every folder contains an album and relies on the md5sum embedded in properly-encoded FLAC files. It basically creates a concatenated string from the sorted md5sum of all tracks in a folder and compares that against the same for all other folders. If the strings match you have a 100% match of the audio stream and thus a duplicate album, irrespective of what tags / metadata might tell you. You can confidently remove all but one of the matched folders.
- If any FLAC files are missing the md5sum or the md5sum is zero then a table is created listing all folders containing FLAC files that should be reprocessed by the official FLAC encoder using ```flac -f -8 --verify *.flac```.  Be careful not to delete duplicates where the concatenated md5sum is a bunch of zeroes or otherwise empty - re-encode these files and re-run tagminder.

#### Renaming of music files and directories based on tag metadata and file attributes

Renames audio files as follows:
- if compilation is set to 1: file renaming: 'discnumber-track - artist - title.ext' ; folder renaming: 'VA - album [release] [bit depth sample rate]'.
- if compilation is set to 0: file renaming: 'discnumber-track - title.ext' ; folder renaming: 'albumartist - album [release] [bit depth sample rate]'
In all instances [bit depth sample rate] are only included where an album is not redbook.

Files and directories are renamed in-situ rather than being moved elsewhere in directory tree.  This means all other files associated with an album remain in the renamed folders.

#### Normalising artist, albumartist, composer, engineer and producer names and getting them consistent throughout your collection.  (record labels to be incorporated in future).

Tagminder includes the capability to affect mass changes across hundreds of thousands of records almost instantaneously.  Music Servers typically employ database models that mean '10CC', '10cc', '10 cc' and '10cc.' are four different artists.  Tagminder includes a transformation function that enables you to transform all instances of names like 10CC, 10cc. and 10 cc to 10cc throughout your collection in a single operation, without having to write any code.  These transformation rules need only be captured once, and are then available for all future metadata ingestion, ensuring that your collection achieves a level of consistency that would otherwise be very difficult (if not impossible) to attain and maintain.

To aid in identifying variations of contributor names that may be the same artist (e.g. the 10cc examples above) tagminder uses string-grouper to compare all unique contributor names in your metatada and present these to you in a table showing possible matches with a condifence level.  All that's required from you is to insert 1 or 0 in the field indicating whether or not the name on the left should be replaced with the name on the right, making it trivial to populate the disambigation table used to drive normalisation of names throughout your music.  tagminder identifies names it thinks might represent the same contributor, then eliminates any you have previously confirmed are false-positives or require replacement by reference to matching names in _REF_disambiguation_workspace where false positives and replacement required are represented as (status=0/1)respectively.  The remaining names can be found in table _INF_string_grouper_possible_namesakes, for consideration by the user.

![image](https://github.com/user-attachments/assets/5bc42222-c8df-4ef0-b022-f86e29c4b369)

#### Identifying different versions of an album
If you're a music fanatic you may have multiple releases of the same album.  At some point your rational mind may get the better of you and you might want to get rid of a few versions that are substantially the same ... same track count, same dynamic range, same bit depth and sampling rate. Tagminder can point these out for you and auto-select some candidates for culling, leaving you with a table of versions to peruse and edit/override or accept versions it has selected as candidates for removal.  Tagminder will not flag a version as a candidate for removal if any of the following keywords are present in the directory name:

| audiophile label signifier |
| -- |
| afz|
| audio fidelity |
| compact classics |
| dcc |
| fim |
| gzs |
| mfsl |
| mobile fidelity |
| mofi |
| mastersound |
| sbm |
| xrcd |              

Whilst tagminder will never remove the versions for you, the table contains everything you need to be able to export the directory paths of those versions you're sure you want to let go of.  A bash script can then do the dirty work or you can work through it manually.  Versions can be found in the table _INF_versions.

#### Pointing out missing metdata and other useful information
Whilst assessing and improving your metadata consistency tagminder populates a number of tables along the way.  All tables that begin with _INF_ as a prefix contain data you may want to peruse because they point to metadata or library issues you may want to address.  The tables and their contents are described below:

| table name | purpose |
| - | - |
| _INF___dirpaths_with_FLACs_to_kill | list of duplicate albums you can delete, leaving behind only one copy |
| _INF___dirpaths_with_same_content | list of albums that are duplicated (as in every track has an identical audio stream as one or more other albums |
| _INF_albums_missing_artist | albums containing tracks without a track artist |
| _INF_albums_missing_tracknumbers | albums containing tracks without a track number |
| _INF_albums_with_duplicated_tracknumbers | albums containing tracks with a track number appearing > 1x |
| _INF_albums_with_nameless_tracks | albums containing tracks without a track title |
| _INF_albums_with_no_genre | albums with no genre tags |
| _INF_albums_with_no_year | albums with no year tags |
| _INF_missing_tracknumbers | missing track sequences by album |
| _INF_nonstandard_FLACS | FLAC files without the embedded md5 of the audio stream |
| _INF_string_grouper_possible_namesakes | possible namesakes for disambiguation or correction to ensure consistenctcy of contributor name |
| _INF_tracks_without_artist | tracks without an artist tag |
| _INF_tracks_without_title | tracks without a title tag |
| _INF_versions | albums where multiple versions are present in library.  killit == 'Investigate' means version has same key attributes as other versions.  killit == '1' means a higher DR version has been identified that is either same or higher sampling rate and bit depth |

## TODO: 
Refer issues list, filter on enhancements.  Refactor all code to leverage Polars DF wherever possible, leveraging vectorisation and significantly improving performance.

## USAGE:

I generally tag with Picard or another semi-automted metadata source, then run the lot though tagminder, then use Puddletag for fine tuning, then re-process the lot through tagminder to ensure I've not introduced any inconsistencies through manual tagging.

I strongly suggest writing the SQLite database to /tmp as its 'alib' table is dynamically modified every time a new tag is encountered when tags are being imported from audio files. (albeit the refactored code handles the entire import in memory and only writes the database at the end).

It'll work on physical disk, but it'll take longer. It'll also trigger a lot of writes whilst ingesting metadata and dynamically altering the table to ingest new tags, so you probably want to avoid hammering a SSD by ensuring that you're not writing the database directly to SSD. Use /tmp!

First import tags from your files into a nominated database:

```
python /path.to/puddletag/tags2db.py import /tmp/dbname.db /path/to/import/from
```
Let that run - it'll take a while to ingest tags from your library, writing each file's metadata to a table called 'alib'

Run tagminder.py against the same database:

```
python ~/tagminder.py /tmp/dbname.db
```

It'll report its workings and stats as it goes.

When it's done the resulting (changed records only) are written to 'export.db', which can be exported back to the underlying files like so:

```
python /path.to/puddletag/tags2db.py export /tmp/export.db /path/imported/from
```

This will overwrite the tags in the associated files, replacing it with the metadata tags stored in 'export.db'

### Workflow

- run it once against your entire music collection to process updates en-mass

- thereafter use tagminder to cleanup tags for any music you want to add to your music collection.  For me that means tagging via Picard followed by Puddletag (to leverage tag sources other than MusicBrainz, inspect tags, standardise filenames, rename folders etc.) and then running tagminder to pick up anything I may have overlooked.
