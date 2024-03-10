# Introduction

tagminder is comprised of two Python scripts.  One imports audio metadata from underlying audio files into a dynamically created SQLite database and can export metadata back to the underlying files.  The other processes the imported metadata in order to correct anomalies and enrich the metadata where possible.

It enables you to affect mass updates / changes using SQL and ultimately write those changes back to the underlying files. It leverages the [puddletag](https://github.com/puddletag/puddletag) codebase to read/write tags so you need to either install puddletag, or at least pull it from the git repo to be able to access its code, specifically puddletag/puddlestuff/audioinfo. 

Tags are read/written using the Mutagen library as used in puddletag. Requires Python 3.x.

## General philosophy and rationale

### Make sense from chaos

I have a relatively large music collection and rely on good metadata to enhance my ability to explore my music collection in useful and interesting ways. 

Taggers are great but can only take you so far. Tag sources also vary in consistency and quality, often times including issues like adding 'feat. artist' entries to track titles or artist and performer tags.  This makes it more difficult for a music server to correctly identify performers and identify a track as a performance of a particular song, and thus include the performance alongside other performances of the same song.  It also means 'feat. xx ' type entries don't give rise to metadata in a form that can be used.

tagminder lets you automatically address these sorts of issues and does a lot of cleanup work that is difficult to do at scale or consistently using a tagger. It does it at scale, and delivers consistent and repeatable results, whether you're handling 1,000 or 1,000,000 tracks; this is simply an impossible task prone to variation and human error when tackled via a tagger.

### Preserves your prior work
tagminder takes your existing tags as a given, not trying to second guess you by replacing your metadata with externally sourced metadata, but rather it looks for common issues in your metadata and solves those automatically.  It also leverages existing metadata related to tracks and artists already in your library to enrich albums without for e.g. composer entries or entries without any genre metadata.

### MusicBrainz aware

Music servers are increasingly leveraging MusicBrainz MBIDs when present.  tagminder seeks to add MusicBrainz MBIDs to your metadata where MBIDs are already available in your existing metadata e.g. if one performance by an artist happens to have a MBID included in its metadata, tagminder will replicate that MBID in every other performance that contains the same performer name. 

To do this, it builds a table of distinct artist/performer/composer and albumartist names that have an associated MBID in your tags and then replicates that MBID to all occurences of that artist/performer/composer in your tag metadata. If your music server is MusicBrainz aware, there's a good chance adding MBID's to your tags will prevent it from merging the work of unrelated artists in your music collection that share the same name.

If you happen to have namesakes within your metadata (i.e. same artist/performer/composer name but with different MBIDs) these artist/performer/composer MBIDs will not be replicated as there would be no way for tagminder to know which MBID to apply. After running tagminder look for namesakes_* tables in the database - any records therein represent artists/performers/composers requiring manual disambiguation by adding the appropriate MBID to matching artist/performer/composer records in the alib table.

### Leaves your files untouched unless you explicitly choose to export changes

tagminder writes changes to a database table and logs which tracks have had metadata changes. It does not make changes to your files unless you explicitly invoke tags2db.py using its export option. All tables in the database can be viewed and edited using a SQLite database editor like [Sqlitestudio](https://github.com/pawelsalawa/sqlitestudio) or [DB Browser for SQLite](https://github.com/sqlitebrowser/sqlitebrowser).  This enables you to browse your metadata and inspect tags to see exactly what would be written to files if you chose to export your changes to the underlying files.

In addition to running the automated changes you're also able to manually edit any records using the aforementioned database editors to further enhance/correct metadata issues manually, or code and run your own SQL queries if you're so inclined.

### Backing out changes is easy

All originally-ingested records are written to a rollback table, so in the event you've made changes to your metadata you don't like, you can simply reinstate your old tags by exporting from the rollback table.

### Reducing the need for incremental file backups

If your music collection is static in terms of filename and location, you can also use the metadata database as a means of backing up and versioning metadata simply by keeping various iterations of the database.  This obviates the need to overwrite a previous backup of the underlying music files, reducing storage needs, backup times and complexity.

Getting metadata current after restoring a dated backup of your music files is as simple as exporting the most recent database against the restored files. The added benefit is it eliminates the need to create incremental backups of your music files simply because you've augmented the metadata - just backup the database and as long as your file locations remain static you have everything you need - the audio files and their metadata.

By default tagminder generates a gen4 uuid for all files, which would be written to your tags on exporting changes.  A future update will remove dependency on static filenames and locations by instead referencing the UUID to ascertain which files to write to on exporting changes from the database.  The UUID would then be referenced rather than file path.  This would have the effect of making your metadata impervious to file move and rename operations.

## Understanding the scripts

### tags2db.py

Handles the import and export from/to the underlying files and SQLite database. It is the means of getting your tags in and out of your underlying audio files. 

This is where the puddletag dependency originates. I've modified Keith's (puddletag's original author) Python 2.x tags to database code to run under Python 3. To get it to work, all that's required is that you pull a copy of [puddletag source](https://github.com/puddletag/puddletag) then copy tags2db.py into the puddletag root folder so that it has access to puddletag's code library. 

You do not need a functioning puddletag with all dependencies installed to be able to use tags2db.py, albeit in time you might find puddletag handy for some cleansing/ editing that's best left to human intervention.

### tagminder.py

Does the heavy lifting where metadata is concerned, handling the cleanup of tags in the SQL table 'alib'. A SQL trigger flags any changed records, whether they're changed by way of a SQL update or a manual edit (the trigger field 'sqlmodded' is incremented every time a tag value in a record is updated).

This enables tagminder to generate a database 'export.db' containing only changed records, enabling you to write changes only to those files that have had their metadata modified by tagminder.
As a bonus tagminder creates a text file called affected_files.csv every time it is run, listing the individual files that have been upated.
A user executed bash shell script addsec2modtime.sh reads that file and adds 1 second to the last modified date of every file listed therein.  This ensures that any update scan by a music server (whether batch or real-time) is able to detect that the underlying files that need rescanning as opposed to rescanning all directories containing music.



At present it does the following:

#### General tag cleanup

- strips all spurious tags from the database so that your files only contain the sanctioned tags listed in tagminder.py (you can obviously modify to suit your needs)

- trims all text fields to remove leading and trailing spaces

- removes all spurious CR/LF occurrences in text tags. It does not process the LYRICS or REVIEW tags

-  replaces all grave accent apostrophes with ASCII and ISO 8859 compliant apostrophe: "  '  "

- removes PERFORMER tags where they match or are already present in the ARTIST tag

- sorts and eliminates duplicate entries in tags

#### Tag standardisation

- merges ALBUM and VERSION tags into ALBUM tag to get around Logitechmediaserver (LMS), Navidrome and other music servers merging different versions of an album into a single album.  VERSION is left intact making it simple to reverse with an UPDATE query

- adds [bit depth/sampling rate kHz], [Mixed Res] or [DSD] to end of all album names where an album is not redbook (16/44.1).  This is because very few music servers differentiate different releases properly if they share exactly the same name, and the dev's typically don't see getting this right as a priority, and if they do they completely overengineer their solution rather than use tags

- sets COMPILATION = 1 for all Various Artists albums and 0 for all others. Tests for presence or otherwise of ALBUMARTIST and whether __dirname of album begins with ‘VA -’  to make its determination.  Does the same for all albums where the __dirname begins with ‘OST - ’.

- removes 'Various Artists' from ALBUMARTIST

- writes out multiple TAGNAME=value entries rather than TAGNAME=value1\\value2 delimited tag entries, and in doing so respects the underlying file type's tagging 'specification' (I use the term loosely).

- normalises RELEASETYPE entries for using `First Letter Caps` for better presentation in music server front-ends that leverage RELEASETYPE (Support for RELEASETYPE this was recently added to Logitechmediaserver massively improving its ability to list an artist's work in a meanigful manner)

- adds MusicBrainz identifiers to artists and albumartists leveraging what already exists in your file tags or where a master table of MBID's exists it leverages that. Where a performer name is associated with > 1 MBID in your tags these performers are ignored so as not to conflate performers.  Check tables namesakes_* for contributors requiring manual disambiguation

#### Handling of ‘Live’ in album names and track titles

- removes all instances and variations of Live entries from track titles and moves or appends that to the SUBTITLE tag as appropriate and ensures that the LIVE tag is set to 1 where this is not already the case.  It does not corrupt track names where the word ‘Live’ is part of a song title

- removes (live) from end of all album names, sets LIVE = '1' where it's not already set to '1' and appends (Live) to SUBTITLE tag where this is not already the case

- ensures LIVE tag is set to 1 for all Live performances where [(live...)] and its many variations appears in TITLE or SUBTITLE tags

#### Handling of Feat. in track title and artist tags

- removes most instances and variations of Feat. entries from ARTIST and TITLE tags and appends the delimited performer names to the ARTIST tag

#### Identifying duplicated FLAC audio content

- identifies all duplicated albums based on records in the alib table. The code assumes every folder contains an album and relies on the md5sum embedded in properly-encoded FLAC files. – It basically creates a concatenated string from the sorted md5sum of all tracks in a folder and compares that against the same for all other folders. If the strings match you have a 100% match of the audio stream and thus a duplicate album, irrespective of what tags / metadata might tell you. You can confidently remove all but one of the matched folders.
- If any FLAC files are missing the md5sum or the md5sum is zero then a table is created listing all folders containing FLAC files that should be reprocessed by the official FLAC encoder using ```flac -f -8 --verify *.flac```.  Be careful not to delete duplicates where the concatenated md5sum is a bunch of zeroes or otherwise empty - re-encode these files and re-run tagminder.

## TODO: 
Refer issues list, filter on enhancements.

## USAGE:

I strongly suggest writing the SQLite database to /tmp as its 'alib' table is dynamically modified every time a new tag is encountered when tags are being imported from audio files. 

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
