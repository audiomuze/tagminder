**tags2sqlite**:  is a collection of Python scripts to import audio metadata from underlying audio files into a dynamically created SQLite database, allowing you to affect mass updates / changes using SQL and ultimately write those changes back to the underlying files.  It leverages the puddletag codebase so you need to install puddletag to be able to access its code, specifically puddletag/puddlestuff/audioinfo.  Tags are read/written using the Mutagen library as used in Puddletag. Requires Python 3.x



**tagfromdb3.py** handles the import and export from/to the underlying files and SQLite database.  Basically it is the means of getting your tags in and out of your underlying audio files.  This is where the puddletag depedency originates.  I've modded Keith's Python 2.x code to run under Python 3.  To get it to work all that's required is that you pull a copy of puddletag source from github: https://github.com/puddletag/puddletag, then copy tagfromdb3.py into the puddletag root folder.  You do not need a functioning puddletag to be able to use tagfromdb3.py, albeit in time you might find puddletag handy for some cleansing/ editing that's best left to human intervention.

**dropbannedtags.py** does the heavy lifting handling the cleanup of tags in the SQL table "alib".  A trigger is used to be able to isolate and write back tags only to files who's tag record has been modified (the trigger field sqlmodded is incremented every time the record is updated)
At present it does the following:
- strips all spurious tags from the database so that your files only contain the sanctioned tags listed in dropbannedtags.py (you can obviously modify to suit your needs).
- trims all text fields to remove leading and trailing spaces
- removes all spurious CR/LF occurences in text tags.  It does not process the ```LYRICS``` or ```REVIEW``` tags
- removes all upper and lowercase (Live in...), (Live at...), [Live in...], [Live at...] entries from track titles and moves or appends that to the ```SUBTITLE``` tag as appropriate
- splits all instances of upper and lowercase (Feat , (Feat. , [Feat , [Feat. entries from track titles and appends the performer names to the ```ARTIST``` tag
- merges ```ALBUM``` and ```VERSION``` tags into ```ALBUM``` tag to get around Logitechmediaserver (LMS), Navidrome and other music servers merging different versions of an album into a single album.  ```VERSION``` is left intact making it simle to reverse with an UPDATE query
- removes ```PERFORMER``` tags where they match the ARTIST tag
- sets ```COMPILATION``` = '1' for all Various Artists albums and to '0' for all others.  Tests for presence or otherwise of ```ALBUMARTIST``` and whether ```__dirname``` of album begins with 'VA - ' to make its deterimation
- ensures ```LIVE``` tag is set to 1 for all Live performances where ```[(live)]``` appears in ```TITLE``` or ```SUBTITLE``` tags
- removes 'Various Artists' as ```ALBUMARTIST```
- writes out multiple ```TAGNAME=value``` rather than ```TAGNAME=value1\\value2``` delimited tag entries
- Normalises ```RELEASETYPE``` entries for using First Letter Caps for better presentation in music server front-ends that leverage it
- identifies all duplicated albums based on records in the alib table.  The code relies on the md5sum embedded in properly encoded FLAC files - it basically takes them, creates a concatenated string
    from the sorted md5sum of all tracks in a folder and compares that against the same for all other folders.  If the strings match you have a 100% match of the audio stream and thus duplicate album, irrespective of tags / metadata.  You can condifently remove all but one of the matched folders.

At present must be started in root of tree you intend to import.
I strongly suggest writing the SQLite database to ```/tmp``` as it's alib table is dynamically modified every time a new tag is encounted in a file being imported.  It'll work on physical disk, but it'll take longer.  It'll also trigger a lot of writes whilst ingesting metadata and dynamically altering the table to ingest new tags, so you probably want to avoid hammering a SSD by having the database import directly to a SSD drive.

TODO:
- eliminate duplicate entries in tags
- incorporate metadata normalisation routines to standardise track ```TITLE```, ```PERFORMER```, ```COMPOSER``` & ```LABEL``` metadata
- leverage cosine similarity to generate potential duplicate in contributor metadata requiring manual intervention 
- add Musicbrainz identifiers to all ```ARTIST```, ```PERFORMER```, ```COMPOSER```, ```LYRICIST```, ```WRITER```, ```LABEL```, ```WORK```, ```PART``` and ```ALBUMARTIST``` tags
- incorporate metadata enrichment leveraging Musicbrainz and inferences based on existing track related metadata in table
- cleanup and standardise genres to eliminate unsanctioned ```GENRE``` entries
- ensure standardisation of various tags across all tracks in a folder/album e.g. all tracks have ```DATE``` and ```GENRE``` assignments and that they're the same
- merge ```GENRE``` and ```STYLE``` tags to ```GENRE``` tag and dedupe both
- enrich "Pop/Rock", "Jazz" & "Classical" only genre assignments with artist based ```GENRE``` and ```STYLE``` entries
- write out __dirpaths for various queries to additonal tables users can use to focus on manual adjustments e.g. adding ```DATE``` tag to albums without dates
- remove (live) from end of all album names, set ```LIVE``` = '1' where it's not already set to '1'
- 
