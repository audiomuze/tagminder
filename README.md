**tags2sqlite**:  is a collection of Python scripts to import audio metadata from underlying audio files into a dynamically created SQLite database, allowing you to affect mass updates / changes using SQL and ultimately write those changes back to the underlying files.  It leverages the puddletag codebase so you need to install pudletag to be able to access its code, specifically audioinfo().  Tags are read/written using the Mutagen library as used in Puddletag. Requires Python 3.x



**tagfromdb3.py** handles the import and export from/to the underlying files and SQLite database.  Basically it is the means of getting your tags in and out of your underlying audio files.  This is where the puddletag depedency originates.  Currently all I've done here is modded the Python 2.x code to run under Python 3.

**dropbannedtags.py** does the heavy lifting handling the cleanup of tags in the SQL table "alib".
At present it does the following:
- strip all spurious tags from the database so that your files only contain the tags listed in dropbannedtags.py
- trim all text fields to remove leading and trailing spaces
- remove all spurious CR/LF occurences in text tags.  It does not process the LYRICS or REVIEW tags
- removes all upper and lowercase (Live in...), (Live at...), [Live in...], [Live at...] entries from track titles and moves that to the ```SUBTITLE``` tag where ```SUBTITLE``` is otherwise empty
- splits all instances of upper and lowercase (Feat , (Feat. , [Feat , [Feat. entries from track titles and appends the performer names to the ```ARTIST``` TAG
- merges ```ALBUM``` and ```VERSION``` tags into ```ALBUM``` tag to get around LMS and Navidrome merging different versions of an album into a single album.  ```VERSION``` is left intact making it simle to reverse with an UPDATE query
- removes ```PERFORMER``` tags where they match the ARTIST tag

At present must be started in root of tree you intend to import.  I strongly suggest writing db to /tmp as it's dynamically modified every time a new tag is encounted in a file being imported.

TODO:
- incorporate metadata normalisation routines to standardise performer and composer names
- add Musicbrainz identifiers to all ```ARTIST```, ```PERFORMER```, ```COMPOSER```, ```LYRICIST```, ```WRITER```, LABEL and ```ALBUMARTIST``` tags
- incorporate metadata enrichment leveraging Musicbrainz
