ATTACH DATABASE '/tmp/amg/qnap1.db' AS qnap1;
ATTACH DATABASE '/tmp/amg/qnap2.db' AS qnap2;
ATTACH DATABASE '/tmp/amg/amg.db' AS amg;



CREATE INDEX IF NOT EXISTS qnap1.path ON alib (
    __path
);

CREATE INDEX IF NOT EXISTS qnap2.path ON alib (
    __path
);

/* create target alib table and insert all records from qnap1.db and qnap2.db */

CREATE TABLE amg.alib AS SELECT *
                       FROM qnap1.alib
                      ORDER BY __path;

INSERT INTO amg.alib SELECT *
                   FROM qnap2.alib
                  ORDER BY __path;

CREATE TRIGGER IF NOT EXISTS amg.sqlmods
                       AFTER UPDATE
                          ON alib
                    FOR EACH ROW
                        WHEN old.sqlmodded IS NULL
BEGIN
    UPDATE alib
       SET sqlmodded = iif(sqlmodded IS NULL, '1', (CAST (sqlmodded AS INTEGER) + 1) )
     WHERE rowid = NEW.rowid;
END;


/* build indexes used by tagminder */
CREATE INDEX IF NOT EXISTS amg.albartists ON alib(albumartist) WHERE albumartist IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.albums ON alib(album) WHERE album IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.artists ON alib(artist) WHERE artist IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.composers ON alib(composer) WHERE composer IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.dirpaths_discnumbers ON alib(__dirpath,discnumber) WHERE discnumber IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.filepaths ON alib(__path);
CREATE INDEX IF NOT EXISTS amg.genres ON alib(genre) WHERE genre IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.lalbartists ON alib(LOWER(albumartist)) WHERE albumartist IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.lartists ON alib(LOWER(artist)) WHERE artist IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.lcomposers ON alib(LOWER(composer)) WHERE composer IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.lperformers ON alib(LOWER(performer)) WHERE performer IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.ltitles ON alib(LOWER(title)) WHERE title IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.performers ON alib(performer) WHERE performer IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.styles ON alib(style) WHERE style IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.titles ON alib(title) WHERE title IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.titles_subtitles ON alib(title, subtitle) WHERE title IS NOT NULL;
CREATE INDEX IF NOT EXISTS amg.writers ON alib(writer) WHERE writer IS NOT NULL;


