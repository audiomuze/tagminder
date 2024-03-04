ATTACH DATABASE '/tmp/amg/master.db' AS master;
ATTACH DATABASE '/tmp/amg/slave.db' AS slave;
ATTACH DATABASE '/tmp/amg/slave_updates.db' AS slave_updates;



CREATE INDEX IF NOT EXISTS master.uuid ON alib (
    tagminder_uuid
);

CREATE INDEX IF NOT EXISTS slave.uuid ON alib (
    tagminder_uuid
);

/* ensure trigger is in place in slave to record incremental changes enabling the changed records to be identified and exported for updating underlying files and modding timestamps */

CREATE TRIGGER IF NOT EXISTS slave.sqlmods
                       AFTER UPDATE
                          ON alib
                    FOR EACH ROW
                        WHEN old.sqlmodded IS NULL
BEGIN
    UPDATE alib
       SET sqlmodded = iif(sqlmodded IS NULL, '1', (CAST (sqlmodded AS INTEGER) + 1) )
     WHERE rowid = NEW.rowid;
END;


/* get uuid's relating to changed records in master that also exist in slave and write out slave's matching uuid's and paths to a tmp table in slave */

CREATE TABLE slave.changes AS SELECT __path,
                                     __filename,
                                     __dirpath,
                                     __filename_no_ext,
                                     __ext,
                                     __accessed,
                                     __created,
                                     __dirname,
                                     __file_access_date,
                                     __file_access_datetime,
                                     __file_access_datetime_raw,
                                     __file_create_date,
                                     __file_create_datetime,
                                     __file_create_datetime_raw,
                                     __file_mod_date,
                                     __file_mod_datetime,
                                     __file_mod_datetime_raw,
                                     __modified,
                                     __parent_dir,
                                     tagminder_uuid
                                FROM slave.alib
                               WHERE slave.alib.tagminder_uuid IN (
                                         SELECT tagminder_uuid
                                           FROM master.alib
                                          WHERE sqlmodded IS NOT NULL
                                     );

CREATE INDEX IF NOT EXISTS slave.change ON changes (
    tagminder_uuid
);


/* now delete all matching records from slave */

WITH cte AS (
    SELECT DISTINCT tagminder_uuid
      FROM master.alib
     WHERE sqlmodded IS NOT NULL
)
DELETE FROM slave.alib
      WHERE alib.tagminder_uuid IN cte;

/* now add all changed records from master to slave where they were also present in slave */
INSERT INTO slave.alib SELECT *
                         FROM master.alib
                        WHERE tagminder_uuid IN (
                                  SELECT tagminder_uuid
                                    FROM slave.changes
                              );


/* now correct the __path and all related metadata for all affected records to that which originally pertained to the
 matching tagminder_uuid in slave */

UPDATE slave.alib
   SET __path = changes.__path,
       __filename = changes.__filename,
       __dirpath = changes.__dirpath,
       __filename_no_ext = changes.__filename_no_ext,
       __ext = changes.__ext,
       __accessed = changes.__accessed,
       __created = changes.__created,
       __dirname = changes.__dirname,
       __file_access_date = changes.__file_access_date,
       __file_access_datetime = changes.__file_access_datetime,
       __file_access_datetime_raw = changes.__file_access_datetime_raw,
       __file_create_date = changes.__file_create_date,
       __file_create_datetime = changes.__file_create_datetime,
       __file_create_datetime_raw = changes.__file_create_datetime_raw,
       __file_mod_date = changes.__file_mod_date,
       __file_mod_datetime = changes.__file_mod_datetime,
       __file_mod_datetime_raw = changes.__file_mod_datetime_raw,
       __modified = changes.__modified,
       __parent_dir = changes.__parent_dir
  FROM changes
 WHERE changes.tagminder_uuid = slave.alib.tagminder_uuid;

/* export changed records to an export database for user to write back to files from */

DROP TABLE IF EXISTS slave_updates.alib;
CREATE TABLE slave_updates.alib AS SELECT * FROM slave.alib WHERE sqlmodded IS NOT NULL ORDER BY __path;

/* reset sqlmodded in slave and in updates */
UPDATE slave.alib
   SET sqlmodded = NULL
 WHERE sqlmodded != NULL;

UPDATE slave_updates.alib
   SET sqlmodded = NULL;

/*DROP TABLE slave.changes; */
VACUUM;
