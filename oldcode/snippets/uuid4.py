import sqlite3
import uuid


def add_file_uuid():
    ''' this uuid v4 is to be an immutable once generated.  The idea behind it is every file will have an UUID added once which makes future tagging updates impervious to file name and/or location changes because updates will be based on
    UUID rather than __path.
    The tag export routine will need to be modified to leverage uuid rather than __path - this is still to come'''
    print(f"\nAdding a file UUID tag where none exists - this makes future tagging operations impervious to file location")
    
    # Get all records lacking a __file_uuid tag entry
    dbcursor.execute('''SELECT rowid, __dirname, __filename FROM alib WHERE tagminder_uuid IS NULL or trim(tagminder_uuid) = '' order by __path;''')
    records_to_process = dbcursor.fetchall()

    ''' now process each in sequence '''
    for record in records_to_process:

        row_id = record[0] # get rowid
        row_dirname = record[1] # get __dirname
        row_filename = record[2] # get filename

        # generate a uuid v4 value
        uuidval = str(uuid.uuid4())
        print(f'Adding UUID {uuidval} to {record[1]}/{record[2]}')
        # Run an update query adding a uuid v4 to every record that doesn't have one already        
        dbcursor.execute('''UPDATE alib set tagminder_uuid = (?) WHERE rowid = (?);''', (uuidval, record[0]))


conn = sqlite3.connect('/tmp/flacs/x.db')
dbcursor = conn.cursor()
add_file_uuid()
conn.commit()
dbcursor = conn.close()
