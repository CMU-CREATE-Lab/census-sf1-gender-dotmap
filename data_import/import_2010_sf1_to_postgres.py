# -*- coding: utf-8 -*-

"""Import Census summary files into their corresponding DB table."""

import glob
import os
import re
import cStringIO
import contextlib
import subprocess
import tempfile
import zipfile

import psycopg2

import db_settings

DATA_DIRECTORY = r'data/census2010/'


def main():
    # Connect to DB
    db_connection = get_db_connection()
    # Import geo and summary files
    import_summary_files_to_db(DATA_DIRECTORY, db_connection)
    # Close DB connection
    db_connection.commit()
    db_connection.close()


def get_db_connection():
    conn = psycopg2.connect(
        database=db_settings.DB,
        user=db_settings.USER,
        password=db_settings.PASSWORD,
        host=db_settings.HOST)
    return conn


def import_summary_files_to_db(base_directory, db_connection):
    # Import geo and summary files for every state
    for state_dir in glob.iglob(os.path.join(base_directory, '*/')):
        db_cursor = db_connection.cursor()
        try:
            import_state_sf(state_dir, db_cursor)
            db_connection.commit()
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            if db_connection:
                db_connection.rollback()


def import_state_sf(state_dir, db_cursor):
    state_zip_files = glob.glob(os.path.join(state_dir, '*.zip'))
    assert len(state_zip_files) == 1
    state_zip_file = state_zip_files[0]
    geofile_re = re.compile('[a-z]{2}geo2010[.]sf1')
    summary_file_re = re.compile('[a-z]{2}[0-9]+2010[.]sf1')
    with zipfile.ZipFile(state_zip_file, 'r') as state_zip:
        for filename in state_zip.namelist():
            if summary_file_re.match(filename):
                insert_sf_to_db(state_zip, filename, db_cursor)
            #elif geofile_re.match(filename):
            #    insert_geo_to_db(state_zip, filename, db_cursor)


def insert_sf_to_db(state_zip, filename, db_cursor):
    table_number = re.match(
            '[a-z]{2}000([0-9]{2})2010[.]sf1', filename).group(1)
    table_name = 'sf1_%s' % table_number
    print 'Copying data from %s to table %s' % (filename, table_name)
    with contextlib.closing(
            cStringIO.StringIO(state_zip.read(filename))) as sf_io:
        db_cursor.copy_from(sf_io, table_name, sep=',')


def insert_geo_to_db(state_zip, filename, db_cursor):
    geo_schema = 'sql/census2010_geo_schema.csv'
    in_temp_file = tempfile.NamedTemporaryFile()
    in_temp_file.write(state_zip.read(filename))
    out_temp_file = open('test.geo', 'w')#tempfile.NamedTemporaryFile()
    print 'Converting %s file to csv' % filename
    pipe = subprocess.check_call(
        ['in2csv', '-e', 'latin1', '-s', geo_schema, in_temp_file.name],
        stdout=out_temp_file)
    in_temp_file.close()
    table_name = re.match('[a-z]{2}(geo[0-9]{4})[.]sf1', filename).group(1)
    print 'Copying data from %s to table %s' % (filename, table_name)
    out_temp_file.close()
    with open('test.geo', 'rb') as infile:
        # Import data
        copy_command = "COPY %s FROM STDIN WITH CSV HEADER" % table_name
        db_cursor.copy_expert(copy_command, infile)

if __name__ == '__main__':
    main()
