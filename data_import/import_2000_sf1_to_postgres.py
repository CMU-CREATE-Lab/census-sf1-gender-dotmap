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

DATA_DIRECTORY = r'data/census2000/'


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
    ZIP_FILE_PER_STATE = 40
    state_zip_files = glob.glob(os.path.join(state_dir, '*.zip'))
    assert len(state_zip_files) == 40
    for state_zip_file in state_zip_files:
        state_zip_filename = os.path.basename(state_zip_file)
        geofile_re = re.compile('[a-z]{2}geo_uf1[.]zip')
        summary_file_re = re.compile('[a-z]{2}[0-9]{5}_uf1[.]zip')
        if summary_file_re.match(state_zip_filename):
            insert_sf_to_db(state_zip_file, db_cursor)
        elif geofile_re.match(state_zip_filename):
            pass
        #insert_geo_to_db(state_zip_file, db_cursor)


def insert_sf_to_db(sf_zip_file, db_cursor):
    zip_filename = os.path.basename(sf_zip_file)
    table_number = re.match(
            '[a-z]{2}000([0-9]{2})_uf1[.]zip', zip_filename).group(1)
    table_name = 'sf1_2000_%s' % table_number
    print 'Copying data from %s to table %s' % (zip_filename, table_name)
    with zipfile.ZipFile(sf_zip_file, 'r') as sf_zip:
        sf_filename = zip_filename.rstrip('.zip').replace('_', '.')
        with contextlib.closing(
                cStringIO.StringIO(sf_zip.read(sf_filename))) as sf_io:
            db_cursor.copy_from(sf_io, table_name, sep=',')


def insert_geo_to_db(geo_zip_filename, db_cursor):
    print 'Processing zip file: %s' % geo_zip_filename
    geo_file = os.path.basename(geo_zip_filename).rstrip('.zip').replace('_', '.')
    in_temp_file = tempfile.NamedTemporaryFile()
    with zipfile.ZipFile(geo_zip_filename, 'r') as geo_zip:
        in_temp_file.write(geo_zip.read(geo_file))
    out_temp_file = open('tmp.geo', 'w')#tempfile.NamedTemporaryFile()
    print 'Converting %s file to csv' % geo_file
    GEO_SCHEMA = 'sql/census2000_geo_schema.csv'
    pipe = subprocess.check_call(
        ['in2csv', '-e', 'latin1', '-s', GEO_SCHEMA, in_temp_file.name],
        stdout=out_temp_file)
    in_temp_file.close()
    TABLE_NAME = 'geo2000'
    print 'Copying data from %s to table %s' % (geo_file, TABLE_NAME)
    out_temp_file.close()
    with open('tmp.geo', 'rb') as infile:
        # Import data
        copy_command = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER ','" % TABLE_NAME
        db_cursor.copy_expert(copy_command, infile)

if __name__ == '__main__':
    main()
