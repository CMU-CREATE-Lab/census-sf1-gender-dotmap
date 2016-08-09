from __future__ import absolute_import
import random
import datetime

import psycopg2
from shapely.geometry import *
from shapely.wkb import loads

from dotgen.celery import app
from dotgen.utils import LonLatToPixelXY
from dotgen.utils import pack_color
from dotgen.utils import split_list
import db_settings


@app.task
def generate_state_csv(state, year=2010):
    cache = {}
    def randomPoint(geom):
        if geom in cache:
            poly, bbox = cache[geom]
        else:
            poly = loads(geom, True)
            bbox = poly.bounds
            cache[geom] = (poly, bbox)
        (l, b, r, t) = bbox
        while True:
            point = Point(random.uniform(l, r), random.uniform(t, b))
            if point is None:
                break
            if poly.contains(point):
                break
        return point.__geo_interface__['coordinates']

    conn = psycopg2.connect(
        database=db_settings.DB,
        user=db_settings.USER,
        password=db_settings.PASSWORD,
        host=db_settings.HOST)
    # Get the lat long form DB
    year_p12_table_map = {
        2010: 'sf1_P12',
        2000: 'sf1_2000_P12'
    }
    year_tabblock_table_map = {
        2010: 'tl_2010_tabblock10',
        2000: 'tl_2010_tabblock00'
    }
    year_geo_table_map = {
        2010: 'geo2010',
        2000: 'geo2000'
    }
    year_geoid_column_map = {
        2010: 'geoid10',
        2000: 'blkidfp00'
    }
    print "Generating csv for %s, year %d" % (state, year)
    P12_TABLE = year_p12_table_map[year]
    GEO_TABLE = year_geo_table_map[year]
    TABBLOCK_TABLE = year_tabblock_table_map[year]
    GEOID_COLUMN = year_geoid_column_map[year]
    query = """
    SELECT geom, male, female
    FROM %s tb JOIN (
        SELECT state || county || tract || block as geoid,
            P012002 as male, P012026 as female
        FROM %s g JOIN %s sf1 ON
            g.logrecno = sf1.logrecno AND g.stusab = '%s' AND sf1.stusab = '%s'
        WHERE sumlev = '101') sf1 ON sf1.geoid = tb.%s
    WHERE male > 0 AND female > 0
    """ % (TABBLOCK_TABLE, GEO_TABLE, P12_TABLE, state, state, GEOID_COLUMN)

    male_color = pack_color({'r': 25, 'g': 75, 'b': 255})
    female_color = pack_color({'r': 20, 'g': 138, 'b': 9})

    BATCH_SIZE = 100000
    cur = conn.cursor(name='gis_cursor_%s' % state)
    cur.itersize = BATCH_SIZE

    cur.execute(query)

    data = []
    for row in cur:
        print '%s batch starting at %d' % (state, len(data))
        block_geom = row[0]
        male = row[1]
        female = row[2]
        sex = (male, female)
        for sex_choice in sex:
            point_color = male_color if sex_choice == male else female_color
            for i in xrange(sex_choice):
                point = randomPoint(block_geom)
                data += LonLatToPixelXY(point)
                data.append(point_color)

    conn.close()

    print "Randomizing points..."
    split = split_list(data, len(data)/3)
    random.shuffle(split)
    data = []
    for x in split:
        for y in x:
            data += [y]
    destination = 'state_csv/dots2tile_%s_%d.csv' % (state, year)
    # Save array to CSV file
    TIME = (datetime.datetime(year, 1, 1) - datetime.datetime(
            1970, 1, 1)).total_seconds()
    with open(destination, 'w') as outfile:
        i = 0
        while i < len(data):
            outfile.write("%s,%s,%s,%s\n" % (
                data[i], data[i+1], data[i+2], TIME))
            i += 3

