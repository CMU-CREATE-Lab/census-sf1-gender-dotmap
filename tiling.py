import array
import csv
import math
import os
import glob


class MapTiler(object):
    def __init__(self, tileSize=256):
        self.tileSize = tileSize

    # Lon Lat to Web Mercator
    def LonLatToPixelXY(self, lon, lat):
        x = (lon + 180.0) * self.tileSize / 360.0
        y = (self.tileSize/2.0) - math.log(math.tan((
            lat + 90.0) * math.pi / 360.0)) * (
                    self.tileSize/2.0) / math.pi
        return x, y

    # Web Mercator to Lon Lat
    def PixelXYToLonLat(self, x, y):
        lat = math.atan(math.exp(((self.tileSize/2.0) - y) * math.pi / (
            self.tileSize/2.0))) * 360.0 / math.pi - 90.0
        lon = x * 360.0 / self.tileSize - 180.0
        return lon, lat

    # Lon Lat to level, column, row
    def LonLatToTileCoords(self, lon, lat, z):
        lat_rad = math.radians(lat)
        n = 2.0 ** z
        xtile = int((lon + 180.0) / 360.0 * n)
        ytile = int((1.0 - math.log(math.tan(lat_rad) + (
            1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
        return z, xtile, ytile


def init_capture(capture_dir):
    # Create capture dir if it doesn't exist
    if not os.path.exists(capture_dir):
        os.makedirs(capture_dir)


def write_tile(rows, total, tiler, zoomlevel, outputdir):
    for row in rows[0:total]:
        lon, lat = tiler.PixelXYToLonLat(float(row['x']), float(row['y']))
        z, r, c = tiler.LonLatToTileCoords(lon, lat, zoomlevel)
        tile = "%s/%s/%s/%s.csv" % (outputdir, z, r, c)
        if not os.path.exists(os.path.dirname(tile)):
            try:
                os.makedirs(os.path.dirname(tile))
            except OSError as exc:
                if exc.errno == errno.EEXIST and os.path.isdir(path):
                    pass
                else:
                    raise

        if not os.path.exists(tile):
            csvfile = open(tile, "w")
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            csvfile.close
        else:
            csvfile = open(tile, "a")
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(row)
            csvfile.close

fieldnames = ['x', 'y', 'color', 'time']


def main():
    outputdir = "tiles/"
    inputdir = "state_csv/"
    for csv_file in glob.iglob(
            '%s/*.csv' % inputdir):
        try:
            generate_state_tile_csv(csv_file, outputdir)
        except Exception, e:
            print "error with file: %s. [%s]" % (csv_file, e)
    csv_tiles2bin(outputdir)


def generate_state_tile_csv(inputfile, outputdir):
    print "processing %s" % inputfile
    rows = []
    with open(inputfile, "r") as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            rows.append(row)
    # Decompose the CSV data into tiles
    tiler = MapTiler()
    zoomlevel = 10   # Largest zoom level
    subsamples = []  # Subsample rates per zoom level
    totals = []
    zooms = list(reversed(range(zoomlevel+1)))
    init_capture(outputdir)
    for i in xrange(len(zooms)):
        s = 1.0/(min(1024, pow(4, i)))
        subsamples.append(s)
        totals.append(int(math.floor(len(rows) * s)))
        write_tile(rows, totals[i], tiler, zooms[i], outputdir)


def csv_tiles2bin(outputdir):
    # Convert the CSV tiles into bin tiles
    for root, dirs, files in os.walk(outputdir):
        for name in files:
            if not name.lower().endswith((".csv")):
                continue
            src = os.path.join(root, name)
            filename, ext = os.path.splitext(name)
            dest = os.path.join(root, filename + '.bin')
            csvfile = open(src, 'rb')
            reader = csv.DictReader(csvfile)
            data = []
            for row in reader:
                data.append(float(row['x']))
                data.append(float(row['y']))
                data.append(float(row['color']))
#                data.append(float(row['time']))
            csvfile.close()
            array.array('f', data).tofile(open(dest, 'w'))


if __name__ == '__main__':
    main()
