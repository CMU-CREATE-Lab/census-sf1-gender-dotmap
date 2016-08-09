import array
import math


def LonLatToPixelXY(lonlat, scale=1.0):
    (lon, lat) = lonlat
    x = (lon + 180.0) * 256.0 / 360.0
    y = 128.0 - math.log(math.tan(
        (lat + 90.0) * math.pi / 360.0)) * 128.0 / math.pi
    return [x*scale, y*scale]


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [
        alist[i*length//wanted_parts:(i+1)*length//wanted_parts] for i in
        xrange(wanted_parts)
    ]


def pack_color(color):
    return color['r'] + color['g'] * 256.0 + color['b'] * 256.0 * 256.0

