# Copyright 2020 Open Climate Tech Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args

import logging
# import gdal


def mapping_with_bounds(latLong, latLongBounds, diffLatLong, rasterSize):
    logging.warning('coords: %f, %f, %f', latLong, latLongBounds, diffLatLong)
    pix = int((latLong - latLongBounds) / diffLatLong)
    logging.warning('pix: (%d)', pix)
    if 0 <= pix <= rasterSize:
        return pix
    else:
        logging.warning("sorry coordinate not in data (%d > %d) or (%d < %d)", latLong, diffLatLong*rasterSize, latLong, latLongBounds)
        return None

# def main():
#     reqArgs = [
#         ["g", "geoTiffName", "File name of geotiff"],
#         ["a", "lat", "latitude of desired point", float],
#         ["o", "long", "longtitude of desired point", float],
#     ]
#     args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
#     tiffData = gdal.Open(args.geoTiffName)
#     logging.warning('x: %d, y: %d', tiffData.RasterXSize, tiffData.RasterYSize)
#     metadata = tiffData.GetGeoTransform()
#     logging.warning('metadata: %s', metadata)
#     specs =  tiffData.ReadAsArray(xoff=0, yoff=0)
#     logging.warning('specs: %s', specs)
#
#     coordX = mapping_with_bounds(args.long, metadata[0], metadata[1], tiffData.RasterXSize)
#     coordY = mapping_with_bounds(args.lat, metadata[3], metadata[5], tiffData.RasterYSize)
#     if coordX != None and coordY != None:
#         val = specs[coordX,coordY]
#         logging.warning("The value is (%s)", val)
#
#
# if __name__=="__main__":
#     main()
