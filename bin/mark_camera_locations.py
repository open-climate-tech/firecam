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

Mark the locations of each camera on given map

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager
from firecam.lib import goog_helper

import logging
import urllib.parse
from urllib.request import urlretrieve
from PIL import Image, ImageDraw

def getCameraLocations(dbManager):
    typesArr = list(map(lambda x: "type='%s'" % x, settings.prodTypes.split(','))) # PSQL wants single quotes
    typesConstraint = '(' + ' or '.join(typesArr)  + ')'
    sqlTemplate = "select latitude,longitude from cameras where locationID in (select distinct locationID from sources where dormant=0 and %s)"
    sqlStr = sqlTemplate % typesConstraint
    dbResult = dbManager.query(sqlStr)
    # print('dbr', len(dbResult), dbResult)
    if len(dbResult) == 0:
        logging.error('Did not find camera locations')
        return None
    return dbResult


def drawCircle(mapImg, centerX, centerY, radius, opacityRatio):
    mapImgAlpha = mapImg.convert('RGBA')
    circle = Image.new('RGBA', mapImgAlpha.size)
    circleDraw = ImageDraw.Draw(circle)
    opacity = max(round(opacityRatio*12), 2)
    circleDraw.ellipse((centerX - radius, centerY - radius, centerX + radius, centerY + radius), fill=(255,0,0,opacity))
    circleDraw.ellipse((centerX - 3, centerY - 3, centerX + 3, centerY + 3), fill=(255,0,0,128))
    mapImgAlpha.paste(circle, mask=circle)
    del circleDraw
    circle.close()
    return mapImgAlpha.convert('RGB')


def main():
    reqArgs = [
        ["m", "mapFile", "base map"],
        ["l", "leftLongitude", "longitude of left edge", float],
        ["r", "rightLongitude", "longitude of right edge", float],
        ["t", "topLatitude", "latitude of top edge", float],
        ["b", "bottomLatitude", "latitude of bottom edge", float],
    ]
    optArgs = [
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    locations = getCameraLocations(dbManager)
    logging.warning('Found %d locations', len(locations))
    mapImg = Image.open(args.mapFile)
    assert args.leftLongitude < args.rightLongitude
    assert args.topLatitude > args.bottomLatitude
    diffLat = args.topLatitude - args.bottomLatitude
    diffLong = args.rightLongitude - args.leftLongitude
    radiusDegrees = 0.3

    for location in locations:
        numNearby = len(list(filter(lambda x: ((x['longitude'] - location['longitude'])**2 + (x['latitude'] - location['latitude'])**2) < radiusDegrees**2, locations)))
        # logging.warning('loc %s, %s', location, numNearby)
        centerX = (location['longitude'] - args.leftLongitude)/diffLong*mapImg.size[0]
        centerY = mapImg.size[1] - (location['latitude'] - args.bottomLatitude)/diffLat*mapImg.size[1]
        opacityRatio = min(4/numNearby, 1)
        mapImg = drawCircle(mapImg, centerX, centerY, radiusDegrees/diffLat*mapImg.size[1], opacityRatio)

    mapImg.save('amap.jpg', quality=95)


if __name__=="__main__":
    main()
