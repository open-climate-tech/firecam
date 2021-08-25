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

Reads data from csv export of one of 3 types of data:
1) votes and polygons
2) CameraID and direction
3) Filename and x/y coordinates of fire region
For each of these, it finds the approximate location and finds the historical weather,
which is cached/saved in DB.
Weather data is merged with fire data to genrate output CSV file.


"""


import os, sys

from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import db_manager
from firecam.lib import img_archive
from firecam.lib import weather

import random
import time, datetime, dateutil.parser
import logging
import csv
import json
import math
from shapely.geometry import Polygon, Point
from PIL import Image


def getCentroid(polygonStr):
    polygonCoords = json.loads(polygonStr)
    poly = Polygon(polygonCoords)
    centerLatLong = list(zip(*poly.centroid.xy))[0]
    return (round(centerLatLong[0],3), round(centerLatLong[1],3))


def getRandInterpolatedVal(percentiles):
    randVal = random.random()
    rand10 = randVal*10
    rand10Int = int(rand10)
    minVal = percentiles[rand10Int]
    maxVal = percentiles[rand10Int + 1]
    return minVal + (rand10 - rand10Int) * (maxVal - minVal)


def keepData(score, centroid, numPolys, isRealFire):
    northMexico = Polygon([(32.533, -117.157), (32.696, -115.173), (32.174, -114.692), (32.073, -117.232)])
    return not northMexico.intersects(Point(centroid))


def outputWithWeather(outFile, score, timestamp, centroid, numPolys, weatherCentroid, weatherCamera, isRealFire):
    dataArr = weather.normalizeWeather(score, numPolys, weatherCentroid, weatherCamera, timestamp, centroid, isRealFire)
    dataArrStr = list(map(str, dataArr))
    # logging.warning('Data arrayStr: %s', dataArrStr)
    dataStr = ', '.join(dataArrStr)
    # logging.warning('Data str: %s', dataStr)
    outFile.write(dataStr + '\n')


def patchCameraId(cameraID):
    if cameraID.startswith('lo-'):
        cameraID = 'm' + cameraID
    elif cameraID.startswith('so-'):
        cameraID = 'sojr-' + cameraID[3:]
    return cameraID


def main():
    reqArgs = [
        ["o", "outputFile", "output file name"],
        ["i", "inputCsv", "csvfile with fire/detection data"],
        ['m', "mode", "mode: votepoly or camdir or pruned"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    mode = args.mode
    assert mode == 'votepoly' or mode == 'camdir' or mode == 'pruned'
    outFile = open(args.outputFile, 'w')
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                     psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                     psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)

    lastCam = None
    lastTime = None
    random.seed(0)
    with open(args.inputCsv) as csvFile:
        csvreader = csv.reader(csvFile)
        for (rowIndex, csvRow) in enumerate(csvreader):
            if rowIndex < startRow:
                continue
            if rowIndex > endRow:
                print('Reached end row', rowIndex, endRow)
                break
            if mode == 'votepoly':
                [cameraID, timestamp, score, polygon, sourcePolygons, isRealFire] = csvRow[:6]
                timestamp = int(timestamp)
                logging.warning('Processing row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
                if cameraID == lastCam and timestamp == lastTime:
                    logging.warning('Duplicate row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
                lastCam = cameraID
                lastTime = timestamp
                centroid = getCentroid(polygon)
                if timestamp < 1607786165: #sourcePolygons didn't exist before this
                    if isRealFire:
                        numPolys = round(getRandInterpolatedVal(settings.percentilesNumPolyFire))
                    else:
                        numPolys = round(getRandInterpolatedVal(settings.percentilesNumPolyOther))
                else:
                    numPolys = 1
                    if sourcePolygons:
                        sourcePolygonsArr = json.loads(sourcePolygons)
                        numPolys = len(sourcePolygonsArr)
                cameraID = patchCameraId(cameraID)
                (mapImgGCS, camLatitude, camLongitude) = dbManager.getCameraMapLocation(cameraID)
            else:
                if mode == 'camdir':
                    [cameraID, isoTime, direction] = csvRow[:3]
                    logging.warning('Processing row: %d, cam: %s, ts: %s', rowIndex, cameraID, isoTime)
                    timestamp = time.mktime(dateutil.parser.parse(isoTime).timetuple())
                    if 'center left' in direction:
                        offset = -20
                    elif 'center right' in direction:
                        offset = 20
                    elif 'center' in direction:
                        offset = 0
                    elif 'left' in direction:
                        offset = -40
                    elif 'right' in direction:
                        offset = 40
                    else:
                        logging.error('Unexpected dir row: %d, dir: %s', rowIndex, direction)
                        continue
                elif mode == 'pruned':
                    [_cropName, minX, _minY, maxX, _maxY, fileName] = csvRow[:6]
                    minX = int(minX)
                    maxX = int(maxX)
                    nameParsed = img_archive.parseFilename(fileName)
                    cameraID = nameParsed['cameraID']
                    cameraID = patchCameraId(cameraID)
                    timestamp = nameParsed['unixTime']
                    dateStr = nameParsed['isoStr'][:nameParsed['isoStr'].index('T')]
                    if dateStr == lastTime and cameraID == lastCam:
                        # logging.warning('Skip same fire. row %s', rowIndex)
                        continue
                    lastCam = cameraID
                    lastTime = dateStr
                    localFilePath = os.path.join(settings.downloadDir, fileName)
                    if not os.path.isfile(localFilePath):
                        logging.warning('Skip missing file %s, row %s', fileName, rowIndex)
                        continue
                    img = Image.open(localFilePath)
                    degreesInView = 110
                    centerX = (minX + maxX) / 2
                    offset = centerX / img.size[0] * degreesInView - degreesInView/2
                    img.close()
                (mapImgGCS, camLatitude, camLongitude) = dbManager.getCameraMapLocation(cameraID)
                camHeading = img_archive.getHeading(cameraID)
                heading = (camHeading + offset) % 360
                angle = 90 - heading
                distanceDegrees = 0.2 # approx 14 miles
                fireLat = camLatitude + math.sin(angle*math.pi/180)*distanceDegrees
                fireLong = camLongitude + math.cos(angle*math.pi/180)*distanceDegrees
                centroid = (fireLat, fireLong)
                score = getRandInterpolatedVal(settings.percentilesScoreFire)
                numPolys = round(getRandInterpolatedVal(settings.percentilesNumPolyFire))
                isRealFire = 1
                logging.warning('Processing row: %d, heading: %s, centroid: %s, score: %s, numpoly: %s', rowIndex, heading, centroid, score, numPolys)
            if not keepData(score, centroid, numPolys, isRealFire):
                logging.warning('Skipping Mexico fire row %d, camera %s', rowIndex, cameraID)
                continue
            (weatherCentroid, weatherCamera) = weather.getWeatherData(dbManager, cameraID, timestamp, centroid, (camLatitude, camLongitude))
            if not weatherCentroid:
                logging.warning('Skipping row %d', rowIndex)
                continue
            # logging.warning('Weather %s', weatherCentroid)
            outputWithWeather(outFile, score, timestamp, centroid, numPolys, weatherCentroid, weatherCamera, isRealFire)

            logging.warning('Processed row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
    outFile.close()


if __name__=="__main__":
    main()
