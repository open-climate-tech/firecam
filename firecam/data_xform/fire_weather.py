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

Reads data from csv export of votes and polygons to find historical weather, which is
cached/saved in DB.  Weather data is merged with fire data to genrate output CSV file.


"""


import os, sys

from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import db_manager
from firecam.lib import img_archive

import random
import time, datetime, dateutil.parser
import logging
import csv
import json
import urllib.request
import math
from shapely.geometry import Polygon
from PIL import Image


def getCentroid(polygonStr):
    polygonCoords = json.loads(polygonStr)
    poly = Polygon(polygonCoords)
    centerLatLong = list(zip(*poly.centroid.xy))[0]
    return (round(centerLatLong[0],3), round(centerLatLong[1],3))


def getDbWeather(dbManager, cameraID, timestamp):
    sqlTemplate = """SELECT weather as weather, source as source FROM weather WHERE CameraId = '%s' and Timestamp = %s """
    sqlStr = sqlTemplate % (cameraID, timestamp)
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) > 0:
        # logging.warning('db weather result: %s', dbResult[0])
        if dbResult[0]['source'] != 'visualcrossing':
            return
        weatherStr = dbResult[0]['weather']
        return json.loads(weatherStr)


def saveDbWeather(dbManager, cameraID, timestamp, weatherInfo, source):
    dbRow = {
        'CameraId': cameraID,
        'Timestamp': timestamp,
        'Weather': json.dumps(weatherInfo),
        'Source': source
    }
    dbManager.add_data('weather', dbRow)


def getHistoricalWeather(dbManager, cameraID, timestampStr, centroidLatLong):
    # first check if cached in DB

    baseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

    baseURL += str(centroidLatLong[0]) + ',' + str(centroidLatLong[1]) + '/'
    timestamp = int(timestampStr)

    isoStr = datetime.datetime.fromtimestamp(timestamp).isoformat()
    baseURL += isoStr

    baseURL += '?key=' + settings.weatherHistoryKey

    weatherInfo = None
    try:
        weatherInfo = getDbWeather(dbManager, cameraID, timestamp)
        if not weatherInfo:
            resp = urllib.request.urlopen(baseURL)
            weatherStr = resp.read().decode('utf-8')

            # XXXXX test data
            # weatherStr = '{"currentConditions": {"datetime": "10:00:00", "datetimeEpoch": 1600102800, "temp": 93.1, "feelslike": 88.7, "humidity": 14.6, "dew": 37.8, "precip": 0.0, "precipprob": null, "snow": null, "snowdepth": 0.0, "preciptype": null, "windgust": null, "windspeed": 0.0, "winddir": 0.0, "pressure": 1014.6, "visibility": 9.9, "cloudcover": 0.0, "solarradiation": null, "solarenergy": null, "uvindex": null, "conditions": "Clear", "icon": "clear-day", "stations": ["KPSP", "69015093121", "KNXP", "72286893138"], "sunrise": "06:27:29", "sunriseEpoch": 1600090049, "sunset": "18:51:27", "sunsetEpoch": 1600134687, "moonphase": 0.94} }'

            weatherJson = json.loads(weatherStr)
            weatherInfo = weatherJson['currentConditions']
            saveDbWeather(dbManager, cameraID, timestamp, weatherInfo, 'visualcrossing')
        if not weatherInfo['temp']:
            logging.error('No temperature %s: %s', baseURL, weatherInfo)
            return None
    except Exception as e:
        logging.error('Weather error %s: %s', baseURL, str(e))

    return weatherInfo

def getRandInterpolatedVal(percentiles):
    randVal = random.random()
    rand10 = randVal*10
    rand10Int = int(rand10)
    minVal = percentiles[rand10Int]
    maxVal = percentiles[rand10Int + 1]
    return minVal + (rand10 - rand10Int) * (maxVal - minVal)


def outputWithWeather(outFile, score, centroid, numPolys, weatherInfo, isRealFire):
    dataArr = [float(score)]
    dataArr += [centroid[0] - 32, centroid[1] + 120]
    dataArr += [numPolys]
    dataArr += [(weatherInfo['temp'] - 70) / 10]
    dataArr += [weatherInfo['humidity'] / 100]
    dataArr += [weatherInfo['precip'] or 0]
    dataArr += [weatherInfo['windspeed'] or 0]
    dataArr += [(weatherInfo['winddir'] or 0) / 360]
    dataArr += [((weatherInfo['pressure'] or 1013)- 1000) / 10]
    dataArr += [weatherInfo['visibility']]
    dataArr += [weatherInfo['cloudcover'] / 100]

    dataArr += [int(isRealFire)]
    # logging.warning('Data array: %s', dataArr)
    dataArrStr = list(map(str, dataArr))
    # logging.warning('Data arrayStr: %s', dataArrStr)
    dataStr = ', '.join(dataArrStr)
    # logging.warning('Data str: %s', dataStr)
    outFile.write(dataStr + '\n')


def main():
    reqArgs = [
        ["o", "outputFile", "output file name"],
        ["i", "inputCsv", "csvfile with contents of Cropped Images"],
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
                logging.warning('Processing row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
                if cameraID == lastCam and timestamp == lastTime:
                    logging.warning('Duplicate row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
                lastCam = cameraID
                lastTime = timestamp
                centroid = getCentroid(polygon)
                numPolys = 1
                if sourcePolygons:
                    sourcePolygonsArr = json.loads(sourcePolygons)
                    numPolys = len(sourcePolygonsArr)
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
                    if cameraID.startswith('lo-'):
                        cameraID = 'm' + cameraID
                    elif cameraID.startswith('so-'):
                        cameraID = 'sojr-' + cameraID[3:]
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
                score = getRandInterpolatedVal(settings.percentilesScore)
                numPolys = round(getRandInterpolatedVal(settings.percentilesNumPoly))
                isRealFire = 1
                logging.warning('Processing row: %d, heading: %s, centroid: %s, score: %s, numpoly: %s', rowIndex, heading, centroid, score, numPolys)
            weatherInfo = getHistoricalWeather(dbManager, cameraID, timestamp, centroid)
            if not weatherInfo:
                logging.warning('Skipping row %d', rowIndex)
                continue
            # logging.warning('Weather %s', weatherInfo)
            outputWithWeather(outFile, score, centroid, numPolys, weatherInfo, isRealFire)

            logging.warning('Processed row: %d, cam: %s, ts: %s', rowIndex, cameraID, timestamp)
    outFile.close()


if __name__=="__main__":
    main()
