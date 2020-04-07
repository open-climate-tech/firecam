# Copyright 2018 The Fuego Authors.
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

Find fire camera location matches and ensure that archives have data for specified start time

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper
import db_manager
import img_archive

import time, datetime
import dateutil.parser
import logging
import csv
import math

def getLocationMatches(dbManager, longitude, latitude, startTime):
    sqlTemplate = """SELECT fires.name, fires.timestamp, cameras.cameraIDs,
    fires.latitude - cameras.latitude AS lat_diff, fires.longitude - cameras.longitude AS long_diff,
    round((fires.Latitude-cameras.Latitude)*(fires.Latitude-cameras.Latitude)+(fires.Longitude-cameras.Longitude)*(fires.Longitude-cameras.Longitude),4) AS distance
    FROM fires CROSS JOIN cameras
    WHERE fires.started IS NOT null
    AND fires.adminunit="MVU" AND cameras.network="HPWREN"
    AND distance < 0.08
    ORDER BY distance"""
    sqlStr = sqlTemplate
    if longitude and latitude and startTime:
        timeDT = dateutil.parser.parse(startTime)
        tstamp = int(time.mktime(timeDT.timetuple()))
        sqlTemplate = """SELECT "%s" as name, %d as timestamp, cameras.cameraIDs,
        %f - cameras.latitude AS lat_diff, %f - cameras.longitude AS long_diff,
        round((%f-cameras.Latitude)*(%f-cameras.Latitude)+(%f-cameras.Longitude)*(%f-cameras.Longitude),4) AS distance
        FROM cameras
        WHERE distance < 0.08
        ORDER BY distance"""
        sqlStr = sqlTemplate % ('fireName', tstamp, latitude, longitude, latitude, latitude, longitude, longitude)
        logging.warning('SQL: %s', sqlStr)

    dbResult = dbManager.query(sqlStr)
    logging.warning('dbr %d: %s', len(dbResult), dbResult[:2])
    return dbResult


def isCamArchiveAvailable(camArchives, cameraID, timeDT):
    matchingCams = list(filter(lambda x: cameraID == x['id'], camArchives))
    for matchingCam in matchingCams:
        # logging.warning('Searching for files in dir %s', matchingCam['dir'])
        hpwrenSource = {
            'cameraID': cameraID,
            'dirName': matchingCam['dir'],
            'startTimeDT': timeDT,
            'endTimeDT': timeDT
        }
        found = img_archive.downloadFilesHpwren(None, None, img_archive.outputDirCheckOnly, hpwrenSource, 1, False)
        if found:
            return True
        
    return False


def outputRow(outputCsv, locMatch, timeDT, availCams):
    angleEast = int(math.atan2(locMatch['lat_diff'], locMatch['long_diff'])*180/math.pi)
    heading = 90 - angleEast
    if heading < 0:
        heading += 360
    direction = 'North'
    if heading >= 45 and heading < 135:
        direction = 'East'
    elif heading >= 135 and heading < 225:
        direction = 'South'
    elif heading >= 225 and heading < 315:
        direction = 'West'
    outputCsv.writerow([locMatch['name'], str(timeDT), ','.join(availCams), heading, direction, locMatch['distance']])


def main():
    reqArgs = [
        ['o', 'outputFile', 'filename for output CSV of fire x camera matches with available archives'],
    ]
    optionalArgs = [
        ['g', 'longitude', 'longitude of fire', float],
        ['t', 'latitude', 'latitude of fire', float],
        ['s', 'startTime', 'start time of fire'],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optionalArgs, parentParsers=[goog_helper.getParentParser()])
    googleServices = goog_helper.getGoogleServices(settings, args)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file)
    outputFile = open(args.outputFile, 'w', newline='')
    outputCsv = csv.writer(outputFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    camArchives = img_archive.getHpwrenCameraArchives(googleServices['sheet'], settings)

    locMatches = getLocationMatches(dbManager, args.longitude, args.latitude, args.startTime)
    totalMatches = len(locMatches)
    numOutput = 0
    for rowNum,locMatch in enumerate(locMatches):
        timeDT = datetime.datetime.fromtimestamp(locMatch['timestamp'])
        cams = locMatch['cameraids'].split(',')
        availCams = []
        for cameraID in cams:
            if isCamArchiveAvailable(camArchives, cameraID, timeDT):
                availCams.append(cameraID)
        # logging.warning('availCams %d: %s', len(availCams), availCams)
        if len(availCams) > 0:
            outputRow(outputCsv, locMatch, timeDT, availCams)
            numOutput += 1
        if (rowNum % 10) == 0:
            logging.warning('Processing %d of %d, output %d', rowNum, totalMatches, numOutput)

    logging.warning('Processed %d, output %d', totalMatches, numOutput)

if __name__=="__main__":
    main()
