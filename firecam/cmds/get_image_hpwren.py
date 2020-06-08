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

Download images from HPWREN archive matching given date/time range for
cameras matching one of following two specs:
1) Given camera ID
2) Given lat/long and checking nearby cameras using camera DB

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.lib import db_manager

import logging
import time, datetime, dateutil.parser
import math
import re


def getHeading(lat_diff, long_diff):
    """Return directional heading (0=North) given lat and long diffs

    Args:
        lat_diff: (float) difference in latitude
        long_diff: (float) difference in longitude

    Returns:
        The heading value
    """
    angleEast = int(math.atan2(lat_diff, long_diff)*180/math.pi)
    heading = 90 - angleEast
    if heading < 0:
        heading += 360
    return heading


def headingMathcesDirection(heading, direction):
    """Check if given heading falls within ~100degree field of view from given
       cardinal direction

    Args:
        heading: (float) direction of interest
        direction: (string) cardinal diretions (e.g. 'n', 'e')

    Returns:
        True if heading is within field of view
    """
    cardinalHeadings = {
        'n': 0,
        'e': 90,
        's': 180,
        'w': 270,
        'ne': 45,
        'se': 135,
        'sw': 225,
        'nw': 315,
    }
    if not direction in cardinalHeadings:
        logging.error('Unexpected angle %s', direction)
        return False
    centralHeading = cardinalHeadings[direction]
    minHeading = (centralHeading - 55) % 360
    maxHeading = (centralHeading + 55) % 360
    if minHeading < maxHeading:
        return heading > minHeading and heading < maxHeading
    else:
        return heading > minHeading or heading < maxHeading


def getNearbyCameras(dbManager, latitude, longitude, distanceMilesLimit):
    """Find all nearby cameras to given lat/long and distance specified in degrees^2

    Args:
        dbManager (DbManager):
        latitude: (float) latitude of interest
        longitude: (float) longitude of interest
        distanceMilesLimit: (float) max distance between camera and location (in miles)

    Returns:
        True if heading is within field of view
    """
    distanceDegreesLimit = distanceMilesLimit/24900*360
    distanceDegrees2Limit = distanceDegreesLimit*distanceDegreesLimit
    sqlTemplate = """SELECT cameras.cameraIDs,
        %f - cameras.latitude AS lat_diff, %f - cameras.longitude AS long_diff,
        round((%f-cameras.Latitude)*(%f-cameras.Latitude)+(%f-cameras.Longitude)*(%f-cameras.Longitude),4) AS distance
        FROM cameras
        WHERE distance < %f
        ORDER BY distance"""
    sqlStr = sqlTemplate % (latitude, longitude, latitude, latitude, longitude, longitude, distanceDegrees2Limit)
    # logging.warning('SQL: %s', sqlStr)

    dbResult = dbManager.query(sqlStr)
    # logging.warning('dbr %d: %s', len(dbResult), dbResult[:2])
    nearbyCameras = []
    for row in dbResult:
        # logging.warning('dbr row %s', row)
        heading = getHeading(row['lat_diff'], row['long_diff'])
        distanceDegrees = math.sqrt(row['distance'])
        distanceMiles = distanceDegrees/360*24900
        cameras = row['cameraids'].split(',')
        for camera in cameras:
            regexMobo = '-([ns]?[ew]?)-mobo-c'
            matches = re.findall(regexMobo, camera)
            if len(matches) == 1:
                camDir = matches[0]
                if headingMathcesDirection(heading, camDir):
                    logging.warning('Mobo camera %s matches heading %d: %d miles', camera, heading, round(distanceMiles))
                    nearbyCameras.append(camera)
            elif '-axis' in camera:
                logging.warning('Axis camera %s check heading %d: %d miles', camera, heading, round(distanceMiles))
                nearbyCameras.append(camera)
    return nearbyCameras


def main():
    reqArgs = [
        ["s", "startTime", "starting date and time in ISO format (e.g., 2019-02-22T14:34:56 in Pacific time zone)"],
    ]
    optArgs = [
        ["c", "cameraID", "ID (code name) of camera"],
        ['n', 'longitude', 'longitude of fire', float],
        ['t', 'latitude', 'latitude of fire', float],
        ['m', 'maxDistance', '(optional default=20) max distance in miles from fire', float],
        ["e", "endTime", "ending date and time in ISO format (e.g., 2019-02-22T14:34:56 in Pacific time zone)"],
        ["d", "durationMinutes", "alternative spec for endTime as start + duration", int],
        ["g", "gapMinutes", "override default of 1 minute gap between images to download"],
        ["o", "outputDir", "directory to save the output image"],
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    googleServices = goog_helper.getGoogleServices(settings, args)
    gapMinutes = int(args.gapMinutes) if args.gapMinutes else 1
    distanceMiles = float(args.maxDistance if args.maxDistance else 20)
    outputDir = args.outputDir if args.outputDir else settings.downloadDir
    startTimeDT = dateutil.parser.parse(args.startTime)
    if args.endTime:
        endTimeDT = dateutil.parser.parse(args.endTime)
    elif args.durationMinutes:
        durationDelta = datetime.timedelta(seconds = 60 * args.durationMinutes)
        endTimeDT = startTimeDT + durationDelta
    else:
        endTimeDT = startTimeDT
    assert startTimeDT.year == endTimeDT.year
    assert startTimeDT.month == endTimeDT.month
    assert startTimeDT.day == endTimeDT.day
    assert endTimeDT >= startTimeDT
    if args.cameraID:
        assert (not args.latitude) and (not args.longitude)
        cameras = [args.cameraID]
    else:
        assert args.latitude and args.longitude
        dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                        psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                        psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
        cameras = getNearbyCameras(dbManager, args.latitude, args.longitude, distanceMiles)
        logging.warning('Matched cmaeras: %s', cameras)

    camArchives = img_archive.getHpwrenCameraArchives(settings.hpwrenArchives)
    allFiles = []
    for cameraID in cameras:
        camFiles = img_archive.getHpwrenImages(googleServices, settings, outputDir, camArchives, cameraID, startTimeDT, endTimeDT, gapMinutes)
        if camFiles:
            allFiles += camFiles
    if allFiles:
        logging.warning('Found %d files.', len(allFiles))
    else:
        logging.error('No filed matched')


if __name__=="__main__":
    main()
