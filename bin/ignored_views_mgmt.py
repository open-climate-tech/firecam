# Copyright 2022 Open Climate Tech Contributors
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

add, update, and stats for ignored_views

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager
from firecam.lib import img_archive

import logging
import time, datetime, dateutil.parser

def execCameraSql(dbManager, sqlTemplate, cameraID, isQuery):
    sqlStr = sqlTemplate % cameraID
    logging.warning('SQL str: %s', sqlStr)
    if isQuery:
        dbResult = dbManager.query(sqlStr)
        logging.warning('dbr %d: %s', len(dbResult), dbResult)
    else:
        dbManager.execute(sqlStr)
        dbResult = None
    return dbResult


def getRecentFalse(dbManager, numFalse, startTimeStr=None, restrictType=None):
    if startTimeStr:
        startTime = int(dateutil.parser.parse(startTimeStr).timestamp())
    else:
        startTime = int(time.time()-3600*24*30) # one month ago
    typesConstraint = dbManager.restrictTypeClause(restrictType)
    if typesConstraint:
        typesConstraint = 'WHERE %s' % typesConstraint
    sqlTemplate = """SELECT v0.cameraname as cameraname,heading as camheading,minx,maxx,count(*) as ct FROM
                        (SELECT cameraname,timestamp FROM votes WHERE timestamp > %s and isrealfire=0) as v0
                        JOIN probables on v0.cameraname=probables.cameraname and v0.timestamp=probables.timestamp
                        JOIN sources on v0.cameraname=sources.name
                        %s
                        GROUP BY v0.cameraname,heading,minx,maxx
                        ORDER BY ct desc,v0.cameraname limit %d"""
    sqlStr = sqlTemplate % (startTime, typesConstraint, numFalse)
    print(sqlStr)
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) == 0:
        logging.warning('getRecentFalse no results')
    logging.warning('getRecentFalse %d: %s', len(dbResult), dbResult)
    return dbResult


def addNew(dbManager, ignoredViews, cameraID, heading, angularWidth):
    matchingCams = list(filter(lambda x: x['cameraid'] == cameraID, ignoredViews))
    logging.warning('Found %d views for camera ID %s: %s', len(matchingCams), cameraID, matchingCams)
    ignoredView = img_archive.findIgnoredView(ignoredViews, cameraID, heading, angularWidth)
    if ignoredView == None:
        dbRow = {
            'CameraId': cameraID,
            'Heading': heading,
            'AngularWidth': angularWidth,
        }
        dbManager.add_data('ignored_views', dbRow)
        logging.warning('Added view %s', dbRow)
        return
    logging.warning('Found intersecting view %s', ignoredView)
    (newHeading, newWidth) = img_archive.unionAngleRanges(ignoredView['heading'], ignoredView['angularwidth'], heading, angularWidth)
    if newHeading != ignoredView['heading'] or newWidth != ignoredView['angularwidth']:
        sqlTemplate = "UPDATE ignored_views set heading=%d, angularwidth=%d where cameraid='%s' and heading=%s"
        sqlStr = sqlTemplate % (newHeading, newWidth, cameraID, ignoredView['heading'])
        dbManager.execute(sqlStr)
        logging.warning('Updated view %s, %s', newHeading, newWidth)


def main():
    reqArgs = [
        ["m", "mode", "list, add, checkdb"],
    ]
    optArgs = [
        ["s", "startTime", "starting date and time in ISO format (e.g., 2019-02-22T14:34:56 in Pacific time zone)"],
        ["c", "cameraID", "ID of the camera (e.g., mg-n-mobo-c)"],
        ["a", "heading", "view heading", int],
        ["w", "angularWidth", "ignored view width", int],
        ["n", "noState", "(optional) no changes to state"],
        ["f", "maxFalse", "(optional) maximum false positives to check (default 10)", int],
        ["r", "restrictType", "Only process images from cameras of given type"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                     psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                     psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    ignoredViews = dbManager.get_ignoredViews()
    logging.warning('Num views: %d', len(ignoredViews))
    numFalse = args.maxFalse if args.maxFalse else 10
    if args.mode == 'list':
        logging.warning('All views: %s', ignoredViews)
    elif args.mode == 'add':
        addNew(dbManager, ignoredViews, args.cameraID, args.heading, args.angularWidth)
    elif args.mode == 'checkdb':
        recents = getRecentFalse(dbManager, numFalse, startTimeStr=args.startTime, restrictType=args.restrictType)
        for entry in recents:
            logging.warning('count %d: cam %s, camHead %s, min %s, max %s', entry['ct'], entry['cameraname'], entry['camheading'], entry['minx'], entry['maxx'])
            fov = img_archive.getApproxCameraFov(entry['cameraname'])
            imgSizeX = img_archive.getApproxCameraSizeX(entry['cameraname'])
            # logging.warning('cam %s: fov %s, sizeX %s', entry['cameraname'], fov, imgSizeX)
            (fireHeading, rangeAngle) = img_archive.getHeadingRange(entry['camheading'], fov, entry['minx'], entry['maxx'], imgSizeX)
            logging.warning('cam %s: fireHeading %s, rangeAngle %s', entry['cameraname'], round(fireHeading), round(rangeAngle))
            if not args.noState:
                addNew(dbManager, ignoredViews, entry['cameraname'], fireHeading, rangeAngle)
                ignoredViews = dbManager.get_ignoredViews() # update state
    else:
        logging.error('unexpected mode: %s', args.mode)
        exit(1)
    return


if __name__=="__main__":
    main()
