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

add, delete, enable, disable, stats, or list cameras in detection system

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager

import logging
import random
import datetime

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


def getTime(dbResult):
    if len(dbResult) != 1:
        return None
    timeVal = dbResult[0]['maxtime']
    if not timeVal:
        return None
    return datetime.datetime.fromtimestamp(timeVal).isoformat()


def main():
    reqArgs = [
        ["m", "mode", "add, delete, enable, disable, stats, or list"],
    ]
    optArgs = [
        ["c", "cameraID", "ID of the camera (e.g., mg-n-mobo-c)"],
        ["u", "url", "url to get images from camera"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                     psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                     psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    cameraInfos = dbManager.get_sources(activeOnly=False)
    logging.warning('Num all cameras: %d', len(cameraInfos))
    logging.warning('Num active cameras: %d', len(list(filter(lambda x: x['dormant'] == 0, cameraInfos))))
    if args.mode == 'list':
        logging.warning('All cameras: %s', list(map(lambda x: x['name'], cameraInfos)))
        return
    matchingCams = list(filter(lambda x: x['name'] == args.cameraID, cameraInfos))
    logging.warning('Found %d matching cams for ID %s', len(matchingCams), args.cameraID)

    if args.mode == 'add':
        if len(matchingCams) != 0:
            logging.error('Camera with ID %s already exists: %s', args.cameraID, matchingCams)
            exit(1)
        dbRow = {
            'name': args.cameraID,
            'url': args.url,
            'dormant': 0,
            'randomID': random.random(),
            'last_date': datetime.datetime.now().isoformat()
        }
        dbManager.add_data('sources', dbRow)
        logging.warning('Successfully added camera %s', args.cameraID)
        return

    if len(matchingCams) != 1:
        logging.error('Cannot find camera with ID %s: %s', args.cameraID, matchingCams)
        exit(1)
    camInfo = matchingCams[0]
    logging.warning('Cam details: %s', camInfo)

    if args.mode == 'delete':
        sqlTemplate = """DELETE FROM sources WHERE name = '%s' """
        execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=False)
        return

    if args.mode == 'enable':
        if camInfo['dormant'] == 0:
            logging.error('Camera already enabled: dormant=%d', camInfo['dormant'])
            exit(1)
        sqlTemplate = """UPDATE sources SET dormant=0 WHERE name = '%s' """
        execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=False)
        return

    if args.mode == 'disable':
        if camInfo['dormant'] == 1:
            logging.error('Camera already disabled: dormant=%d', camInfo['dormant'])
            exit(1)
        sqlTemplate = """UPDATE sources SET dormant=1 WHERE name = '%s' """
        execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=False)
        return

    if args.mode == 'stats':
        sqlTemplate = """SELECT max(timestamp) as maxtime FROM scores WHERE CameraName = '%s' """
        dbResult = execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=True)
        logging.warning('Most recent image scanned: %s', getTime(dbResult))
        sqlTemplate = """SELECT max(timestamp) as maxtime FROM detections WHERE CameraName = '%s' """
        dbResult = execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=True)
        logging.warning('Most recent smoke detection: %s', getTime(dbResult))
        sqlTemplate = """SELECT max(timestamp) as maxtime FROM alerts WHERE CameraName = '%s' """
        dbResult = execCameraSql(dbManager, sqlTemplate, args.cameraID, isQuery=True)
        logging.warning('Most recent smoke alert: %s', getTime(dbResult))
        return

    logging.error('Unexpected mode: %s', args.mode)
    exit(1)


if __name__=="__main__":
    main()
