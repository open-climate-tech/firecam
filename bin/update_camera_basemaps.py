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

Get maps covering camera areas and upload them to GCS and store GCS ID in SQL DB

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager
from firecam.lib import goog_helper

import logging
import urllib.parse
from urllib.request import urlretrieve

def getCameraLocations(dbManager):
    sqlStr = "select locationID, latitude, longitude from cameras where locationID !='' "

    dbResult = dbManager.query(sqlStr)
    # print('dbr', len(dbResult), dbResult)
    if len(dbResult) == 0:
        logging.error('Did not find camera locations')
        return None
    return dbResult


def updateCameraMap(dbManager, locationID, mapFile):
    sqlTemplate = "UPDATE cameras SET mapFile = '%s' where locationID='%s'"
    sqlStr = sqlTemplate % (mapFile, locationID)
    # print('sqls', sqlStr)
    dbManager.execute(sqlStr)


def main():
    reqArgs = [
    ]
    optArgs = [
        ["l", "locationID", "camera locationID"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    locations = getCameraLocations(dbManager)
    if args.locationID:
        locations = list(filter(lambda x: x['locationid'] == args.locationID.strip(), locations))
    for location in locations:
        logging.warning('loc %s', location)
        params = {
            'center': str(location['latitude']) + ',' + str(location['longitude']),
            'zoom': 9,
            'size': '640x640',
            'format': 'jpg',
            'key': settings.mapsKey
        }
        # print('params', params)
        url = 'http://maps.googleapis.com/maps/api/staticmap?' + urllib.parse.urlencode(params)
        imgPath = location['locationid'] + '-map640z9.jpg'
        urlretrieve(url, imgPath)
        logging.warning('uploading %s', imgPath)
        mapFileGS = goog_helper.copyFile(imgPath, settings.mapsDir)
        os.remove(imgPath)
        logging.warning('updating DB %s', location['locationid'])
        updateCameraMap(dbManager, location['locationid'], mapFileGS)


if __name__=="__main__":
    main()
