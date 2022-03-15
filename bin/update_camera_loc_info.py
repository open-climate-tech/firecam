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

Update locaiton information for camears:
1) Get maps covering camera areas and upload them to GCS and store GCS ID in SQL DB
2) Find city name and store in SQL DB

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager
from firecam.lib import goog_helper

import logging
import urllib.parse
from urllib.request import urlretrieve
import googlemaps
import re

def getCameraLocations(dbManager):
    sqlStr = "select locationID, latitude, longitude from cameras where locationID !='' "

    dbResult = dbManager.query(sqlStr)
    # print('dbr', len(dbResult), dbResult)
    if len(dbResult) == 0:
        logging.error('Did not find camera locations')
        return None
    return dbResult


def updateCameraMaps(dbManager, locationID, mapFiles):
    sqlTemplate = "UPDATE cameras SET mapFile = '%s' where locationID='%s'"
    sqlStr = sqlTemplate % (','.join(mapFiles), locationID)
    # print('sqls', sqlStr)
    dbManager.execute(sqlStr)


def updateCameraCity(dbManager, locationID, cityName):
    sqlTemplate = "UPDATE cameras SET CityName = '%s' where locationID='%s'"
    sqlStr = sqlTemplate % (cityName, locationID)
    # print('sqls', sqlStr)
    dbManager.execute(sqlStr)


def cityNameFromComp(geoCodeComp):
    cityInfo = list(filter(lambda x: 'locality' in x['types'], geoCodeComp['address_components']))
    if len(cityInfo) == 0:
        cityInfo = list(filter(lambda x: 'political' in x['types'], geoCodeComp['address_components']))
    if len(cityInfo) == 0:
        cityInfo = list(filter(lambda x: 'administrative_area_level_2' in x['types'], geoCodeComp['address_components']))
    if len(cityInfo) == 0:
        cityInfo = [geoCodeComp['address_components'][0]]
    if not 'locality' in cityInfo[0]['types']:
        logging.warning('using type %s', cityInfo[0]['types'])
    cityName = cityInfo[0]['long_name']
    return cityName


def cityNameFromCode(geoCodeRes):
    cityName = ''
    for geoCodeComp in geoCodeRes:
        cityName = cityNameFromComp(geoCodeComp)
        if cityName and re.findall('^[A-Za-z ]+$', cityName): # contains only alphabet and spaces
            break
    return cityName


def getMapLocal(latitude, longitude, locationid, zoom):
    params = {
        'center': str(latitude) + ',' + str(longitude),
        'zoom': zoom,
        'size': '640x640',
        'format': 'jpg',
        'key': settings.mapsKey
    }
    # print('params', params)
    url = 'http://maps.googleapis.com/maps/api/staticmap?' + urllib.parse.urlencode(params)
    imgPath = locationid + ('-map640z%s.jpg' % zoom)
    urlretrieve(url, imgPath)
    return imgPath


def uploadMapGCS(latitude, longitude, locationid, zoom):
    imgPath = getMapLocal(latitude, longitude, locationid, zoom)
    logging.warning('uploading %s', imgPath)
    mapFileGCS = goog_helper.copyFile(imgPath, settings.mapsDir)
    os.remove(imgPath)
    return mapFileGCS


def main():
    reqArgs = [
        ["m", "mode", "basemaps, city"],
    ]
    optArgs = [
        ["l", "locationID", "camera locationID"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    gmaps = googlemaps.Client(key=settings.mapsKey)
    locations = getCameraLocations(dbManager)
    if args.locationID:
        locations = list(filter(lambda x: x['locationid'] == args.locationID.strip(), locations))
    for location in locations:
        logging.warning('loc %s', location)
        if args.mode == 'basemaps':
            mapFiles = []
            for zoom in range(settings.MAP_ZOOM_MIN, settings.MAP_ZOOM_MAX + 1):
                mapFileGCS = uploadMapGCS(location['latitude'], location['longitude'], location['locationid'], zoom)
                mapFiles.append(mapFileGCS)
            if len(mapFiles) > 0:
                logging.warning('updating DB %s', location['locationid'])
                updateCameraMaps(dbManager, location['locationid'], mapFiles)
        elif args.mode == 'city':
            geoCodeRes = gmaps.reverse_geocode((location['latitude'], location['longitude']))
            for geoCodeComp in geoCodeRes:
                cityName = cityNameFromComp(geoCodeComp)
                if re.findall('^[A-Za-z ]+$', cityName): # contains only alphabet and spaces
                    break
            logging.warning('city for %s is %s', location['locationid'], cityName)
            updateCameraCity(dbManager, location['locationid'], cityName)

if __name__=="__main__":
    main()
