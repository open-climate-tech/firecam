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

Helper functions for prescribed burns related functionality

"""

import os, sys
from firecam.lib import settings
from firecam.lib import img_archive

import logging
import pathlib
import time, datetime
import urllib.request
import csv
from PIL import Image, ImageDraw


def drawRxBurn(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, latLong):
    pixelCenter = img_archive.convertLatLongToPixels(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, latLong)
    flamePath = os.path.join(str(pathlib.Path(os.path.realpath(__file__)).parent.parent), 'data/flame32.bmp')
    flame = Image.open(flamePath)
    crossPath = os.path.join(str(pathlib.Path(os.path.realpath(__file__)).parent.parent), 'data/plus.bmp')
    cross = Image.open(crossPath)
    mapImgAlpha = mapImg.convert('RGBA')
    burnImgA = Image.new('RGBA', mapImgAlpha.size)
    burnDraw = ImageDraw.Draw(burnImgA)
    burnDraw.bitmap((pixelCenter[0] - round(flame.size[0]/2),pixelCenter[1] - round(flame.size[1]/2)), flame, fill=(0,100,100, 128))
    burnDraw.bitmap((pixelCenter[0] - round(cross.size[0]/2),pixelCenter[1] - round(cross.size[1]/2)), cross, fill=(255,0,0, 128))
    mapImgAlpha.paste(burnImgA, mask=burnImgA)
    del burnDraw
    burnImgA.close()
    flame.close()
    cross.close()
    return mapImgAlpha.convert('RGB')


def getBurnsDataUrl():
    dateStr = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d')
    rxBurnsUrl = settings.rxBurnsUrlTemplate % dateStr
    rxBurnsStr = ''
    try:
        resp = urllib.request.urlopen(rxBurnsUrl)
        rxBurnsStr = resp.read().decode('utf-8')

        # XXXXX test data
        # rxBurnsStr = '"Burn ID",Date,"Air District",County,"Air Basin","Acres Planned","Acres Approved","Acres Burned","Total Tons",Agency,"Burn Unit","Fuel Type","Burn Type",Latitude,Longitude,Status,"Planned TS","Approved TS","Active TS","Patrol TS","Out TS"\n102075604,3/2/2022,26,20,13,-999,5,0,0,6,"2022-ECCO Lot 3",Slash,1,37.387140,-119.628453,2,"Tue Mar 01, 2022 @ 2:47 PM","Tue Mar 01, 2022 @ 2:47 PM",NA,NA,NA\n102069323,3/2/2022,30,47,8,75,75,25,UNK,4,Ditches,UNK,1,41.942415,-121.7034,5,"Fri Feb 18, 2022 @ 11:49 AM","Fri Feb 18, 2022 @ 11:54 AM",NA,"Wed Mar 02, 2022 @ 7:08 AM",NA\n'
    except Exception as e:
        logging.error('RxBurns error %s: %s', rxBurnsUrl, str(e))
        rxBurnsStr = ''
    return rxBurnsStr


def getRawBurnsDataCached(dbManager):
    minTimestamp = int(time.time()) - 60*60 # 1 hour before current time
    sqlTemplate = """SELECT timestamp,info FROM rx_burns where timestamp > %s and source = '%s' order by timestamp desc limit 1"""
    sqlStr = sqlTemplate % (minTimestamp, settings.rxBurnsSource)

    dbResult = dbManager.query(sqlStr)
    if len(dbResult) == 1:
        logging.warning('Found recent rx_burns data')
        return dbResult[0]['info'].replace('\\n','\n') # DB seems to add extra '\'

    logging.warning('No compatible data.  Fetching new rx_burns data')
    rawData = getBurnsDataUrl()

    # delete all old DB cache entries for given source
    sqlTemplate = """DELETE FROM rx_burns where source = '%s' """
    sqlStr = sqlTemplate % (settings.rxBurnsSource)
    dbManager.execute(sqlStr)

    # insert new rawData into DB cache
    dbRow = {
        'Source': settings.rxBurnsSource,
        'Timestamp':  int(time.time()),
        'Info': rawData
    }
    dbManager.add_data('rx_burns', dbRow)

    return rawData


def filterActiveBurns(burnsStr):
    csvreader = csv.reader(burnsStr.splitlines())
    burnsList = list(csvreader)
    header = burnsList[0]
    latIndex = header.index('Latitude')
    longIndex = header.index('Longitude')
    statusIndex = header.index('Status')
    activeBurns = []
    for burnInfo in burnsList[1:]:
        ## if Approved, Active, or Patrol (check and mop up) burns
        if burnInfo[statusIndex] == '2' or burnInfo[statusIndex] == '3' or burnInfo[statusIndex] == '5':
            activeBurns.append({
                'latitude': float(burnInfo[latIndex]),
                'longitude': float(burnInfo[longIndex]),
            })
    return activeBurns


def getCurrentBurns(dbManager):
    rawData = getRawBurnsDataCached(dbManager)
    activeBurnLocations = filterActiveBurns(rawData)
    return activeBurnLocations


