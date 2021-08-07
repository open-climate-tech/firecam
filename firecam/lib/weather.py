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

Helper functions for weather related functionality

"""

from firecam.lib import settings

import logging
import datetime
import urllib.request
import json
import pandas as pd


def getHistoricalWeatherExternal(timestamp, centroidLatLong):
    baseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

    baseURL += str(centroidLatLong[0]) + ',' + str(centroidLatLong[1]) + '/'
    isoStr = datetime.datetime.fromtimestamp(timestamp).isoformat()
    baseURL += isoStr

    baseURL += '?key=' + settings.weatherHistoryKey

    weatherInfo = None
    try:
        resp = urllib.request.urlopen(baseURL)
        weatherStr = resp.read().decode('utf-8')

        # XXXXX test data
        # weatherStr = '{"currentConditions": {"datetime": "10:00:00", "datetimeEpoch": 1600102800, "temp": 93.1, "feelslike": 88.7, "humidity": 14.6, "dew": 37.8, "precip": 0.0, "precipprob": null, "snow": null, "snowdepth": 0.0, "preciptype": null, "windgust": null, "windspeed": 0.0, "winddir": 0.0, "pressure": 1014.6, "visibility": 9.9, "cloudcover": 0.0, "solarradiation": null, "solarenergy": null, "uvindex": null, "conditions": "Clear", "icon": "clear-day", "stations": ["KPSP", "69015093121", "KNXP", "72286893138"], "sunrise": "06:27:29", "sunriseEpoch": 1600090049, "sunset": "18:51:27", "sunsetEpoch": 1600134687, "moonphase": 0.94} }'

        weatherJson = json.loads(weatherStr)
        weatherInfo = weatherJson['currentConditions']
    except Exception as e:
        logging.error('Weather error %s: %s', baseURL, str(e))

    return weatherInfo


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


def getHistoricalWeather(dbManager, cameraID, timestamp, centroidLatLong):
    # first check if cached in DB
    weatherInfo = getDbWeather(dbManager, cameraID, timestamp)
    if not weatherInfo:
        weatherInfo = getHistoricalWeatherExternal(timestamp, centroidLatLong)
        if not weatherInfo:
            return None
        saveDbWeather(dbManager, cameraID, timestamp, weatherInfo, 'visualcrossing')
    if not weatherInfo['temp']:
        logging.error('No temperature %s: %s', timestamp, weatherInfo)
        return None

    return weatherInfo


def normalizeWeather(score, timestamp, centroid, numPolys, weatherInfo, isRealFire):
    dataArr = [(float(score) - 0.5) * 2]
    dt = datetime.datetime.fromtimestamp(timestamp)
    # month would bias because too little data
    dataArr += [(dt.hour - 12) / 6]
    dataArr += [centroid[0] - 33.5]
    dataArr += [centroid[1] + 118]
    dataArr += [numPolys - 1]
    dataArr += [(weatherInfo['temp'] - 70) / 20]
    # feelslike is almost identical to temp
    # uvindex is null
    dataArr += [(weatherInfo['dew'] - 50) / 20]
    dataArr += [(weatherInfo['humidity'] - 50) / 50]
    dataArr += [(weatherInfo['precip'] or 0) * 5]
    dataArr += [((weatherInfo['windspeed'] or 0) - 6) / 6]
    dataArr += [((weatherInfo['winddir'] or 0) - 180)/ 180]
    dataArr += [((weatherInfo['pressure'] or 1013) - 1013) / 10]
    dataArr += [(weatherInfo['visibility'] - 9) / 5]
    dataArr += [(weatherInfo['cloudcover'] - 50) / 50]

    dataArr += [int(isRealFire)]
    # logging.warning('Data array: %s', dataArr)
    return dataArr


def readWeatherCsv(inputCsv):
    column_names = ['imgScore', 'hour', 'lat', 'long', 'numintersects', 'temp', 'dew',
                    'humidity', 'precip', 'windspeed', 'winddir', 'pressure', 'visibility', 'cloudcover',
                    'realfire']
    raw_dataset = pd.read_csv(inputCsv, names=column_names, skipinitialspace=True)

    # drop useless columns
    raw_dataset.pop('lat')
    raw_dataset.pop('long')
    raw_dataset.pop('hour')

    labels = raw_dataset.pop('realfire')
    return (raw_dataset, labels)
