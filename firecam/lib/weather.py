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
from firecam.lib import tf_helper

import logging
import time, datetime
import urllib.request
import json
import pandas as pd
import numpy as np


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
        weatherInfo = None

    return (weatherInfo, 'visualcrossing')


def getCurrentWeatherExternal(centroidLatLong):
    urlTemplate = 'https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&units=%s&exclude=minutely,hourly,daily&appid=%s'
    urlStr = urlTemplate % (centroidLatLong[0], centroidLatLong[1], 'imperial', settings.weatherCurrentKey)
    weatherInfo = None
    try:
        resp = urllib.request.urlopen(urlStr)
        weatherStr = resp.read().decode('utf-8')
        # XXXXX test data
        # weatherStr = '{"current": {"dt": 1628472036, "sunrise": 1628420122, "sunset": 1628472234, "temp": 69.17, "feels_like": 70.39, "pressure": 1007, "humidity": 98, "dew_point": 68.58, "uvi": 0, "clouds": 100, "visibility": 8592, "wind_speed": 1.54, "wind_deg": 330, "wind_gust": 2.06, "weather": [{"id": 501, "main": "Rain", "description": "moderate rain", "icon": "10d"}], "rain": {"1h": 1.64}}}'
        weatherJson = json.loads(weatherStr)
        weatherInfo = weatherJson['current']
        weatherInfo['dew'] = weatherInfo['dew_point']
        if 'rain' in weatherInfo and '1h' in weatherInfo['rain']:
            weatherInfo['precip'] = weatherInfo['rain']['1h']
        elif 'snow' in weatherInfo and '1h' in weatherInfo['snow']:
            weatherInfo['precip'] = weatherInfo['snow']['1h']
        else:
            weatherInfo['precip'] = 0
        weatherInfo['windspeed'] = weatherInfo['wind_speed']
        weatherInfo['winddir'] = weatherInfo['wind_deg']
        weatherInfo['orig_visibility'] = weatherInfo['visibility']
        weatherInfo['visibility'] = weatherInfo['visibility'] / 1000 # meters to km
        # max visibility on visualcrossing is 9.9 and openweathermap max is 10000
        weatherInfo['cloudcover'] = weatherInfo['clouds']
        # logging.warning('wc %s', weatherInfo )
    except Exception as e:
        logging.error('Weather error %s: %s', urlStr, str(e))
        weatherInfo = None

    return (weatherInfo, 'openweathermap')


def getDbWeather(dbManager, cameraID, timestamp):
    sqlTemplate = """SELECT weather as weather, source as source FROM weather WHERE CameraId = '%s' and Timestamp = %s """
    sqlStr = sqlTemplate % (cameraID, timestamp)
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) > 0:
        # logging.warning('db weather result: %s', dbResult[0])
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


def getWeatherData(dbManager, cameraID, timestamp, centroidLatLong):
    # first check if cached in DB
    weatherInfo = getDbWeather(dbManager, cameraID, timestamp)
    if not weatherInfo:
        currentTime = time.time()
        if (currentTime - timestamp) > 60 * 60: # one hour
            (weatherInfo, source) = getHistoricalWeatherExternal(timestamp, centroidLatLong)
        else:
            (weatherInfo, source) = getCurrentWeatherExternal(centroidLatLong)
        if not weatherInfo:
            return None
        saveDbWeather(dbManager, cameraID, timestamp, weatherInfo, source)
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
    dataArr += [(weatherInfo['visibility'] - 5) / 5]
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


def measureTrueFalse(labels, predictions, threshold):
    val_thresh = predictions > threshold
    logging.warning('Threshold %s', threshold)
    truePositive = np.logical_and(labels, val_thresh).sum()
    falseNegative = np.logical_and(labels, np.logical_not(val_thresh)).sum()
    falsePositive = np.logical_and(np.logical_not(labels), val_thresh).sum()
    trueNegative = np.logical_and(np.logical_not(labels), np.logical_not(val_thresh)).sum()
    logging.warning('TT %s', truePositive)
    logging.warning('TF %s', falseNegative)
    logging.warning('FT %s', falsePositive)
    logging.warning('FF %s', trueNegative)
    (precision, recall, f1, accuracy) = tf_helper.confusionStats(truePositive, trueNegative, falsePositive, falseNegative)
    logging.warning('Precision: %f, Recall: %f, F1: %f, Accuracy: %f', precision, recall, f1, accuracy)
