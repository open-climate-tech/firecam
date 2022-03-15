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

Common settings useful for all firecam code

"""

import logging
import os, sys
import json
from firecam.lib import goog_helper

def findSettingsFile():
    """Find the path with the settings file by searching the usual suspects
       (cwd, ~, ~/Desktop, ~/Documents) in order

    Returns:
        (string) path to file if it exists
    """
    settingsName = 'oct-fire-settings.json'
    userPath = os.path.expanduser('~')
    if os.path.exists(settingsName):
        return settingsName
    elif os.path.exists(os.path.join(userPath, settingsName)):
        return os.path.join(userPath, settingsName)
    elif os.path.exists(os.path.join(userPath, 'Desktop', settingsName)):
        return os.path.join(userPath, 'Desktop', settingsName)
    elif os.path.exists(os.path.join(userPath, 'Documents', settingsName)):
        return os.path.join(userPath, 'Documents', settingsName)
    elif os.path.exists(os.path.join(userPath, 'Downloads', settingsName)):
        return os.path.join(userPath, 'Downloads', settingsName)
    raise Exception('Could not locate settings file')


def readSettingsFile():
    """Read the settings JSON file and parse into a dict

    Returns:
        dict with parsed settings JSON
    """
    settingsPath = os.environ['OCT_FIRE_SETTINGS'] if 'OCT_FIRE_SETTINGS' in os.environ else None
    if not settingsPath:
        settingsPath = findSettingsFile()
        logging.warning('Using settings from %s', settingsPath)
    settingsStr = goog_helper.readFile(settingsPath)
    settingsDict = json.loads(settingsStr)
    # logging.warning('settings %s', settingsDict)
    return settingsDict


# configure logging module to add timestamps and pid, and to silence useless logs
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR) # silence googleapiclient logs
logging.basicConfig(format='%(asctime)s.%(msecs)03d: %(process)d: %(message)s', datefmt='%F %T')

MAP_ZOOM_MIN = 7
MAP_ZOOM_MAX = 10
MAP_LAT_DIFF = 5.6 # at MAP_ZOOM_MIN
MAP_LONG_DIFF = 7 # at MAP_ZOOM_MIN

# set module attributes based on json file data
settingsJson = readSettingsFile()
for (key, val) in settingsJson.items():
    setattr(sys.modules[__name__], key, val)
    # set environment variable GOOGLE_APPLICATION_CREDENTIALS if value is specified in config
    if (key == 'gcpServiceKey') and val and not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = val
