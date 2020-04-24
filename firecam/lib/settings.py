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

def readSettingsFile():
    """Read the settings JSON file and parse into a dict

    Returns:
        dict with parsed settings JSON
    """
    configPath = os.environ['OCT_FIRE_SETTINGS']
    configStr = goog_helper.readFile(configPath)
    config = json.loads(configStr)
    # logging.warning('config %s', config)
    return config


# set module attributes based on json file data
settingsJson = readSettingsFile()
for (key, val) in settingsJson.items():
    setattr(sys.modules[__name__], key, val)
    # set environment variable GOOGLE_APPLICATION_CREDENTIALS if value is specified in config
    if (key == 'gcpServiceKey') and val and not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = val


# configure logging module to add timestamps and pid, and to silence useless logs
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR) # silence googleapiclient logs
logging.basicConfig(format='%(asctime)s.%(msecs)03d: %(process)d: %(message)s', datefmt='%F %T')
