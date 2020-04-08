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

Test Google cloud function for ffmpeg

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
import requests
import uuid

import logging

def callGCF(url, creds, cameraID, folderID):
    headers = {'Authorization': f'bearer {creds.id_token_jwt}'}
    data = {
        'hostName': 'c1',
        'cameraID': cameraID,
        'yearDir': 2017,
        'dateDir': 20170613,
        'qNum': 3, # 'Q3.mp4'
        'uploadDir': folderID
    }
    response = requests.post(url, headers=headers, data=data)
    return response.content


def main():
    reqArgs = [
        ["c", "cameraID", "ID (code name) of camera"],
    ]
    optArgs = [
        ["l", "localhost", "localhost for testing"],
    ]
    
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    googleCreds = goog_helper.getCreds(settings, args)
    googleServices = goog_helper.getGoogleServices(settings, args)

    folderName = str(uuid.uuid4())
    folderID = goog_helper.createFolder(googleServices['drive'], settings.ffmpegFolder, folderName)
    url = settings.ffmpegUrl
    if args.localhost:
        url = 'http://localhost:8080'
    respData = callGCF(url, googleCreds, args.cameraID, folderID)
    logging.warning('GCF Result: %s', respData)
    logging.warning('New folder %s (%s) should be cleaned up', folderName, folderID)


if __name__=="__main__":
    main()
