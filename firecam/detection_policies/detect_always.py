# Copyright 2018 The Fuego Authors.
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

This detection policy always returns a detection.  Meant for testing the code

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot

import time

class DetectAlways:

    def __init__(self, settings, args, google_services, dbManager, tfConfig, camArchives, minusMinutes, useArchivedImages):
        pass


    def detect(self, image_spec):
        detectionResult = {
            'annotatedFile': '',
            'fireSegment': {
                'score': 0.9
            },
            'driveFileIDs': '',
            'timeMid': time.time()
        }
        return detectionResult
