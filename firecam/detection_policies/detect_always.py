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

This detection policy always returns a detection.  Meant for testing the code

"""

import os, sys
import time

class DetectAlways:

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        self.modelId = 'always'


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        detectionResult = {
            'fireSegment': {
                'score': 0.9,
                'MinX': 30,
                'MinY': 130,
                'MaxX': 70,
                'MaxY': 170,
            },
            'timeMid': time.time()
        }
        return detectionResult
