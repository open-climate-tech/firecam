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

This detection policy never returns a detection.  Meant for testing the code

"""

import os, sys

import time

class DetectNever:

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        self.modelId = 'never'


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        detectionResult = {
            'fireSegment': None,
            'timeMid': time.time()
        }
        return detectionResult
