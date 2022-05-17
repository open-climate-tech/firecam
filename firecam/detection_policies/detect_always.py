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
import random
import time
from PIL import Image

class DetectAlways:

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        self.modelId = 'always'


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        last_image_spec = image_spec[-1]
        imgPath = last_image_spec['path']
        img = Image.open(imgPath)
        centerX = int((random.random()*img.size[0]*0.5) + img.size[0]*0.25)
        centerY = int((random.random()*img.size[1]*0.5) + img.size[1]*0.25)
        detectionResult = {
            'fireSegment': {
                'score': 0.9,
                'MinX': centerX - 50,
                'MinY': centerY - 50,
                'MaxX': centerX + 50,
                'MaxY': centerY + 50,
            },
            'timeMid': time.time()
        }
        img.close()
        return detectionResult
