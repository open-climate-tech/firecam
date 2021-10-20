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

This detection policy uses diff images on underlying inception_and_threshold policy

"""

import os, sys
from firecam.lib import img_archive
from . import inception_and_threshold

import tempfile
import logging

class DetectDiff:

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        logging.warning('Diff init %s', modelLocation)
        BasePolicy = inception_and_threshold.InceptionV3AndHistoricalThreshold
        self.basePolicy = BasePolicy(args, dbManager, stateless=True, modelLocation=modelLocation)
        self.modelId = self.basePolicy.modelId
        self.outputDirObj = tempfile.TemporaryDirectory()


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        last_image_spec = image_spec[-1]
        timestamp = last_image_spec['timestamp']
        cameraID = last_image_spec['cameraID']
        parsedName = img_archive.parseFilename(last_image_spec['path'])
        base_image_spec = image_spec
        diffImgPath = None
        if not parsedName['diffMinutes']:
            outputDirName = self.outputDirObj.name
            diffImg = fetchDiff(outputDirName)
            if not diffImg:
                logging.warning('Failed to fetch diff image for %s', last_image_spec['path'])
                return {
                    'fireSegment': None,
                    'timeMid': 0
                }
            diffImgPath = img_archive.getImgPath(outputDirName, cameraID, timestamp, diffMinutes=1)
            diffImg.save(diffImgPath, format='JPEG', quality=95)
            last_image_spec = last_image_spec.copy()
            last_image_spec['path'] = diffImgPath
            base_image_spec = [last_image_spec]

        detectionResult = self.basePolicy.detect(base_image_spec, checkShifts=checkShifts)
        if diffImgPath:
            os.remove(diffImgPath)
        return detectionResult
