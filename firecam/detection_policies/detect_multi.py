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

This detection policy wraps multiple policies

"""

import os, sys
from firecam.lib import settings
from firecam.lib import rect_to_squares
from firecam.detection_policies import policies

import time
import logging

class DetectMulti:

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        self.dbManager = dbManager
        self.stateless = stateless
        self.mainPolicy = None
        self.confirmationPolicies = []
        multiPolicySpec = modelLocation if modelLocation else settings.multiPolicySpec
        modelIDs = []
        for counter, spec in enumerate(multiPolicySpec):
            policyType = spec[0]
            policyArg = spec[1]
            logging.warning('Multi init %s: %s, %s', counter, policyType, policyArg)
            DetectionPolicyClass = policies.get_policies()[policyType]
            statelessArg = stateless or (counter > 0)
            detectionPolicy = DetectionPolicyClass(args, dbManager, stateless=statelessArg, modelLocation=policyArg)
            if counter == 0:
                self.mainPolicy = detectionPolicy
            else:
                self.confirmationPolicies.append(detectionPolicy)
            modelIDs.append(detectionPolicy.modelId)
        self.modelId = ','.join(modelIDs)


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        mainDetectionResult = self.mainPolicy.detect(image_spec, checkShifts=checkShifts, fetchDiff=fetchDiff)
        mainFireSegment = mainDetectionResult['fireSegment']
        if not mainFireSegment:
            return mainDetectionResult

        last_image_spec = image_spec[-1]
        dbRows = []
        if not self.stateless:
            dbRow = {
                'CameraName': last_image_spec['cameraID'],
                'Heading': last_image_spec['heading'],
                'Timestamp': last_image_spec['timestamp'],
                'MinX': mainFireSegment['MinX'],
                'MinY': mainFireSegment['MinY'],
                'MaxX': mainFireSegment['MaxX'],
                'MaxY': mainFireSegment['MaxY'],
                'Score': mainFireSegment['score'],
                'ModelId': self.modelId,
                'ModelLevel': 0,
            }
            dbRows.append(dbRow)
        # update image_spec to restrict search area centered around detected region
        centerX = int((mainFireSegment['MinX'] + mainFireSegment['MaxX']) / 2)
        sizeX = 299
        (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX, sizeX, 0, 1e9)
        centerY = int((mainFireSegment['MinY'] + mainFireSegment['MaxY']) / 2)
        sizeY = 299
        (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY, sizeY, 0, 1e9)
        last_image_spec['startX'] = newMinX
        last_image_spec['endX'] = newMaxX
        last_image_spec['startY'] = newMinY
        last_image_spec['endY'] = newMaxY

        # ensure all the confirmation policies also detect fire
        for counter, confirmationPolicy in enumerate(self.confirmationPolicies):
            # no need to check shifts as these are already double checking confirmation policies
            detectionResult = confirmationPolicy.detect(image_spec, checkShifts=checkShifts, fetchDiff=fetchDiff)
            # logging.warning('Multi confirm %s res %s', counter, detectionResult['fireSegment'])
            if not detectionResult['fireSegment']:
                # return result as is if last policy or no fire detected
                if len(dbRows) > 0:
                    self.dbManager.add_data('multi_policy', dbRows)
                return detectionResult
            else:
                # overwrite location with confirmationModel's location
                mainFireSegment['MinX'] = detectionResult['fireSegment']['MinX']
                mainFireSegment['MinY'] = detectionResult['fireSegment']['MinY']
                mainFireSegment['MaxX'] = detectionResult['fireSegment']['MaxX']
                mainFireSegment['MaxY'] = detectionResult['fireSegment']['MaxY']
                if not self.stateless:
                    dbRow = {
                        'CameraName': last_image_spec['cameraID'],
                        'Heading': last_image_spec['heading'],
                        'Timestamp': last_image_spec['timestamp'],
                        'MinX': mainFireSegment['MinX'],
                        'MinY': mainFireSegment['MinY'],
                        'MaxX': mainFireSegment['MaxX'],
                        'MaxY': mainFireSegment['MaxY'],
                        'Score': detectionResult['fireSegment']['score'],
                        'ModelId': self.modelId,
                        'ModelLevel': counter + 1,
                    }
                    dbRows.append(dbRow)

        if len(dbRows) > 0:
            self.dbManager.add_data('multi_policy', dbRows)

        return mainDetectionResult
