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

This detection policy segments images into 299x299 squares that are
evaluated using a model with InceptionV3 architecture to detect smoke, then
followed go through a filter that raises the thresholds based on recent
historical scores.

"""

import os, sys
from firecam.lib import settings
from firecam.lib import goog_helper
from firecam.lib import tf_helper
from firecam.lib import rect_to_squares

import pathlib
from PIL import Image
import logging
import datetime
import time
import random

import tensorflow as tf

testMode = False

class InceptionV3AndHistoricalThreshold:

    SEQUENCE_LENGTH = 1
    SEQUENCE_SPACING_MIN = None

    def __init__(self, args, dbManager, stateless, modelLocation=None):
        self.dbManager = dbManager
        self.args = args
        self.minusMinutes = 0
        self.stateless = stateless
        if not modelLocation:
            modelLocation = settings.model_file
        self.modelId = '/'.join(modelLocation.split('/')[-2:]) # the last two dirpath components
        logging.warning('InceptionV3 init %s', self.modelId)
        if testMode:
            self.model = None
        else:
            self.model = tf_helper.loadModel(modelLocation)


    def _segmentImage(self, imgPath, startX, endX, startY, endY):
        """Segment the given image into sections to for smoke classificaiton

        Args:
            imgPath (str): filepath of the image

        Returns:
            List of dictionary containing information on each segment
        """
        img = Image.open(imgPath)
        crops, segments = rect_to_squares.cutBoxesArray(img, startX, endX, startY, endY)
        img.close()
        return crops, segments


    def _segmentAndClassify(self, imgPath, startX, endX, startY, endY):
        """Segment the given image into squares and classify each square

        Args:
            imgPath (str): filepath of the image to segment and clasify

        Returns:
            list of segments with scores sorted by decreasing score
        """
        # logging.warning('SAC %s: %s, %s, %s, %s, %s', self.modelId, startX, startY, endX, endY, imgPath)
        crops, segments = self._segmentImage(imgPath, startX, endX, startY, endY)
        if len(crops) == 0:
            return []
        # testMode fakes all scores
        if testMode:
            for segmentInfo in segments:
                segmentInfo['score'] = random.random()
        else:
            tf_helper.classifySegments(self.model, crops, segments)

        segments.sort(key=lambda x: -x['score'])
        # logging.warning('SAC top: %s', segments[0])
        return segments


    def _collectPositves(self, imgPath, segments):
        """Collect all positive scoring segments

        Copy the images for all segments that score highter than > .5 to folder
        settings.positivesDir. These will be used to train future models.
        Also, copy the full image for reference.

        Args:
            imgPath (str): path name for main image
            segments (list): List of dictionary containing information on each segment
        """
        positiveSegments = 0
        ppath = pathlib.PurePath(imgPath)
        imgNameNoExt = str(os.path.splitext(ppath.name)[0])
        imgObj = None
        for segmentInfo in segments:
            if segmentInfo['score'] > .5:
                if settings.positivesDir:
                    positivesPrefix = settings.positivesDir if settings.positivesDir[-1] != '/' else settings.positivesDir[:-1]
                    postivesModelDir = positivesPrefix + '/' + self.modelId.replace('/', '_')
                    postivesDateDir = goog_helper.dateSubDir(postivesModelDir)
                    cropImgName = imgNameNoExt + '_Crop_' + segmentInfo['coordStr'] + '.jpg'
                    cropImgPath = os.path.join(str(ppath.parent), cropImgName)
                    if not imgObj:
                        imgObj = Image.open(imgPath)
                    cropped_img = imgObj.crop(segmentInfo['coords'])
                    cropped_img.save(cropImgPath, format='JPEG', quality=95)
                    cropped_img.close()
                    goog_helper.copyFile(cropImgPath, postivesDateDir)
                    os.remove(cropImgPath)
                positiveSegments += 1

        if positiveSegments > 0:
            logging.warning('Found %d positives in image %s', positiveSegments, ppath.name)
        if imgObj:
            imgObj.close()


    def _recordScores(self, cameraID, heading, timestamp, segments):
        """Record the smoke scores for each segment into SQL DB

        Args:
            cameraID (str): camera ID
            heading (int): direction camera is facing
            timestamp (int):
            segments (list): List of dictionary containing information on each segment
        """
        dt = datetime.datetime.fromtimestamp(timestamp)
        secondsInDay = (dt.hour * 60 + dt.minute) * 60 + dt.second

        dbRows = []
        for segmentInfo in segments:
            dbRow = {
                'CameraName': cameraID,
                'Heading': heading,
                'Timestamp': timestamp,
                'MinX': segmentInfo['MinX'],
                'MinY': segmentInfo['MinY'],
                'MaxX': segmentInfo['MaxX'],
                'MaxY': segmentInfo['MaxY'],
                'Score': segmentInfo['score'],
                'MinusMinutes': self.minusMinutes,
                'SecondsInDay': secondsInDay,
                'ModelId': self.modelId
            }
            dbRows.append(dbRow)
        self.dbManager.add_data('scores', dbRows)


    def _postFilter(self, cameraID, heading, timestamp, segments):
        """Post classification filter to reduce false positives

        Many times smoke classification scores segments with haze and glare
        above 0.5.  Haze and glare occur tend to occur at similar time over
        multiple days, so this filter raises the threshold based on the max
        smoke score for same segment at same time of day over the last few days.
        Score must be > halfway between max value and 1.  Also, minimum .1 above max.

        Args:
            cameraID (str): camera ID
            heading (int): direction camera is facing
            timestamp (int):
            segments (list): Sorted List of dictionary containing information on each segment

        Returns:
            Dictionary with information for the segment most likely to be smoke
            or None
        """
        # testMode fakes a detection to test alerting functionality
        if testMode:
            maxFireSegment = segments[0]
            maxFireSegment['AdjScore'] = 0.3
            return maxFireSegment

        # segments is sorted, so skip all work if max score is < .5
        if segments[0]['score'] < .5:
            return None

        sqlTemplate = """SELECT MinX,MinY,MaxX,MaxY,count(*) as cnt, avg(score) as avgs, max(score) as maxs FROM scores
        WHERE CameraName='%s' and Heading=%s and Timestamp > %s and Timestamp < %s and SecondsInDay > %s and SecondsInDay < %s
        and ModelId='%s'
        GROUP BY MinX,MinY,MaxX,MaxY"""

        dt = datetime.datetime.fromtimestamp(timestamp)
        secondsInDay = (dt.hour * 60 + dt.minute) * 60 + dt.second
        sqlStr = sqlTemplate % (cameraID, heading, timestamp - 60*60*int(24*7.5), timestamp - 60*60*12, secondsInDay - 60*60, secondsInDay + 60*60, self.modelId)
        # print('sql', sqlStr, timestamp)
        dbResult = self.dbManager.query(sqlStr)
        # if len(dbResult) > 0:
        #     print('post filter result', dbResult)
        maxFireSegment = None
        maxFireScore = 0
        for segmentInfo in segments:
            if segmentInfo['score'] < .5: # segments is sorted. we've reached end of segments >= .5
                break
            for row in dbResult:
                if (row['minx'] == segmentInfo['MinX'] and row['miny'] == segmentInfo['MinY'] and
                    row['maxx'] == segmentInfo['MaxX'] and row['maxy'] == segmentInfo['MaxY']):
                    threshold = (row['maxs'] + 1)/2 # threshold is halfway between max and 1
                    # Segments with historical value above 0.8 are too noisy, so discard them by setting
                    # threshold at least .2 above max.  Also requires .7 to reach .9 vs just .85
                    threshold = max(threshold, row['maxs'] + 0.2)
                    # print('thresh', row['minx'], row['miny'], row['maxx'], row['maxy'], row['maxs'], threshold)
                    if (segmentInfo['score'] > threshold) and (segmentInfo['score'] > maxFireScore):
                        maxFireScore = segmentInfo['score']
                        maxFireSegment = segmentInfo
                        maxFireSegment['HistAvg'] = row['avgs']
                        maxFireSegment['HistMax'] = row['maxs']
                        maxFireSegment['HistNumSamples'] = row['cnt']
                        maxFireSegment['AdjScore'] = (segmentInfo['score'] - threshold) / (1 - threshold)

        return maxFireSegment


    def detect(self, image_spec, checkShifts=False, silent=False, fetchDiff=None):
        # This detection policy only uses a single image, so just take the last one
        last_image_spec = image_spec[-1]
        imgPath = last_image_spec['path']
        timestamp = last_image_spec['timestamp']
        cameraID = last_image_spec['cameraID']
        if not self.stateless:
            heading = last_image_spec['heading']
        detectionResult = {
            'fireSegment': None
        }
        startX = last_image_spec['startX'] if 'startX' in last_image_spec else 0
        endX = last_image_spec['endX'] if 'endX' in last_image_spec else None
        startY = last_image_spec['startY'] if 'startY' in last_image_spec else 0
        endY = last_image_spec['endY'] if 'endY' in last_image_spec else None
        segments = self._segmentAndClassify(imgPath, startX, endX, startY, endY)
        detectionResult['segments'] = segments
        detectionResult['timeMid'] = time.time()
        if len(segments) == 0: # happens sometimes when camera is malfunctioning
            return detectionResult
        if getattr(self.args, 'collectPositves', None):
            self._collectPositves(imgPath, segments)
        fireSegment = None
        if self.stateless:
            if segments[0]['score'] > 0.5:
                fireSegment = segments[0]
        else:
            self._recordScores(cameraID, heading, timestamp, segments)
            fireSegment = self._postFilter(cameraID, heading, timestamp, segments)
        if fireSegment and checkShifts:
            fireSegment = fireSegment.copy() # copy so segments array won't be affected
            # check shifted images
            sizeX = fireSegment['MaxX'] - fireSegment['MinX']
            sizeY = fireSegment['MaxY'] - fireSegment['MinY']
            # note it's OK if start[XY] goes negative or end[XY] go beyond image size
            # because they are validated against limits later
            startX = fireSegment['MinX'] - int(sizeX / 3)
            endX = fireSegment['MaxX'] + int(sizeX / 3)
            startY = fireSegment['MinY'] - int(sizeY / 3)
            endY = fireSegment['MaxY'] + int(sizeY / 3)
            newSegments = self._segmentAndClassify(imgPath, startX, endX, startY, endY)
            segments += newSegments
            # intersect fireSegment
            if newSegments[0]['score'] > 0.5:
                for segment in newSegments:
                    if segment['score'] > 0.5:
                        fireSegment['MinX'] = max(fireSegment['MinX'], segment['MinX'])
                        fireSegment['MinY'] = max(fireSegment['MinY'], segment['MinY'])
                        fireSegment['MaxX'] = min(fireSegment['MaxX'], segment['MaxX'])
                        fireSegment['MaxY'] = min(fireSegment['MaxY'], segment['MaxY'])
            else:
                fireSegment = None # don't report fire
        detectionResult['fireSegment'] = fireSegment
        if not silent:
            logging.warning('Highest score for camera %s: %f' % (cameraID, segments[0]['score']))

        return detectionResult
