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
from PIL import Image, ImageFile, ImageDraw, ImageFont
import logging
import shutil
import datetime
import math
import time
import tempfile

import tensorflow as tf

class InceptionV3AndHistoricalThreshold:

    SEQUENCE_LENGTH = 1
    SEQUENCE_SPACING_MIN = None

    def __init__(self, args, dbManager, camArchives, minusMinutes, useArchivedImages):
        self.dbManager = dbManager
        self.args = args
        self.camArchives = camArchives
        self.minusMinutes = minusMinutes
        self.useArchivedImages = useArchivedImages
        # if model is on GCS, download it locally first
        modelLocation = settings.model_file
        self.modelId = '/'.join(modelLocation.split('/')[-2:]) # the last two dirpath components
        gcsModel = goog_helper.parseGCSPath(modelLocation)
        if gcsModel:
            tmpDir = tempfile.TemporaryDirectory()
            goog_helper.downloadBucketDir(gcsModel['bucket'], gcsModel['name'], tmpDir.name)
            modelLocation = tmpDir.name
        self.model = tf_helper.loadModel(modelLocation)


    def _segmentImage(self, imgPath):
        """Segment the given image into sections to for smoke classificaiton

        Args:
            imgPath (str): filepath of the image

        Returns:
            List of dictionary containing information on each segment
        """
        img = Image.open(imgPath)
        crops, segments = rect_to_squares.cutBoxesArray(img)
        img.close()
        return crops, segments


    def _segmentAndClassify(self, imgPath):
        """Segment the given image into squares and classify each square

        Args:
            imgPath (str): filepath of the image to segment and clasify

        Returns:
            list of segments with scores sorted by decreasing score
        """
        crops, segments = self._segmentImage(imgPath)
        if len(crops) == 0:
            return []
        tf_helper.classifySegments(self.model, crops, segments)

        segments.sort(key=lambda x: -x['score'])
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
                    postivesDateDir = goog_helper.dateSubDir(settings.positivesDir)
                    cropImgName = imgNameNoExt + '_Crop_' + segmentInfo['coordStr'] + '.jpg'
                    cropImgPath = os.path.join(str(ppath.parent), cropImgName)
                    if not imgObj:
                        imgObj = Image.open(imgPath)
                    cropped_img = imgObj.crop(segmentInfo['coords'])
                    cropped_img.save(cropImgPath, format='JPEG')
                    cropped_img.close()
                    goog_helper.copyFile(cropImgPath, postivesDateDir)
                    os.remove(cropImgPath)
                positiveSegments += 1

        if positiveSegments > 0:
            logging.warning('Found %d positives in image %s', positiveSegments, ppath.name)
        if imgObj:
            imgObj.close()


    def _recordScores(self, camera, timestamp, segments):
        """Record the smoke scores for each segment into SQL DB

        Args:
            camera (str): camera name
            timestamp (int):
            segments (list): List of dictionary containing information on each segment
        """
        dt = datetime.datetime.fromtimestamp(timestamp)
        secondsInDay = (dt.hour * 60 + dt.minute) * 60 + dt.second

        dbRows = []
        for segmentInfo in segments:
            dbRow = {
                'CameraName': camera,
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


    def _postFilter(self, camera, timestamp, segments):
        """Post classification filter to reduce false positives

        Many times smoke classification scores segments with haze and glare
        above 0.5.  Haze and glare occur tend to occur at similar time over
        multiple days, so this filter raises the threshold based on the max
        smoke score for same segment at same time of day over the last few days.
        Score must be > halfway between max value and 1.  Also, minimum .1 above max.

        Args:
            camera (str): camera name
            timestamp (int):
            segments (list): Sorted List of dictionary containing information on each segment

        Returns:
            Dictionary with information for the segment most likely to be smoke
            or None
        """
        # # enable the next few lines fakes a detection to test alerting functionality
        # maxFireSegment = segments[0]
        # maxFireSegment['HistAvg'] = 0.1
        # maxFireSegment['HistMax'] = 0.2
        # maxFireSegment['HistNumSamples'] = 10
        # maxFireSegment['AdjScore'] = 0.3
        # return maxFireSegment

        # segments is sorted, so skip all work if max score is < .5
        if segments[0]['score'] < .5:
            return None

        sqlTemplate = """SELECT MinX,MinY,MaxX,MaxY,count(*) as cnt, avg(score) as avgs, max(score) as maxs FROM scores
        WHERE CameraName='%s' and Timestamp > %s and Timestamp < %s and SecondsInDay > %s and SecondsInDay < %s
        GROUP BY MinX,MinY,MaxX,MaxY"""

        dt = datetime.datetime.fromtimestamp(timestamp)
        secondsInDay = (dt.hour * 60 + dt.minute) * 60 + dt.second
        sqlStr = sqlTemplate % (camera, timestamp - 60*60*int(24*3.5), timestamp - 60*60*12, secondsInDay - 60*60, secondsInDay + 60*60)
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


    def _recordDetection(self, camera, timestamp, imgPath, fireSegment):
        """Record that a smoke/fire has been detected

        Record the detection with useful metrics in 'detections' table in SQL DB.
        Also, upload image file to google cloud

        Args:
            camera (str): camera name
            timestamp (int):
            imgPath: filepath of the image
            fireSegment (dictionary): dictionary with information for the segment with fire/smoke

        Returns:
            File IDs for the uploaded image file
        """
        logging.warning('Fire detected by camera %s, image %s, segment %s', camera, imgPath, str(fireSegment))
        # copy/upload file to detection dir
        detectionsDateDir = goog_helper.dateSubDir(settings.detectionsDir)
        fileID = goog_helper.copyFile(imgPath, detectionsDateDir)
        logging.warning('Uploaded to detections folder %s', fileID)

        dbRow = {
            'CameraName': camera,
            'Timestamp': timestamp,
            'MinX': fireSegment['MinX'],
            'MinY': fireSegment['MinY'],
            'MaxX': fireSegment['MaxX'],
            'MaxY': fireSegment['MaxY'],
            'Score': fireSegment['score'],
            'HistAvg': fireSegment['HistAvg'],
            'HistMax': fireSegment['HistMax'],
            'HistNumSamples': fireSegment['HistNumSamples'],
            'ImageID': fileID,
            'ModelId': self.modelId
        }
        self.dbManager.add_data('detections', dbRow)
        return fileID


    def detect(self, image_spec):
        # This detection policy only uses a single image, so just take the last one
        last_image_spec = image_spec[-1]
        imgPath = last_image_spec['path']
        timestamp = last_image_spec['timestamp']
        cameraID = last_image_spec['cameraID']
        detectionResult = {
            'fireSegment': None
        }
        segments = self._segmentAndClassify(imgPath)
        detectionResult['timeMid'] = time.time()
        if len(segments) == 0: # happens sometimes when camera is malfunctioning
            return detectionResult
        if self.args.collectPositves:
            self._collectPositves(imgPath, segments)
        if not self.useArchivedImages:
            self._recordScores(cameraID, timestamp, segments)
            fireSegment = self._postFilter(cameraID, timestamp, segments)
            if fireSegment:
                self._recordDetection(cameraID, timestamp, imgPath, fireSegment)
                detectionResult['fireSegment'] = fireSegment
        logging.warning('Highest score for camera %s: %f' % (cameraID, segments[0]['score']))

        return detectionResult
