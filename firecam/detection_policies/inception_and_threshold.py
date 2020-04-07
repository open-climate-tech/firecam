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

This detection policy segments images into 299x299 squares that are
evaluated using a model with InceptionV3 architecture to detect smoke, then
followed go through a filter that raises the thresholds based on recent
historical scores.

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import goog_helper
import tf_helper
import rect_to_squares

import pathlib
from PIL import Image, ImageFile, ImageDraw, ImageFont
import logging
import shutil
import datetime
import math
import time

import tensorflow as tf

class InceptionV3AndHistoricalThreshold:

    SEQUENCE_LENGTH = 1
    SEQUENCE_SPACING_MIN = None

    def __init__(self, settings, args, google_services, dbManager, tfConfig, camArchives, minusMinutes, useArchivedImages):
        self.dbManager = dbManager
        self.args = args
        self.google_services = google_services
        self.camArchives = camArchives
        self.minusMinutes = minusMinutes
        self.useArchivedImages = useArchivedImages
        self.graph = tf_helper.load_graph(settings.model_file)
        self.labels = tf_helper.load_labels(settings.labels_file)
        self.tfSession = tf.Session(graph=self.graph, config=tfConfig)


    def _segmentImage(self, imgPath):
        """Segment the given image into sections to for smoke classificaiton

        Args:
            imgPath (str): filepath of the image

        Returns:
            List of dictionary containing information on each segment
        """
        img = Image.open(imgPath)
        ppath = pathlib.PurePath(imgPath)
        segments = rect_to_squares.cutBoxes(img, str(ppath.parent), imgPath)
        img.close()
        return segments


    def _segmentAndClassify(self, imgPath):
        """Segment the given image into squares and classify each square

        Args:
            imgPath (str): filepath of the image to segment and clasify

        Returns:
            list of segments with scores sorted by decreasing score
        """
        segments = self._segmentImage(imgPath)
        # print('si', segments)
        tf_helper.classifySegments(self.tfSession, self.graph, self.labels, segments)
        segments.sort(key=lambda x: -x['score'])
        return segments


    def _collectPositves(self, imgPath, segments):
        """Collect all positive scoring segments

        Copy the images for all segments that score highter than > .5 to google drive folder
        settings.positivePictures. These will be used to train future models.
        Also, copy the full image for reference.

        Args:
            imgPath (str): path name for main image
            segments (list): List of dictionary containing information on each segment
        """
        positiveSegments = 0
        ppath = pathlib.PurePath(imgPath)
        googleDrive = self.google_services['drive']
        for segmentInfo in segments:
            if segmentInfo['score'] > .5:
                if hasattr(settings, 'positivePicturesDir'):
                    pp = pathlib.PurePath(segmentInfo['imgPath'])
                    destPath = os.path.join(settings.positivePicturesDir, pp.name)
                    shutil.copy(segmentInfo['imgPath'], destPath)
                else:
                    goog_helper.uploadFile(googleDrive, settings.positivePictures, segmentInfo['imgPath'])
                positiveSegments += 1

        if positiveSegments > 0:
            # Commenting out saving full images for now to reduce data
            # goog_helper.uploadFile(googleDrive, settings.positivePictures, imgPath)
            logging.warning('Found %d positives in image %s', positiveSegments, ppath.name)


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
                'SecondsInDay': secondsInDay
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

        return maxFireSegment


    def _drawRect(self, imgDraw, x0, y0, x1, y1, width, color):
        for i in range(width):
            imgDraw.rectangle((x0+i,y0+i,x1-i,y1-i),outline=color)


    def _drawFireBox(self, imgPath, fireSegment):
        """Draw bounding box with fire detection with score on image

        Stores the resulting annotated image as new file

        Args:
            imgPath (str): filepath of the image

        Returns:
            filepath of new image file
        """
        img = Image.open(imgPath)
        imgDraw = ImageDraw.Draw(img)
        x0 = fireSegment['MinX']
        y0 = fireSegment['MinY']
        x1 = fireSegment['MaxX']
        y1 = fireSegment['MaxY']
        centerX = (x0 + x1)/2
        centerY = (y0 + y1)/2
        color = "red"
        lineWidth=3
        self._drawRect(imgDraw, x0, y0, x1, y1, lineWidth, color)

        fontSize=80
        font = ImageFont.truetype(os.path.join(settings.fuegoRoot, 'lib/Roboto-Regular.ttf'), size=fontSize)
        scoreStr = '%.2f' % fireSegment['score']
        textSize = imgDraw.textsize(scoreStr, font=font)
        imgDraw.text((centerX - textSize[0]/2, centerY - textSize[1]), scoreStr, font=font, fill=color)

        color = "blue"
        fontSize=70
        font = ImageFont.truetype(os.path.join(settings.fuegoRoot, 'lib/Roboto-Regular.ttf'), size=fontSize)
        scoreStr = '%.2f' % fireSegment['HistMax']
        textSize = imgDraw.textsize(scoreStr, font=font)
        imgDraw.text((centerX - textSize[0]/2, centerY), scoreStr, font=font, fill=color)

        filePathParts = os.path.splitext(imgPath)
        annotatedFile = filePathParts[0] + '_Score' + filePathParts[1]
        img.save(annotatedFile, format="JPEG")
        del imgDraw
        img.close()
        return annotatedFile


    def _recordDetection(self, camera, timestamp, imgPath, annotatedFile, fireSegment):
        """Record that a smoke/fire has been detected

        Record the detection with useful metrics in 'detections' table in SQL DB.
        Also, upload image file to google drive

        Args:
            camera (str): camera name
            timestamp (int):
            imgPath: filepath of the image
            annotatedFile: filepath of the image with annotated box and score
            fireSegment (dictionary): dictionary with information for the segment with fire/smoke

        Returns:
            List of Google drive IDs for the uploaded image files
        """
        logging.warning('Fire detected by camera %s, image %s, segment %s', camera, imgPath, str(fireSegment))
        # upload file to google drive detection dir
        driveFileIDs = []
        googleDrive = self.google_services['drive']
        driveFile = goog_helper.uploadFile(googleDrive, settings.detectionPictures, imgPath)
        if driveFile:
            driveFileIDs.append(driveFile['id'])
        driveFile = goog_helper.uploadFile(googleDrive, settings.detectionPictures, annotatedFile)
        if driveFile:
            driveFileIDs.append(driveFile['id'])
        logging.warning('Uploaded to google drive detections folder %s', str(driveFileIDs))

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
            'ImageID': driveFileIDs[0] if driveFileIDs else ''
        }
        self.dbManager.add_data('detections', dbRow)
        return driveFileIDs


    def detect(self, image_spec):
        # This detection policy only uses a single image, so just take the last one
        last_image_spec = image_spec[-1]
        imgPath = last_image_spec['path']
        timestamp = last_image_spec['timestamp']
        cameraID = last_image_spec['cameraID']
        detectionResult = {
            'annotatedFile': '',
            'fireSegment': None
        }
        annotatedFile = None

        segments = self._segmentAndClassify(imgPath)
        detectionResult['timeMid'] = time.time()
        if self.args.collectPositves:
            self._collectPositves(imgPath, segments)
        if not self.useArchivedImages:
            self._recordScores(cameraID, timestamp, segments)
            fireSegment = self._postFilter(cameraID, timestamp, segments)
            if fireSegment:
                annotatedFile = self._drawFireBox(imgPath, fireSegment)
                driveFileIDs = self._recordDetection(cameraID, timestamp, imgPath, annotatedFile, fireSegment)
                detectionResult['fireSegment'] = fireSegment
                detectionResult['annotatedFile'] = annotatedFile
                detectionResult['driveFileIDs'] = driveFileIDs
        logging.warning('Highest score for camera %s: %f' % (cameraID, segments[0]['score']))
        for segmentInfo in segments:
            os.remove(segmentInfo['imgPath'])

        return detectionResult


