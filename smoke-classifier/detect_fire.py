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

This is the main code for reading images from webcams and detecting fires

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive

from firecam.lib import rect_to_squares
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' # quiet down tensorflow logging (must be done before tf_helper)
from firecam.lib import tf_helper
from firecam.lib import db_manager
from firecam.lib import email_helper
from firecam.lib import sms_helper
from firecam.detection_policies import policies

import logging
import pathlib
import tempfile
import shutil
import time, datetime, dateutil.parser
import random
import math
import re
import hashlib
import gc
from urllib.request import urlretrieve
import tensorflow as tf
from PIL import Image, ImageFile, ImageDraw, ImageFont
ImageFile.LOAD_TRUNCATED_IMAGES = True
import ffmpeg


def getNextImage(dbManager, cameras, cameraID=None):
    """Gets the next image to check for smoke

    Uses a shared counter being updated by all cooperating detection processes
    to index into the list of cameras to download the image to a local
    temporary directory

    Args:
        dbManager (DbManager):
        cameras (list): list of cameras
        cameraID (str): optional specific camera to get image from

    Returns:
        Tuple containing camera name, current timestamp, and filepath of the image
    """
    if getNextImage.tmpDir == None:
        getNextImage.tmpDir = tempfile.TemporaryDirectory()
        logging.warning('TempDir %s', getNextImage.tmpDir.name)

    if cameraID:
        camera = list(filter(lambda x: x['name'] == cameraID, cameras))[0]
    else:
        index = dbManager.getNextSourcesCounter() % len(cameras)
        camera = cameras[index]
    timestamp = int(time.time())
    imgPath = img_archive.getImgPath(getNextImage.tmpDir.name, camera['name'], timestamp)
    # logging.warning('urlr %s %s', camera['url'], imgPath)
    try:
        urlretrieve(camera['url'], imgPath)
    except Exception as e:
        logging.error('Error fetching image from %s %s', camera['name'], str(e))
        return getNextImage(dbManager, cameras)
    md5 = hashlib.md5(open(imgPath, 'rb').read()).hexdigest()
    if ('md5' in camera) and (camera['md5'] == md5) and not cameraID:
        logging.warning('Camera %s image unchanged', camera['name'])
        # skip to next camera
        return getNextImage(dbManager, cameras)
    camera['md5'] = md5
    return (camera['name'], timestamp, imgPath, md5)
getNextImage.tmpDir = None

# XXXXX Use a fixed stable directory for testing
# from collections import namedtuple
# Tdir = namedtuple('Tdir', ['name'])
# getNextImage.tmpDir = Tdir('c:/tmp/dftest')


def stretchBounds(minOrig, maxOrig, limit):
    """Stretch the given range to triple size by extending on both sides

    Args:
        minOrig (int): minimum value of range
        maxOrig (int): maximum value of range
        limit (int): limit that cannot be crossed during stretching

    Returns:
        Tuple (min, max) of stretched range
    """
    size = maxOrig - minOrig
    finalSize = 3*size
    finalSize = math.ceil(finalSize/2)*2 # make even
    if (minOrig - size < 0):
        minNew = 0
        maxNew = min(minNew + finalSize, limit)
    elif (maxOrig + size >= limit - 1):  # (limit - 1 to workaround rounding up to even)
        maxNew = limit
        minNew = max(maxNew - finalSize, 0)
    else:
        minNew = minOrig - size
        maxNew = min(minNew + finalSize, limit)
    return (minNew, maxNew)


def drawRect(imgDraw, x0, y0, x1, y1, width, color):
    for i in range(width):
        imgDraw.rectangle((x0 + i, y0 + i, x1 - i, y1 -i), outline=color)


def drawFireBox(img, destPath, fireSegment, x0, y0, x1, y1, timestamp=None, writeScores=False, color='red'):
    """Draw bounding box with fire detection and optionally write scores

    Also watermarks the image and stores the resulting annotated image as new file

    Args:
        img (Image): Image object to draw on
        destPath (str): filepath where to write the output image
        fireSegment (dict): dict describing segment with fire
        x0, y0, x1, y1 (int): coordinates of fire segment
        writeScores (bool): if set to True, the scores are written on the image as well
    """
    imgDraw = ImageDraw.Draw(img)

    lineWidth=3
    drawRect(imgDraw, x0, y0, x1, y1, lineWidth, color)

    fontPath = os.path.join(str(pathlib.Path(__file__).parent.parent), 'firecam/data/Roboto-Regular.ttf')
    if writeScores:
        # Write ML score above towards left of the fire box
        color = "red"
        fontSize=70
        font = ImageFont.truetype(fontPath, size=fontSize)
        scoreStr = '%.2f' % fireSegment['score']
        textSize = imgDraw.textsize(scoreStr, font=font)
        imgDraw.text((x0, y0 - textSize[1]), scoreStr, font=font, fill=color)

        # Write historical max value above towards right of the fire box
        color = "blue"
        fontSize=60
        font = ImageFont.truetype(fontPath, size=fontSize)
        scoreStr = '%.2f' % fireSegment['HistMax']
        textSize = imgDraw.textsize(scoreStr, font=font)
        imgDraw.text((x1 - textSize[0], y0 - textSize[1]), scoreStr, font=font, fill=color)

    if timestamp:
        fontSize=32
        font = ImageFont.truetype(fontPath, size=fontSize)
        timeStr = datetime.datetime.fromtimestamp(timestamp).isoformat()
        # first little bit of black outline
        color = "black"
        for i in range(0,5):
            for j in range(0,5):
                imgDraw.text((i, j), timeStr, font=font, fill=color)

        # now actual data in orange
        color = "orange"
        imgDraw.text((2, 2), timeStr, font=font, fill=color)

    # "watermark" the image
    color = "orange"
    fontSize=32
    font = ImageFont.truetype(fontPath, size=fontSize)
    imgDraw.text((20, img.size[1] - fontSize - 20), "Open Climate Tech - Wildfire", font=font, fill=color)

    img.save(destPath, format="JPEG")
    del imgDraw


def genAnnotatedImages(constants, cameraID, timestamp, imgPath, fireSegment):
    """Generate annotated images (one cropped video, and other full size image)

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        imgPath (str): filepath of the image
        fireSegment (dict): dict describing segment with fire

    Returns:
        Tuple (str, str): filepaths of cropped and full size annotated iamges
    """
    filePathParts = os.path.splitext(imgPath)
    img = Image.open(imgPath)
    x0 = fireSegment['MinX'] if 'MinX' in fireSegment else 0
    y0 = fireSegment['MinY'] if 'MinY' in fireSegment else 0
    x1 = fireSegment['MaxX'] if 'MaxX' in fireSegment else img.size[0]
    y1 = fireSegment['MaxY'] if 'MaxY' in fireSegment else img.size[0]

    (cropX0, cropX1) = stretchBounds(x0, x1, img.size[0])
    (cropY0, cropY1) = stretchBounds(y0, y1, img.size[1])
    cropCoords = (cropX0, cropY0, cropX1, cropY1)
    # get images spanning a few minutes so reviewers can evaluate based on progression
    startTimeDT = datetime.datetime.fromtimestamp(timestamp - 4*60)
    endTimeDT = datetime.datetime.fromtimestamp(timestamp - 1*60)

    with tempfile.TemporaryDirectory() as tmpDirName:
        oldImages = img_archive.getHpwrenImages(constants['googleServices'], settings, tmpDirName,
                                                constants['camArchives'], cameraID, startTimeDT, endTimeDT, 1)
        imgSequence = oldImages or []
        imgSequence.append(imgPath)
        mspecPath = os.path.join(tmpDirName, 'mspec.txt')
        mspecFile = open(mspecPath, 'w')
        for (i, imgFile) in enumerate(imgSequence):
            finalImg = (i == len(imgSequence) - 1)
            imgParsed = img_archive.parseFilename(imgFile)
            cropName = 'img' + ("%03d" % i) + filePathParts[1]
            croppedPath = os.path.join(tmpDirName, cropName)
            imgSeq = Image.open(imgFile)
            croppedImg = imgSeq.crop(cropCoords)
            color = 'red' if finalImg else 'yellow'
            drawFireBox(croppedImg, croppedPath, fireSegment, x0 - cropX0, y0 - cropY0, x1 - cropX0, y1 - cropY0, timestamp=imgParsed['unixTime'], color=color)
            imgSeq.close()
            croppedImg.close()
            mspecFile.write("file '" + croppedPath + "'\n")
            mspecFile.write('duration 1\n')
            if finalImg:
                mspecFile.write("file '" + croppedPath + "'\n")
        mspecFile.close()
        # now make movie from this sequence of cropped images
        moviePath = filePathParts[0] + '_AnnCrop_' + 'x'.join(list(map(lambda x: str(x), cropCoords))) + '.mp4'
        (
            ffmpeg.input(mspecPath, format='concat', safe=0)
                .filter('fps', fps=25, round='up')
                .output(moviePath, pix_fmt='yuv420p').run()
        )

    annotatedPath = filePathParts[0] + '_Ann' + filePathParts[1]
    drawFireBox(img, annotatedPath, fireSegment, x0, y0, x1, y1)
    img.close()

    return (moviePath, annotatedPath)


def isDuplicateAlert(dbManager, cameraID, timestamp):
    """Check if alert has been recently sent out for given camera

    Args:
        dbManager (DbManager):
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken

    Returns:
        True if this is a duplicate alert, False otherwise
    """
    # Only alert if there has not been a detection in the last hour.  This prevents spam
    # from long lasting fires.
    sqlTemplate = """SELECT * FROM detections
    where CameraName='%s' and timestamp > %s and timestamp < %s"""
    sqlStr = sqlTemplate % (cameraID, timestamp - 60*60, timestamp)

    dbResult = dbManager.query(sqlStr)
    if len(dbResult) > 0:
        logging.warning('Supressing new alert due to recent detection')
        return True
    return False


def updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, fireSegment):
    """Add new entry to Alerts table

    Args:
        dbManager (DbManager):
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        annotatedUrl: Public URL for annotated iamge
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
    """
    dbRow = {
        'CameraName': cameraID,
        'Timestamp': timestamp,
        'AdjScore': fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score'],
        'ImageID': annotatedUrl,
        'CroppedID': croppedUrl
    }
    dbManager.add_data('alerts', dbRow)


def pubsubFireNotification(cameraID, timestamp, croppedUrl, annotatedUrl, fireSegment):
    """Send a pubsub notification for a potential new fire

    Sends pubsub message with information about the camera and fire score includeing
    image attachments

    Args:
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        annotatedUrl: Public URL for annotated iamge
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
    """
    message = {
        'timestamp': timestamp,
        'cameraID': cameraID,
        "mlScore": str(fireSegment['score']),
        "histMax": str(fireSegment['HistMax'] if 'HistMax' in fireSegment else 0),
        "adjScore": str(fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score']),
        'croppedUrl': croppedUrl,
        'annotatedUrl': annotatedUrl
    }
    goog_helper.publish(message)


def emailFireNotification(constants, cameraID, timestamp, imgPath, annotatedFile, fireSegment):
    """Send an email alert for a potential new fire

    Send email with information about the camera and fire score includeing
    image attachments

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        imgPath: filepath of the original image
        annotatedFile: filepath of the annotated image
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
    """
    dbManager = constants['dbManager']
    subject = 'Possible (%d%%) fire in camera %s' % (int(fireSegment['score']*100), cameraID)
    body = 'Please check the attached images for fire.'

    # emails are sent from settings.fuegoEmail and bcc to everyone with active emails in notifications SQL table
    dbResult = dbManager.getNotifications(filterActiveEmail = True)
    emails = [x['email'] for x in dbResult]
    if len(emails) > 0:
        # attach images spanning a few minutes so reviewers can evaluate based on progression
        startTimeDT = datetime.datetime.fromtimestamp(timestamp - 3*60)
        endTimeDT = datetime.datetime.fromtimestamp(timestamp - 1*60)
        with tempfile.TemporaryDirectory() as tmpDirName:
            oldImages = img_archive.getHpwrenImages(constants['googleServices'], settings, tmpDirName,
                                                    constants['camArchives'], cameraID, startTimeDT, endTimeDT, 1)
            attachments = oldImages or []
            attachments.append(imgPath)
            if annotatedFile:
                attachments.append(annotatedFile)
            email_helper.sendEmail(constants['googleServices']['mail'], settings.fuegoEmail, emails, subject, body, attachments)


def smsFireNotification(dbManager, cameraID):
    """Send an sms (phone text message) alert for a potential new fire

    Args:
        dbManager (DbManager):
        cameraID (str): camera name
    """
    message = 'Firecam fire notification in camera %s. Please check email for details' % cameraID
    dbResult = dbManager.getNotifications(filterActivePhone = True)
    phones = [x['phone'] for x in dbResult]
    if len(phones) > 0:
        for phone in phones:
            sms_helper.sendSms(settings, phone, message)


def alertFire(constants, cameraID, timestamp, imgPath, fireSegment):
    """Update Alerts DB and send alerts about given fire through all channels (pubsub, email, and sms)

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        imgPath: filepath of the original image
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
    """
    (croppedPath, annotatedPath) = genAnnotatedImages(constants, cameraID, timestamp, imgPath, fireSegment)

    # copy annotated image to publicly accessible settings.noticationsDir
    alertsDateDir = goog_helper.dateSubDir(settings.noticationsDir)
    croppedID = goog_helper.copyFile(croppedPath, alertsDateDir)
    annotatedID = goog_helper.copyFile(annotatedPath, alertsDateDir)
    # convert fileIDs into URLs usable by web UI
    croppedUrl = croppedID.replace('gs://', 'https://storage.googleapis.com/')
    annotatedUrl = annotatedID.replace('gs://', 'https://storage.googleapis.com/')

    dbManager = constants['dbManager']
    updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, fireSegment)
    pubsubFireNotification(cameraID, timestamp, croppedUrl, annotatedUrl, fireSegment)
    emailFireNotification(constants, cameraID, timestamp, imgPath, annotatedPath, fireSegment)
    smsFireNotification(dbManager, cameraID)

    # remove both temporary files
    os.remove(croppedPath)
    os.remove(annotatedPath)


def deleteImageFiles(imgPath, origImgPath):
    """Delete all image files given in segments

    Args:
        imgPath: filepath of the processed image
        origImgPath: filepath of the original image
    """
    os.remove(imgPath)
    if imgPath != origImgPath:
        os.remove(origImgPath)
    # ppath = pathlib.PurePath(imgPath)
    # leftoverFiles = os.listdir(str(ppath.parent))
    # if len(leftoverFiles) > 0:
    #     logging.warning('leftover files %s', str(leftoverFiles))


def getLastScoreCamera(dbManager):
    sqlStr = "SELECT CameraName from scores order by Timestamp desc limit 1;"
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) > 0:
        return dbResult[0]['CameraName']
    return None


def heartBeat(filename):
    """Inform monitor process that this detection process is alive

    Informs by updating the timestamp on given file

    Args:
        filename (str): file path of file used for heartbeating
    """
    pathlib.Path(filename).touch()


def genDiffImage(imgPath, earlierImgPath, minusMinutes):
    """Subtract the two given images and store result in new difference image file

    Args:
        imgPath (str): filepath of the current image (to subtract from)
        imgPath (str): filepath of the earlier image (value to subtract)
        minusMinutes (int): number of minutes separating subtracted images

    Returns:
        file path to the difference image
    """
    imgA = Image.open(imgPath)
    imgB = Image.open(earlierImgPath)
    imgDiff = img_archive.diffImages(imgA, imgB)
    parsedName = img_archive.parseFilename(imgPath)
    parsedName['diffMinutes'] = minusMinutes
    imgDiffName = img_archive.repackFileName(parsedName)
    ppath = pathlib.PurePath(imgPath)
    imgDiffPath = os.path.join(str(ppath.parent), imgDiffName)
    imgDiff.save(imgDiffPath, format='JPEG')
    return imgDiffPath


def updateTimeTracker(timeTracker, processingTime):
    """Update the time tracker data with given time to process current image

    If enough samples new samples have been reorded, resets the history and
    updates the average timePerSample

    Args:
        timeTracker (dict): tracks recent image processing times
        processingTime (float): number of seconds needed to process current image
    """
    timeTracker['totalTime'] += processingTime
    timeTracker['numSamples'] += 1
    # after N samples, update the rate to adapt to current conditions
    # N = 50 should be big enough to be stable yet small enough to adapt
    if timeTracker['numSamples'] > 50:
        timeTracker['timePerSample'] = timeTracker['totalTime'] / timeTracker['numSamples']
        timeTracker['totalTime'] = 0
        timeTracker['numSamples'] = 0
        logging.warning('New timePerSample %.2f', timeTracker['timePerSample'])


def initializeTimeTracker():
    """Initialize the time tracker

    Returns:
        timeTracker (dict):
    """
    return {
        'totalTime': 0.0,
        'numSamples': 0,
        'timePerSample': 3 # start off with estimate of 3 seconds per camera
    }


def getArchivedImages(constants, cameras, startTimeDT, timeRangeSeconds, minusMinutes):
    """Get random images from HPWREN archive matching given constraints and optionally subtract them

    Args:
        constants (dict): "global" contants
        cameras (list): list of cameras
        startTimeDT (datetime): starting time of time range
        timeRangeSeconds (int): number of seconds in time range
        minusMinutes (int): number of desired minutes between images to subract

    Returns:
        Tuple containing camera name, current timestamp, filepath of regular image, and filepath of difference image
    """
    if getArchivedImages.tmpDir == None:
        getArchivedImages.tmpDir = tempfile.TemporaryDirectory()
        logging.warning('TempDir %s', getArchivedImages.tmpDir.name)

    cameraID = cameras[int(len(cameras)*random.random())]['name']
    timeDT = startTimeDT + datetime.timedelta(seconds = random.random()*timeRangeSeconds)
    # ensure time between 8AM and 8PM because currently focusing on daytime only
    if timeDT.hour < 8:
        timeDT += datetime.timedelta(hours=8)
    elif timeDT.hour >= 20:
        timeDT -= datetime.timedelta(hours=4)
    if minusMinutes:
        prevTimeDT = timeDT + datetime.timedelta(seconds = -60 * minusMinutes)
    else:
        prevTimeDT = timeDT
    files = img_archive.getHpwrenImages(constants['googleServices'], settings, getArchivedImages.tmpDir.name,
                                        constants['camArchives'], cameraID, prevTimeDT, timeDT, minusMinutes or 1)
    # logging.warning('files %s', str(files))
    if not files:
        return (None, None, None, None)
    if minusMinutes:
        if len(files) > 1:
            if files[0] >= files[1]: # files[0] is supposed to be earlier than files[1]
                logging.warning('unexpected file order %s', str(files))
                for file in files:
                    os.remove(file)
                return (None, None, None, None)
            imgDiffPath = genDiffImage(files[1], files[0], minusMinutes)
            os.remove(files[0]) # no longer needed
            parsedName = img_archive.parseFilename(files[1])
            return (cameraID, parsedName['unixTime'], files[1], imgDiffPath)
        else:
            logging.warning('unexpected file count %s', str(files))
            for file in files:
                os.remove(file)
            return (None, None, None, None)
    elif len(files) > 0:
        parsedName = img_archive.parseFilename(files[0])
        return (cameraID, parsedName['unixTime'], files[0], files[0])
    return (None, None, None, None)
getArchivedImages.tmpDir = None


def main():
    optArgs = [
        ["b", "heartbeat", "filename used for heartbeating check"],
        ["c", "collectPositves", "collect positive segments for training data"],
        ["t", "time", "Time breakdown for processing images"],
        ["m", "minusMinutes", "(optional) subtract images from given number of minutes ago"],
        ["r", "restrictType", "Only process images from cameras of given type"],
        ["s", "startTime", "(optional) performs search with modifiedTime > startTime"],
        ["e", "endTime", "(optional) performs search with modifiedTime < endTime"],
        ["z", "randomSeed", "(optional) override random seed"],
    ]
    args = collect_args.collectArgs([], optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    minusMinutes = int(args.minusMinutes) if args.minusMinutes else 0
    # TODO: Fix googleServices auth to resurrect email alerts
    # googleServices = goog_helper.getGoogleServices(settings, args)
    googleServices = None
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    cameras = dbManager.get_sources(activeOnly=True, restrictType=args.restrictType)
    usableRegions = dbManager.get_usable_regions_dict()
    startTimeDT = dateutil.parser.parse(args.startTime) if args.startTime else None
    endTimeDT = dateutil.parser.parse(args.endTime) if args.endTime else None
    timeRangeSeconds = None
    useArchivedImages = False
    if startTimeDT or endTimeDT:
        assert startTimeDT and endTimeDT
        timeRangeSeconds = (endTimeDT-startTimeDT).total_seconds()
        assert timeRangeSeconds > 0
        assert args.collectPositves
        useArchivedImages = True
        # if seed not specified, use os.urandom and log value
        randomSeed = args.randomSeed if args.randomSeed else os.urandom(4).hex()
        logging.warning('Random seed %s', randomSeed)
        random.seed(randomSeed, version=2)
    camArchives = img_archive.getHpwrenCameraArchives(settings.hpwrenArchives)
    DetectionPolicyClass = policies.get_policies()[settings.detectionPolicy]
    detectionPolicy = DetectionPolicyClass(args, dbManager, minusMinutes, stateless=useArchivedImages)
    constants = { # dictionary of constants to reduce parameters in various functions
        'args': args,
        'googleServices': googleServices,
        'camArchives': camArchives,
        'dbManager': dbManager,
    }

    processingTimeTracker = initializeTimeTracker()
    while True:
        classifyImgPath = None
        timeStart = time.time()
        if useArchivedImages:
            (cameraID, timestamp, imgPath, classifyImgPath) = \
                getArchivedImages(constants, cameras, startTimeDT, timeRangeSeconds, minusMinutes)
        # elif minusMinutes: to be resurrected using archive functionality
        else: # regular (non diff mode), grab image and process
            (cameraID, timestamp, imgPath, md5) = getNextImage(dbManager, cameras)
            classifyImgPath = imgPath
        if not cameraID:
            continue # skip to next camera
        timeFetch = time.time()

        image_spec = [{}]
        image_spec[-1]['path'] = classifyImgPath
        image_spec[-1]['timestamp'] = timestamp
        image_spec[-1]['cameraID'] = cameraID
        if cameraID in usableRegions:
            usableEntry = usableRegions[cameraID]
            if 'startY' in usableEntry:
                image_spec[-1]['startY'] = usableEntry['startY']
            if 'endY' in usableEntry:
                image_spec[-1]['endY'] = usableEntry['endY']

        detectionResult = detectionPolicy.detect(image_spec)
        timeDetect = time.time()
        if detectionResult['fireSegment'] and not useArchivedImages:
            if not isDuplicateAlert(dbManager, cameraID, timestamp):
                alertFire(constants, cameraID, timestamp, imgPath, detectionResult['fireSegment'])
        deleteImageFiles(imgPath, imgPath)
        if (args.heartbeat):
            heartBeat(args.heartbeat)

        timePost = time.time()
        updateTimeTracker(processingTimeTracker, timePost - timeStart)
        if args.time:
            if not detectionResult['timeMid']:
                detectionResult['timeMid'] = timeDetect
            logging.warning('Timings: fetch=%.2f, detect0=%.2f, detect1=%.2f post=%.2f',
                timeFetch-timeStart, detectionResult['timeMid']-timeFetch, timeDetect-detectionResult['timeMid'], timePost-timeDetect)
        # free all memory for current iteration and trigger GC to prevent memory growth
        detectionResult = None
        gc.collect()

if __name__=="__main__":
    main()
