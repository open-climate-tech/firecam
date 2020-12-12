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
import json
import hashlib
import gc
from urllib.request import urlretrieve
import tensorflow as tf
from PIL import Image, ImageFile, ImageDraw, ImageFont
ImageFile.LOAD_TRUNCATED_IMAGES = True
import ffmpeg
from shapely.geometry import Polygon


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

    fontPath = os.path.join(str(pathlib.Path(os.path.realpath(__file__)).parent.parent), 'firecam/data/Roboto-Regular.ttf')
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

    (cropX0, cropX1) = rect_to_squares.getRangeFromCenter((x0 + x1)/2, 800, 0, img.size[0])
    (cropY0, cropY1) = rect_to_squares.getRangeFromCenter((y0 + y1)/2, 600, 0, img.size[1])
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


def getHeadingRange(cameraID, imgPath, minX, maxX):
    """Return heading (degrees 0 = North) and range of uncertainty of heading
       for the potential fire direction from given camera

    Args:
        cameraID (str): camera name
        imgPath: filepath of the original image
        minX (int): fire segment minX
        maxX (int): fire segment maxX

    Returns:
        Tuple (int, int): heading and uncertainty of heading
    """
    degreesInView = 110 # camera horizontal field of view is 110 for most Mobotix cameras
    degreesAlignmentError = 10 # cameras are not exactly aligned to cardnial headings

    # get horizontal pixel width
    img = Image.open(imgPath)
    imgSizeX = img.size[0]
    img.close()

    # calculate heading
    centerX = (minX + maxX) / 2
    angleFromCenter = centerX / imgSizeX * degreesInView - degreesInView/2
    centralHeading = img_archive.getHeading(cameraID)
    heading = angleFromCenter + centralHeading

    # calculate rangeAngle
    range = (maxX - minX) / imgSizeX * degreesInView + degreesAlignmentError
    return (heading, range)


def getCameraMapLocation(dbManager, cameraID):
    """Return the map surrounding the given camera by check SQL DB

    Args:
        dbManager (DbManager):
        cameraID (str): camera name

    Returns:
        GCS file for map
    """
    sqlTemplate = """SELECT mapFile,latitude,longitude FROM cameras WHERE locationID =
                     (SELECT locationID FROM sources WHERE name='%s')"""
    sqlStr = sqlTemplate % (cameraID)
    dbResult = dbManager.query(sqlStr)
    # print('dbr', len(dbResult), dbResult)
    if len(dbResult) == 0:
        logging.error('Did not find camera map %s', cameraID)
        return None
    return (dbResult[0]['mapfile'], dbResult[0]['latitude'], dbResult[0]['longitude'])


def drawPolyPixels(mapImg, coordsPixels, fillColor):
    """Draw translucent polygon on given map image with given pixel coordinates and fill color

    Args:
        mapImg (Image): existing image
        coordsPixels (list): list of vertices of polygon
        fillColor (list): RGBA values of fill color

    Returns:
        Image object
    """
    mapImgAlpha = mapImg.convert('RGBA')
    polyImg = Image.new('RGBA', mapImgAlpha.size)
    polyDraw = ImageDraw.Draw(polyImg)
    polyDraw.polygon(coordsPixels, fill=fillColor)
    mapImgAlpha.paste(polyImg, mask=polyImg)
    del polyDraw
    polyImg.close()
    return mapImgAlpha.convert('RGB')


def convertLatLongToPixels(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, latLong):
    """Convert given lat/long coordinates into pixel X/Y coordinates given map and its borders

    Args:
        mapImg (Image): map image
        left/right/top/bottom: borders of map
        latlong (list): (lat, long)

    Returns:
        (x, y) pixel values of cooressponding pixel in image
    """
    assert latLong[0] > bottomLatitude
    assert latLong[0] < topLatitude
    assert latLong[1] > leftLongitude
    assert latLong[1] < rightLongitude

    diffLat = topLatitude - bottomLatitude
    diffLong = rightLongitude - leftLongitude

    pixelX = (latLong[1] - leftLongitude)/diffLong*mapImg.size[0]
    pixelX = max(min(pixelX, mapImg.size[0] - 1), 0)
    pixelY = mapImg.size[1] - (latLong[0] - bottomLatitude)/diffLat*mapImg.size[1]
    pixelY = max(min(pixelY, mapImg.size[1] - 1), 0)
    return (pixelX, pixelY)


def drawPolyLatLong(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, coords, fillColor):
    """Draw translucent polygon on given map image with given lat/long coordinates and fill color

    Args:
        mapImg (Image): existing image
        left/right/top/bottom: borders of map
        coords (list): list of vertices of polygon in lat/long format
        fillColor (list): RGBA values of fill color

    Returns:
        Image object
    """
    coordsPixels = []
    # logging.warning('coords latLong %s', str(coords))
    for point in coords:
        pixels = convertLatLongToPixels(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, point)
        coordsPixels.append(pixels)
    # logging.warning('coords pixels %s', str(coordsPixels))
    return drawPolyPixels(mapImg, coordsPixels, fillColor)


def cropCentered(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, polygonCoords):
    """Crop given image to 1/4 size centered at the centroid of given polygon

    Args:
        mapImg (Image): existing image
        left/right/top/bottom: borders of map
        polygonCoords (list): list of vertices of polygon in lat/long format

    Returns:
        Cropped Image object
    """
    poly = Polygon(polygonCoords)
    centerLatLong = list(zip(*poly.centroid.xy))[0]
    centerXY = convertLatLongToPixels(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, centerLatLong)
    centerX = min(max(centerXY[0], mapImg.size[0]/4), mapImg.size[0]*3/4)
    centerY = min(max(centerXY[1], mapImg.size[1]/4), mapImg.size[1]*3/4)
    coords = (centerX - mapImg.size[0]/4, centerY - mapImg.size[1]/4, centerX + mapImg.size[0]/4, centerY + mapImg.size[1]/4)
    return mapImg.crop(coords)


def genAnnotatedMap(mapImgGCS, camLatitude, camLongitude, imgPath, polygon, sourcePolygons):
    """Generate annotated map highlighting potential fire area

    Args:
        mapImgGCS (str): GCS path to map around camera
        camLatitude (float): latitude of camera
        camLongitude (float): longitude of camera
        imgPath (str): filepath of the image
        polygon (list): list of vertices of polygon of potential fire location
        sourcePolygons (list): list of polygons from individual cameras contributing to the polygon

    Returns:
        filepath of annotated map
    """
    # download map from GCS to local
    filePathParts = os.path.splitext(imgPath)
    parsedPath = goog_helper.parseGCSPath(mapImgGCS)
    mapOrig = filePathParts[0] + '_mapOrig.jpg'
    goog_helper.downloadBucketFile(parsedPath['bucket'], parsedPath['name'], mapOrig)

    mapWidthLong = 1.757 # all maps have span this many longitidues
    mapHeightLat = 1.466 # all maps have span this many latitudes
    leftLongitude = camLongitude - mapWidthLong/2
    rightLongitude = camLongitude + mapWidthLong/2
    bottomLatitude = camLatitude - mapHeightLat/2
    topLatitude = camLatitude + mapHeightLat/2

    # markup map to show fire area
    mapImg = Image.open(mapOrig)
    # first draw all source polygons (in light red) that contributed to this fire area
    for sourcePolygon in sourcePolygons:
        lightRed = (255,0,0, 50)
        mapImg = drawPolyLatLong(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, sourcePolygon, lightRed)
    # if there were multiple source polygons, highlight the fire area in light blue
    if len(sourcePolygons) > 1:
        lightBlue = (0,0,255, 75)
        mapImg = drawPolyLatLong(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, polygon, lightBlue)
    # crop to smaller map centered around fire area
    mapImgCropped = cropCentered(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, polygon)
    mapCroppedPath = filePathParts[0] + '_map.jpg'
    mapImgCropped.save(mapCroppedPath)
    mapImgCropped.close()
    mapImg.close()
    os.remove(mapOrig)
    return mapCroppedPath


def getTriangleVertices(latitude, longitude, heading, rangeAngle):
    """Return list of vertices of the isocelees triangle given lat/long as one vertex
       and heading/rangeAngle specifying the angle to the other vertices.

    Args:
        latitude (float): latitude of central vertex
        longitude (float): longitude of central vertex
        heading (int): direction of the central angle
        rangeAngle (int): degrees (size) of the central angle

    Returns:
        List of all vertices in [lat,long] format
    """
    distanceDegrees = 0.5 # approx 35 miles

    vertices = [[latitude, longitude]]
    angle = 90 - heading
    minAngle = (angle - rangeAngle/2) % 360
    maxAngle = (angle + rangeAngle/2) % 360

    p0Lat = latitude + math.sin(minAngle*math.pi/180)*distanceDegrees
    p0Long = longitude + math.cos(minAngle*math.pi/180)*distanceDegrees
    vertices.append([p0Lat, p0Long])

    p1Lat = latitude + math.sin(maxAngle*math.pi/180)*distanceDegrees
    p1Long = longitude + math.cos(maxAngle*math.pi/180)*distanceDegrees
    vertices.append([p1Lat, p1Long])
    return vertices


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


def getRecentAlerts(dbManager, timestamp):
    """Return all recent (last 10 minutes) alerts

    Args:
        dbManager (DbManager):
        timestamp (int): time.time() value when image was taken

    Returns:
        List of alerts
    """
    sqlTemplate = """SELECT * FROM alerts where timestamp > %s order by timestamp desc"""
    sqlStr = sqlTemplate % (timestamp - 10*60)

    dbResult = dbManager.query(sqlStr)
    return dbResult


def getPolygonIntersection(coords1, coords2):
    """Find the area intersection of the two given polygons

    Args:
        coords1 (list): vertices of polygon 1
        coords2 (list): vertices of polygon 2

    Returns:
        List of vertices of intersection area or None
    """
    poly1 = Polygon(coords1)
    poly2 = Polygon(coords2)
    if not poly1.intersects(poly2):
        return None
    intPoly = poly1.intersection(poly2)
    if intPoly.area == 0: # point intersections treated as not intersecting
        return None
    # logging.warning('intpoly: %s', str(intPoly))
    intersection = []
    for i in range(len(intPoly.exterior.coords.xy[0])):
        intersection.append([intPoly.exterior.coords.xy[0][i], intPoly.exterior.coords.xy[1][i]])
    return intersection


def intersectRecentAlerts(dbManager, timestamp, triangle):
    """Check for area intersection of given triangle with polygons of recent alerts

    Args:
        dbManager (DbManager):
        timestamp (int): time.time() value when image was taken
        triangle (list): vertices of triangle

    Returns:
        Intersection area and all source polygons of recent alert
    """
    recentAlerts = getRecentAlerts(dbManager, timestamp)
    for alert in recentAlerts:
        alertCoords = json.loads(alert['polygon'])
        intersection = getPolygonIntersection(triangle, alertCoords)
        if intersection:
            return (intersection, json.loads(alert['sourcepolygons']))


def updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons):
    """Add new entry to Alerts table

    Args:
        dbManager (DbManager):
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        croppedUrl: Public URL for cropped video
        annotatedUrl: Public URL for annotated iamge
        mapUrl: Public URL for annotated map
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
        polygon (list): list of vertices of polygon of potential fire location
        sourcePolygons (list): list of polygons from individual cameras contributing to the polygon
    """
    dbRow = {
        'CameraName': cameraID,
        'Timestamp': timestamp,
        'AdjScore': fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score'],
        'ImageID': annotatedUrl,
        'CroppedID': croppedUrl,
        'MapID': mapUrl,
        'polygon': str(polygon),
        'sourcePolygons': str(sourcePolygons),
    }
    dbManager.add_data('alerts', dbRow)


def pubsubFireNotification(cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon):
    """Send a pubsub notification for a potential new fire

    Sends pubsub message with information about the camera and fire score includeing
    image attachments

    Args:
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        croppedUrl: Public URL for cropped video
        annotatedUrl: Public URL for annotated iamge
        mapUrl: Public URL for annotated map
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
        polygon (list): list of vertices of polygon of potential fire location
    """
    message = {
        'timestamp': timestamp,
        'cameraID': cameraID,
        "mlScore": str(fireSegment['score']),
        "histMax": str(fireSegment['HistMax'] if 'HistMax' in fireSegment else 0),
        "adjScore": str(fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score']),
        'croppedUrl': croppedUrl,
        'annotatedUrl': annotatedUrl,
        'mapUrl': mapUrl,
        'polygon': str(polygon)
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
    dbManager = constants['dbManager']

    (mapImgGCS, camLatitude, camLongitude) = getCameraMapLocation(dbManager, cameraID)
    (heading, rangeAngle) = getHeadingRange(cameraID, imgPath, fireSegment['MinX'], fireSegment['MaxX'])
    (croppedPath, annotatedPath) = genAnnotatedImages(constants, cameraID, timestamp, imgPath, fireSegment)
    triangle = getTriangleVertices(camLatitude, camLongitude, heading, rangeAngle)
    intersectionInfo = intersectRecentAlerts(dbManager, timestamp, triangle)
    if intersectionInfo:
        polygon = intersectionInfo[0]
        sourcePolygons = intersectionInfo[1] + [triangle]
    else:
        polygon = triangle
        sourcePolygons = [triangle]
    mapPath = genAnnotatedMap(mapImgGCS, camLatitude, camLongitude, imgPath, polygon, sourcePolygons)

    # copy annotated image to publicly accessible settings.noticationsDir
    alertsDateDir = goog_helper.dateSubDir(settings.noticationsDir)
    croppedID = goog_helper.copyFile(croppedPath, alertsDateDir)
    annotatedID = goog_helper.copyFile(annotatedPath, alertsDateDir)
    mapID = goog_helper.copyFile(mapPath, alertsDateDir)
    # convert fileIDs into URLs usable by web UI
    croppedUrl = croppedID.replace('gs://', 'https://storage.googleapis.com/')
    annotatedUrl = annotatedID.replace('gs://', 'https://storage.googleapis.com/')
    mapUrl = mapID.replace('gs://', 'https://storage.googleapis.com/')

    updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons)
    pubsubFireNotification(cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon)
    emailFireNotification(constants, cameraID, timestamp, imgPath, annotatedPath, fireSegment)
    smsFireNotification(dbManager, cameraID)

    # remove temporary files
    os.remove(croppedPath)
    os.remove(annotatedPath)
    os.remove(mapPath)


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
    diffImg = img_archive.diffSmoothImages(imgA, imgB)
    extremas = diffImg.getextrema()
    if extremas[0][0] == 128 or extremas[0][1] == 128 or extremas[1][0] == 128 or extremas[1][1] == 128 or extremas[2][0] == 128 or extremas[2][1] == 128:
        logging.warning('Skipping no diffs %s', str(extremas))
        return None
    parsedName = img_archive.parseFilename(imgPath)
    parsedName['diffMinutes'] = minusMinutes
    diffImgName = img_archive.repackFileName(parsedName)
    ppath = pathlib.PurePath(imgPath)
    diffImgPath = os.path.join(str(ppath.parent), diffImgName)
    diffImg.save(diffImgPath, format='JPEG')
    return diffImgPath


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
            diffFail = False
            imgDiffPath = None
            if files[0] < files[1]: # files[0] is supposed to be earlier than files[1]
                imgDiffPath = genDiffImage(files[1], files[0], minusMinutes)
            else:
                diffFail = True
            if diffFail or not imgDiffPath:
                logging.warning('diff fail %s', str(files))
                prevFile = None
                for file in files:
                    if file != prevFile:
                        os.remove(file)
                    prevFile = file
                return (None, None, None, None)
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
        ["m", "minusMinutes", "(optional) subtract images from given number of minutes ago", int],
        ["r", "restrictType", "Only process images from cameras of given type"],
        ["s", "startTime", "(optional) performs search with modifiedTime > startTime"],
        ["e", "endTime", "(optional) performs search with modifiedTime < endTime"],
        ["z", "randomSeed", "(optional) override random seed"],
        ["l", "limitImages", "(optional) stop after processing given number of images", int],
    ]
    args = collect_args.collectArgs([], optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    minusMinutes = args.minusMinutes if args.minusMinutes else 0
    limitImages = args.limitImages if args.limitImages else 1e9
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

    numImages = 0
    numDetections = 0
    numAlerts = 0
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

        detectionResult = detectionPolicy.detect(image_spec, checkShifts=True)
        timeDetect = time.time()
        numImages += 1
        if detectionResult['fireSegment']:
            numDetections += 1
        if detectionResult['fireSegment'] and not useArchivedImages:
            if not isDuplicateAlert(dbManager, cameraID, timestamp):
                alertFire(constants, cameraID, timestamp, imgPath, detectionResult['fireSegment'])
                numAlerts += 1
        deleteImageFiles(classifyImgPath, imgPath)
        if (args.heartbeat):
            heartBeat(args.heartbeat)

        timePost = time.time()
        updateTimeTracker(processingTimeTracker, timePost - timeStart)
        if args.time:
            if not detectionResult['timeMid']:
                detectionResult['timeMid'] = timeDetect
            logging.warning('Timings: fetch=%.2f, detect0=%.2f, detect1=%.2f post=%.2f',
                timeFetch-timeStart, detectionResult['timeMid']-timeFetch, timeDetect-detectionResult['timeMid'], timePost-timeDetect)
        if (numImages % 10) == 0:
            logging.warning('Stats: alerts=%d, detects=%d, images=%d', numAlerts, numDetections, numImages)
            if numImages >= limitImages:
                logging.warning('Reached limit on images')
                return
        # free all memory for current iteration and trigger GC to prevent memory growth
        detectionResult = None
        gc.collect()

if __name__=="__main__":
    main()
