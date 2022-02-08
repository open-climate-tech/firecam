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
from firecam.lib import weather
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


def getNextImage(dbManager, cameras, stateless, counterName):
    """Gets the next image to check for smoke

    Uses a shared counter being updated by all cooperating detection processes
    to index into the list of cameras to download the image to a local
    temporary directory

    Args:
        dbManager (DbManager):
        cameras (list): list of cameras
        stateless (bool): [optional] if specified use stateless mechanism for camera selection

    Returns:
        Tuple containing camera name, current heading, current timestamp, and filepath of the image
    """
    if getNextImage.tmpDir == None:
        getNextImage.tmpDir = tempfile.TemporaryDirectory()
        logging.warning('TempDir %s', getNextImage.tmpDir.name)

    if getNextImage.queueCamera and (len(getNextImage.queue) > 0):
        camera = getNextImage.queueCamera
    elif stateless:
        camera = cameras[int(len(cameras)*random.random())]
    else:
        counterValue = dbManager.incrementCounter(counterName)
        index = counterValue % len(cameras)
        camera = cameras[index]

    try:
        if len(getNextImage.queue) > 0:
            fetchResult = getNextImage.queue[0]
            getNextImage.queue = getNextImage.queue[1:]
            if len(getNextImage.queue) == 0:
                getNextImage.queueCamera = None
        else:
            fetchResult = img_archive.fetchImageAndMeta(dbManager, camera['name'], camera['url'], getNextImage.tmpDir.name)
        if isinstance(fetchResult, list):
            if len(fetchResult) > 1:
                getNextImage.queue = fetchResult[1:]
                getNextImage.queueCamera = camera
            fetchResult = fetchResult[0]
        (imgPath, heading, timestamp, fov) = fetchResult
        if imgPath == None or heading == None or timestamp == None:
            logging.error('Image or metadata unavailable for %s', camera['name'])
            return (None, None, None, None, None)

        md5 = hashlib.md5(open(imgPath, 'rb').read()).hexdigest()
        if ('md5' in camera) and (camera['md5'] == md5):
            logging.warning('Camera %s image unchanged', camera['name'])
            # skip to next camera
            return (None, None, None, None, None)
        camera['md5'] = md5
    except Exception as e:
        logging.error('Error fetching image from %s %s', camera['name'], str(e))
        return (None, None, None, None, None)

    return (camera['name'], heading, timestamp, fov, imgPath)
getNextImage.tmpDir = None
getNextImage.queue = []
getNextImage.queueCamera = None


# XXXXX Use a fixed stable directory for testing
# from collections import namedtuple
# Tdir = namedtuple('Tdir', ['name'])
# getNextImage.tmpDir = Tdir('c:/tmp/dftest')


def isProto(cameraID):
    return img_archive.isPTZ(cameraID)


def drawRect(imgDraw, x0, y0, x1, y1, width, color):
    for i in range(width):
        imgDraw.rectangle((x0 + i, y0 + i, x1 - i, y1 -i), outline=color)


def drawFireBox(img, destPath, fireBoxCoords, timestamp=None, fireSegment=None, color='red'):
    """Draw bounding box with fire detection and optionally write scores

    Also watermarks the image and stores the resulting annotated image as new file

    Args:
        img (Image): Image object to draw on
        destPath (str): filepath where to write the output image
        fireBoxCoords (list): coordinates of fire box (x0, y0, x1, y1)
        fireSegment (dict): [optional] if present, write scores on the image
    """
    imgDraw = ImageDraw.Draw(img)

    (x0, y0, x1, y1) = fireBoxCoords
    lineWidth=3
    drawRect(imgDraw, x0, y0, x1, y1, lineWidth, color)

    fontPath = os.path.join(str(pathlib.Path(os.path.realpath(__file__)).parent.parent), 'firecam/data/Roboto-Regular.ttf')
    if fireSegment:
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

    img.save(destPath, format="JPEG", quality=95)
    del imgDraw


def genMovie(notificationsDateDir, constants, cameraID, cameraHeading, timestamp, imgPath, cropCoords, fireBoxCoords):
    """Generate cropped movie by fetching old images from archive

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        imgPath (str): filepath of the image
        fireSegment (dict): dict describing segment with fire
        cropCoords (list): coordinates for cropping full image
        fileBoxCoords (list): coordinates for highlighting fire box withing cropped region

    Returns:
        Filepath of cropped movie
    """
    filePathParts = os.path.splitext(imgPath)
    # get images spanning a few minutes so reviewers can evaluate based on progression
    startTimeDT = datetime.datetime.fromtimestamp(timestamp - 4*60)
    endTimeDT = datetime.datetime.fromtimestamp(timestamp + 4*60)  # check "future" in case new image arrived during processing

    with tempfile.TemporaryDirectory() as tmpDirName:
        oldImages = img_archive.getArchiveImages(constants['googleServices'], settings, constants['dbManager'], tmpDirName,
                                                 constants['camArchives'], cameraID, cameraHeading, startTimeDT, endTimeDT, 1)
        imgSequence = oldImages or []
        imgSequence = imgSequence[-5:] # max 5 total images
        imgIDs = []
        mspecPath = os.path.join(tmpDirName, 'mspec.txt')
        mspecFile = open(mspecPath, 'w')
        for (i, imgFile) in enumerate(imgSequence):
            imgParsed = img_archive.parseFilename(imgFile)
            if imgParsed['unixTime'] != timestamp:
                algined = img_archive.alignImage(imgFile, imgPath)
                if not algined:
                    continue # skip this image
            imgIDs.append(goog_helper.copyFile(imgFile, notificationsDateDir))
            cropName = 'img' + ("%03d" % i) + filePathParts[1]
            croppedPath = os.path.join(tmpDirName, cropName)
            imgSeq = Image.open(imgFile)
            croppedImg = imgSeq.crop(cropCoords)
            if imgParsed['unixTime'] < timestamp:
                color = 'yellow'
            elif imgParsed['unixTime'] == timestamp:
                color = 'red'
            else:
                color = 'orange'
            drawFireBox(croppedImg, croppedPath, fireBoxCoords, timestamp=imgParsed['unixTime'], color=color)
            imgSeq.close()
            croppedImg.close()
            mspecFile.write("file '" + croppedPath + "'\n")
            mspecFile.write('duration 1\n')
            if (i == len(imgSequence) - 1): # final image has to be repeated for ffmpeg
                mspecFile.write("file '" + croppedPath + "'\n")
        mspecFile.flush()
        os.fsync(mspecFile.fileno())
        mspecFile.close()
        # now make movie from this sequence of cropped images
        moviePath = filePathParts[0] + '_AnnCrop_' + 'x'.join(list(map(lambda x: str(x), cropCoords))) + '.mp4'
        (
            ffmpeg.input(mspecPath, format='concat', safe=0)
                .filter('fps', fps=25, round='up')
                .output(moviePath, pix_fmt='yuv420p').run()
        )
        movieID = goog_helper.copyFile(moviePath, notificationsDateDir)
        os.remove(moviePath)
        return (movieID, imgIDs)


def genAnnotatedImages(notificationsDateDir, constants, cameraID, cameraHeading, timestamp, imgPath, fireSegment):
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
    fireBoxCoords = (x0 - cropX0, y0 - cropY0, x1 - cropX0, y1 - cropY0)
    (movieID, imgIDs) = genMovie(notificationsDateDir, constants, cameraID, cameraHeading, timestamp, imgPath, cropCoords, fireBoxCoords)

    annotatedPath = filePathParts[0] + '_Ann' + filePathParts[1]
    drawFireBox(img, annotatedPath, (x0, y0, x1, y1))
    img.close()
    annotatedID = goog_helper.copyFile(annotatedPath, notificationsDateDir)
    os.remove(annotatedPath)

    return (movieID, imgIDs, annotatedID)


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
    latitude = min(max(latLong[0], bottomLatitude), topLatitude)
    longitude = min(max(latLong[1], leftLongitude), rightLongitude)

    diffLat = topLatitude - bottomLatitude
    diffLong = rightLongitude - leftLongitude

    pixelX = (longitude - leftLongitude)/diffLong*mapImg.size[0]
    pixelX = max(min(pixelX, mapImg.size[0] - 1), 0)
    pixelY = mapImg.size[1] - (latitude - bottomLatitude)/diffLat*mapImg.size[1]
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


def getCentroid(polygonCoords):
    poly = Polygon(polygonCoords)
    return list(zip(*poly.centroid.xy))[0]


def cropCentered(mapImg, leftLongitude, rightLongitude, topLatitude, bottomLatitude, polygonCoords):
    """Crop given image to 1/4 size centered at the centroid of given polygon

    Args:
        mapImg (Image): existing image
        left/right/top/bottom: borders of map
        polygonCoords (list): list of vertices of polygon in lat/long format

    Returns:
        Cropped Image object
    """
    centerLatLong = getCentroid(polygonCoords)
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
    mapImgCropped.save(mapCroppedPath, quality=95)
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
    distanceDegrees = 0.6 # approx 40 miles

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


def recordProbables(dbManager, cameraID, heading, timestamp, imgPath, fireSegment, modelId, stateless):
    """Record that a probable smoke/fire has been observed

    Record the probable detection with useful metrics in 'probables' table in SQL DB.
    Also, upload image file to google cloud

    Args:
        dbManager (DbManager):
        cameraID (str): camera ID
        heading (int): direction camera is facing
        timestamp (int):
        imgPath: filepath of the image
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke

    Returns:
        File IDs for the uploaded image file
    """
    logging.warning('Fire detected by camera %s, image %s, segment %s', cameraID, imgPath, str(fireSegment))
    # copy/upload file to detection dir
    probablesDateDir = goog_helper.dateSubDir(settings.probablesDir)
    fileID = goog_helper.copyFile(imgPath, probablesDateDir)
    logging.warning('Uploaded to probables folder %s', fileID)

    if not stateless:
        dbRow = {
            'CameraName': cameraID,
            'Heading': heading,
            'Timestamp': timestamp,
            'MinX': fireSegment['MinX'],
            'MinY': fireSegment['MinY'],
            'MaxX': fireSegment['MaxX'],
            'MaxY': fireSegment['MaxY'],
            'Score': fireSegment['score'],
            'ImageID': fileID,
            'ModelId': modelId
        }
        dbManager.add_data('probables', dbRow)
    return fileID


def isDuplicateProbables(dbManager, cameraID, heading, timestamp):
    """Check if this event has already been recently (last hour) discovered for given camera
       This prevents spam from long lasting fires

    Args:
        dbManager (DbManager):
        cameraID (str): camera ID
        heading (int): direction camera is facing
        timestamp (int): time.time() value when image was taken

    Returns:
        True if this is a duplicate probables, False otherwise
    """
    sqlTemplate = """SELECT * FROM probables
    where CameraName='%s' and Heading=%s and timestamp > %s and timestamp < %s"""
    sqlStr = sqlTemplate % (cameraID, heading, timestamp - 60*60, timestamp)

    dbResult = dbManager.query(sqlStr)
    if len(dbResult) > 0:
        logging.warning('Supressing due to recent probables')
        return True
    return False


def getRecentDetections(dbManager, timestamp):
    """Return all recent (last 15 minutes) detections

    Args:
        dbManager (DbManager):
        timestamp (int): time.time() value when image was taken

    Returns:
        List of alerts
    """
    sqlTemplate = """SELECT * FROM detections where timestamp > %s order by timestamp desc"""
    sqlStr = sqlTemplate % (timestamp - 15*60)

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


def intersectRecentDetections(dbManager, timestamp, triangle):
    """Check for area intersection of given triangle with polygons of recent detections

    Args:
        dbManager (DbManager):
        timestamp (int): time.time() value when image was taken
        triangle (list): vertices of triangle

    Returns:
        Intersection area and all source polygons of recent detections
    """
    recentDetections = getRecentDetections(dbManager, timestamp)
    for alert in recentDetections:
        alertCoords = json.loads(alert['polygon'])
        intersection = getPolygonIntersection(triangle, alertCoords)
        if intersection:
            return (intersection, json.loads(alert['sourcepolygons']))


def intersectLand(triangle):
    landVertices = [
        [42.252, -114.000], [42.252, -124.411], #oregon
        [41.996, -124.211], [41.814, -124.231], [41.784, -124.255], [41.746, -124.203], [41.737, -124.159],
        [41.657, -124.134], [41.593, -124.100], [41.562, -124.096], [41.546, -124.075], [41.531, -124.080],
        [41.437, -124.063], [41.286, -124.090], [41.228, -124.086], [41.226, -124.108], [41.157, -124.101],
        [41.156, -124.135], [41.138, -124.157], [41.100, -124.162], [41.070, -124.158], [41.031, -124.116],
        [40.931, -124.131], [40.868, -124.159], [40.844, -124.077], [40.802, -124.135], [40.796, -124.181],
        [40.754, -124.194], [40.723, -124.222], [40.688, -124.201], [40.691, -124.280], [40.443, -124.411],
        [40.242, -124.325], [39.750, -123.825], [39.494, -123.765], [39.362, -123.822], [39.285, -123.798],
        [38.936, -123.723], [38.236, -122.972], [38.026, -123.001], [37.908, -122.649], [37.767, -122.512],
        [37.497, -122.490], [37.327, -122.397], [37.213, -122.419], [36.946, -122.078], [36.946, -121.892],
        [36.788, -121.776], [36.660, -121.826], [36.576, -121.975], [36.546, -121.934], [36.515, -121.947],
        [36.408, -121.918], [36.306, -121.901], [36.237, -121.818], [36.156, -121.671], [36.020, -121.570],
        [36.003, -121.504], [35.881, -121.456], [35.770, -121.325], [35.714, -121.312], [35.671, -121.283],
        [35.642, -121.200], [35.631, -121.159], [35.461, -121.001], [35.445, -120.901], [35.366, -120.866],
        [35.255, -120.897], [35.163, -120.762], [35.164, -120.691], [35.114, -120.635], [35.010, -120.639],
        [34.903, -120.670], [34.884, -120.640], [34.859, -120.608], [34.758, -120.635], [34.705, -120.601],
        [34.568, -120.636], [34.540, -120.549], [34.457, -120.470], [34.464, -120.093], [34.434, -119.953],
        [34.407, -119.860], [34.395, -119.715], [34.420, -119.601], [34.353, -119.434], [34.276, -119.304],
        [34.153, -119.219], [34.084, -119.052], [34.009, -118.808], [34.037, -118.533], [33.824, -118.387],
        [33.773, -118.425], [33.707, -118.289], [33.768, -118.167], [33.617, -117.937], [33.546, -117.801],
        [33.460, -117.714], [33.428, -117.628], [33.378, -117.586], [33.204, -117.390], [33.026, -117.287],
        [32.916, -117.256], [32.849, -117.259], [32.843, -117.286], [32.771, -117.255], [32.664, -117.242],
        [32.592, -117.131], [32.536, -117.124],
        [32.100, -116.950], [32.100, -114.000], # mexico
    ]
    return getPolygonIntersection(triangle, landVertices)


def checkWeatherInfo(weatherModel, dbManager, cameraID, timestamp, fireSegment, polygon, sourcePolygons, cameraLatLong):
    centroidLatLong = getCentroid(polygon)
    (weatherCentroid, weatherCamera) = weather.getWeatherData(dbManager, cameraID, timestamp, centroidLatLong, cameraLatLong)
    if (not weatherCentroid) or (not weatherCamera):
        return 1
    numPolys = len(sourcePolygons)
    imgScore = fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score']
    featureData = weather.normalizeWeather(imgScore, numPolys, weatherCentroid, weatherCamera)
    prediction = weatherModel.predict([featureData])[0][0]
    return prediction


def updateDetectionsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons, imgIDs):
    """Add new entry to detections table

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
        'IsProto': int(isProto(cameraID)),
        'WeatherScore': fireSegment['weatherScore'],
        'ImgSequence': ','.join(imgIDs),
    }
    dbManager.add_data('detections', dbRow)


def updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons):
    """Add new entry to alerts table

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
        'IsProto': int(isProto(cameraID)),
        'WeatherScore': fireSegment['weatherScore'],
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
        "adjScore": str(fireSegment['AdjScore'] if 'AdjScore' in fireSegment else fireSegment['score']),
        'annotatedUrl': annotatedUrl,
        'croppedUrl': croppedUrl,
        'mapUrl': mapUrl,
        'polygon': str(polygon),
        'isProto': isProto(cameraID),
        'weatherScore': str(fireSegment['weatherScore']),
    }
    goog_helper.publish(message)


def emailFireNotification(constants, cameraID, timestamp, imgPath, annotatedUrl, fireSegment):
    """Send an email alert for a potential new fire

    Send email with information about the camera and fire score includeing
    image attachments

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        timestamp (int): time.time() value when image was taken
        imgPath: filepath of the original image
        annotatedUrl: Public URL for annotated iamge
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


def publishAlert(cameraID, weatherScore):
    return (weatherScore > settings.weatherThreshold) and not isProto(cameraID)


def fireDetected(constants, cameraID, cameraHeading, timestamp, fov, imgPath, fireSegment):
    """Update Detections DB and send alerts about given fire through all channels (pubsub, email, and sms)

    Args:
        constants (dict): "global" contants
        cameraID (str): camera name
        cameraHeading (int): direction camera is facing
        timestamp (int): time.time() value when image was taken
        imgPath: filepath of the original image
        fireSegment (dictionary): dictionary with information for the segment with fire/smoke
    """
    dbManager = constants['dbManager']
    weatherModel = constants['weatherModel']

    # copy annotated image to publicly accessible settings.noticationsDir
    notificationsDateDir = goog_helper.dateSubDir(settings.noticationsDir)
    (mapImgGCS, camLatitude, camLongitude) = dbManager.getCameraMapLocation(cameraID)

    # get horizontal pixel width
    img = Image.open(imgPath)
    imgSizeX = img.size[0]
    img.close()

    # find angular heading, and check if it should be ignored due to frequent false positives
    (fireHeading, rangeAngle) = img_archive.getHeadingRange(cameraHeading, fov, fireSegment['MinX'], fireSegment['MaxX'], imgSizeX)
    ignoredHeading = img_archive.findIgnoredViewHeading(constants['ignoredViews'], cameraID, fireHeading, rangeAngle)
    if ignoredHeading != None:
        logging.warning('Ignored View %s, %s, %s, %s', cameraID, fireHeading, rangeAngle, ignoredHeading)
        dbManager.incrementIgnoreCounter(cameraID, ignoredHeading)
        return

    (croppedID, imgIDs, annotatedID) = genAnnotatedImages(notificationsDateDir, constants, cameraID, cameraHeading, timestamp, imgPath, fireSegment)
    if len(imgIDs) < 2:
        return # ignore events without multiple images
    triangle = getTriangleVertices(camLatitude, camLongitude, fireHeading, rangeAngle)
    currentViewShed = intersectLand(triangle)
    intersectionInfo = intersectRecentDetections(dbManager, timestamp, currentViewShed)
    if intersectionInfo:
        polygon = intersectionInfo[0]
        sourcePolygons = intersectionInfo[1] + [currentViewShed]
    else:
        polygon = currentViewShed
        sourcePolygons = [currentViewShed]
    weatherScore = checkWeatherInfo(weatherModel, dbManager, cameraID, timestamp, fireSegment, polygon, sourcePolygons, (camLatitude, camLongitude))
    fireSegment['weatherScore'] = round(weatherScore, 4)

    mapPath = genAnnotatedMap(mapImgGCS, camLatitude, camLongitude, imgPath, polygon, sourcePolygons)

    mapID = goog_helper.copyFile(mapPath, notificationsDateDir)
    # convert fileIDs into URLs usable by web UI
    croppedUrl = croppedID.replace('gs://', 'https://storage.googleapis.com/')
    annotatedUrl = annotatedID.replace('gs://', 'https://storage.googleapis.com/')
    mapUrl = mapID.replace('gs://', 'https://storage.googleapis.com/')

    updateDetectionsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons, imgIDs)
    if publishAlert(cameraID, weatherScore):
        updateAlertsDB(dbManager, cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon, sourcePolygons)
        pubsubFireNotification(cameraID, timestamp, croppedUrl, annotatedUrl, mapUrl, fireSegment, polygon)
        emailFireNotification(constants, cameraID, timestamp, imgPath, annotatedUrl, fireSegment)
        smsFireNotification(dbManager, cameraID)

    # remove temporary files
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


def getArchivedImages(constants, cameras, startTimeDT, timeRangeSeconds):
    """Get random images from HPWREN archive matching given constraints and optionally subtract them

    Args:
        constants (dict): "global" contants
        cameras (list): list of cameras
        startTimeDT (datetime): starting time of time range
        timeRangeSeconds (int): number of seconds in time range

    Returns:
        Tuple containing camera name, current timestamp, filepath of regular image, and filepath of difference image
    """
    if getArchivedImages.tmpDir == None:
        getArchivedImages.tmpDir = tempfile.TemporaryDirectory()
        logging.warning('TempDir %s', getArchivedImages.tmpDir.name)

    # setup caching of the archive files locally
    if (getArchivedImages.cache == None) and settings.downloadDir:
        writable = os.access(settings.downloadDir, os.W_OK|os.X_OK)
        writeDirPath = settings.downloadDir
        if not writable:
            getArchivedImages.writeDir = tempfile.TemporaryDirectory()
            writeDirPath = getArchivedImages.writeDir.name
        getArchivedImages.cache = img_archive.cacheDir(settings.downloadDir, writeDirPath)

    if getArchivedImages.cache:
        downloadDirOrCache = getArchivedImages.cache
    else:
        downloadDirOrCache = getArchivedImages.tmpDir.name

    cameraID = cameras[int(len(cameras)*random.random())]['name']
    timeDT = startTimeDT + datetime.timedelta(seconds = random.random()*timeRangeSeconds)
    # ensure time between 8AM and 8PM because currently focusing on daytime only
    if timeDT.hour < 8:
        timeDT += datetime.timedelta(hours=8)
    elif timeDT.hour >= 20:
        timeDT -= datetime.timedelta(hours=4)
    files = img_archive.getHpwrenImages(constants['googleServices'], settings, downloadDirOrCache,
                                        constants['camArchives'], cameraID, timeDT, timeDT, 1)
    # logging.warning('files %s', str(files))
    if not files:
        return (None, None, None, None)

    # if in cache mode, copy files to temporary directory because they will be deleted later by main loop
    if getArchivedImages.cache:
        tmpFiles = []
        for srcFilePath in files:
            srcFilePP = pathlib.PurePath(srcFilePath)
            destPath = os.path.join(getArchivedImages.tmpDir.name, str(srcFilePP.name))
            shutil.copy(srcFilePath, destPath)
            tmpFiles.append(destPath)
        files = tmpFiles

    if len(files) > 0:
        parsedName = img_archive.parseFilename(files[0])
        return (cameraID, parsedName['unixTime'], files[0], files[0])
    return (None, None, None, None)
getArchivedImages.tmpDir = None
getArchivedImages.cache = None


def fetchPriorAligned(constants, cameraID, heading, timestamp, baseImgPath, outputDirName):
    imgDT = datetime.datetime.fromtimestamp(timestamp)
    dt = imgDT - datetime.timedelta(seconds = 60)
    oldImages = img_archive.getArchiveImages(constants['googleServices'], settings, constants['dbManager'], outputDirName,
                    constants['camArchives'], cameraID, heading, dt, dt, 1)
    if not oldImages:
        return None
    priorImg = None
    if len(oldImages) >= 1:
        # find the most recent aligned image
        oldImages.reverse()
        if img_archive.isPTZ(cameraID): # PTZ iamges require alignment
            for filePath in oldImages:
                img = img_archive.alignImageObj(filePath, baseImgPath)
                if img:
                    priorImg = img
                    break
        else:
            priorImg = Image.open(oldImages[0])
    if priorImg:
        priorImg.load() # force load to allow remove below to succeed on Windows
    for filePath in oldImages:
        os.remove(filePath)
    return priorImg


def fetchDiffImage(constants, cameraID, heading, timestamp, baseImgPath, outputDirName):
    priorImg = fetchPriorAligned(constants, cameraID, heading, timestamp, baseImgPath, outputDirName)
    if not priorImg:
        return None
    imgOrig = Image.open(baseImgPath)
    return img_archive.diffWithChecks(imgOrig, priorImg)


def getGroupConfig():
    groupName = goog_helper.getInstanceGroup()
    if not groupName:
        return None
    groupName = groupName.split('/').pop() # get last path component
    if not settings.detectGroups:
        return None
    groupConfig = next(filter(lambda x: x[0] == groupName, settings.detectGroups), None)
    if not groupConfig:
        return None
    groupParams = {
        'name': groupConfig[0],
        'numInstances': groupConfig[1],
        'counterName': groupConfig[2],
        'restrictType': groupConfig[3],
    }
    logging.warning('GroupConfig %s', groupParams)
    return groupParams


def main():
    optArgs = [
        ["b", "heartbeat", "filename used for heartbeating check"],
        ["c", "collectPositves", "collect positive segments for training data"],
        ["t", "time", "Time breakdown for processing images"],
        ["r", "restrictType", "Only process images from cameras of given type"],
        ["d", "counterName", "Name of row in counters table"],
        ["n", "noState", "(optional) no changes to state"],
        ["s", "startTime", "(optional) performs search with modifiedTime > startTime"],
        ["e", "endTime", "(optional) performs search with modifiedTime < endTime"],
        ["z", "randomSeed", "(optional) override random seed"],
        ["o", "randomOffset", "(optional) random offset - skip given number of random images", int],
        ["l", "limitImages", "(optional) stop after processing given number of images", int],
    ]
    args = collect_args.collectArgs([], optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    limitImages = args.limitImages if args.limitImages else 1e9
    # TODO: Fix googleServices auth to resurrect email alerts
    # googleServices = goog_helper.getGoogleServices(settings, args)
    googleServices = None
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    groupConfig = getGroupConfig()
    if args.restrictType:
        restrictType = args.restrictType
    elif groupConfig:
        restrictType = groupConfig['restrictType']
    else:
        restrictType = None
    logging.warning('RestrictType %s', restrictType)

    cameras = dbManager.get_sources(activeOnly=True, restrictType=restrictType)
    logging.warning('Found %d cameras', len(cameras))
    if len(cameras) == 0:
        return
    usableRegions = dbManager.get_usable_regions_dict()
    ignoredViews = dbManager.get_ignoredViews()

    if args.counterName:
        counterName = args.counterName
    elif groupConfig:
        counterName = groupConfig['counterName']
    else:
        counterName = 'sources'
    logging.warning('Counter name %s', counterName)

    startTimeDT = dateutil.parser.parse(args.startTime) if args.startTime else None
    endTimeDT = dateutil.parser.parse(args.endTime) if args.endTime else None
    timeRangeSeconds = None
    useArchivedImages = False
    stateless = True if args.noState else False
    if startTimeDT or endTimeDT:
        assert startTimeDT and endTimeDT
        timeRangeSeconds = (endTimeDT-startTimeDT).total_seconds()
        assert timeRangeSeconds > 0
        assert args.collectPositves
        useArchivedImages = True
        stateless = True
        # if seed not specified, use os.urandom and log value
        randomSeed = args.randomSeed if args.randomSeed else os.urandom(4).hex()
        logging.warning('Random seed %s', randomSeed)
        random.seed(randomSeed, version=2)
        if args.randomOffset:
            for x in range(args.randomOffset):
                # use two random()s each iteration to match getArchivedImages
                random.random()
                random.random()
    camArchives = img_archive.getHpwrenCameraArchives(settings.hpwrenArchives)
    DetectionPolicyClass = policies.get_policies()[settings.detectionPolicy]
    detectionPolicy = DetectionPolicyClass(args, dbManager, stateless=stateless)
    logging.warning('weatherModel %s threshold %s', settings.weather_model, settings.weatherThreshold)
    weatherModel = tf_helper.loadModel(settings.weather_model)
    constants = { # dictionary of constants to reduce parameters in various functions
        'args': args,
        'googleServices': googleServices,
        'camArchives': camArchives,
        'dbManager': dbManager,
        'weatherModel': weatherModel,
        'ignoredViews': ignoredViews,
    }

    numImages = 0
    numProbables = 0
    numAlerts = 0
    processingTimeTracker = initializeTimeTracker()
    while True:
        classifyImgPath = None
        timeStart = time.time()
        if useArchivedImages:
            (cameraID, timestamp, imgPath, classifyImgPath) = \
                getArchivedImages(constants, cameras, startTimeDT, timeRangeSeconds)
            if cameraID:
                heading = img_archive.getHeading(cameraID)
            fov = img_archive.getCameraFov(cameraID)
        else: # regular (non diff mode), grab image and process
            (cameraID, heading, timestamp, fov, imgPath) = getNextImage(dbManager, cameras, stateless, counterName)
            classifyImgPath = imgPath
        if not cameraID:
            continue # skip to next camera
        timeFetch = time.time()

        image_spec = [{}]
        image_spec[-1]['path'] = classifyImgPath
        image_spec[-1]['timestamp'] = timestamp
        image_spec[-1]['cameraID'] = cameraID
        image_spec[-1]['heading'] = heading
        if cameraID in usableRegions:
            usableEntry = usableRegions[cameraID]
            if 'startY' in usableEntry:
                image_spec[-1]['startY'] = usableEntry['startY']
            if 'endY' in usableEntry:
                image_spec[-1]['endY'] = usableEntry['endY']
        # ignore top and bottom 50 (cloud, metadata, too nearby)
        if ('startY' not in image_spec[-1]) or not image_spec[-1]['startY']:
            image_spec[-1]['startY'] = 50
        if ('endY' not in image_spec[-1]) or not image_spec[-1]['endY']:
            image_spec[-1]['endY'] = -50

        detectionResult = detectionPolicy.detect(image_spec, checkShifts=True,
                            fetchDiff=lambda x: fetchDiffImage(constants, cameraID, heading, timestamp, classifyImgPath, x))
        timeDetect = time.time()
        numImages += 1
        fireSegment = detectionResult['fireSegment']
        if fireSegment:
            numProbables += 1
        if fireSegment and not useArchivedImages:
            recordProbables(dbManager, cameraID, heading, timestamp, imgPath, fireSegment, detectionPolicy.modelId, stateless)
            if not (isDuplicateProbables(dbManager, cameraID, heading, timestamp) or stateless):
                fireDetected(constants, cameraID, heading, timestamp, fov, imgPath, fireSegment)
                numAlerts += 1
        if not stateless:
            img_archive.markImageProcessed(dbManager, cameraID, heading, timestamp)
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
            logging.warning('Stats: alerts=%d, detects=%d, images=%d', numAlerts, numProbables, numImages)
            if numImages >= limitImages:
                logging.warning('Reached limit on images')
                return
        # free all memory for current iteration and trigger GC to prevent memory growth
        detectionResult = None
        gc.collect()

if __name__=="__main__":
    main()
