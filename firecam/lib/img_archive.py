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

Library code to manage image archives

"""

from firecam.lib import goog_helper

import os
import logging
import urllib.request
import time, datetime, dateutil.parser
from html.parser import HTMLParser
import requests
import re
import pathlib
import math
from PIL import Image, ImageMath, ImageStat
import numpy as np
import cv2
import shutil

def isPTZ(cameraID):
    return False


def getCameraFov(cameraID):
    return 110 # camera horizontal field of view is 110 for most Mobotix cameras


def getCameraSizeX(cameraID):
    return 3072 # camera horizontal pixel size. Most Mobotix are 3072 x 2048


def fetchUrlHPWren(cameraID, cameraUrl, imgDir, timestamp, imgPath):
    urllib.request.urlretrieve(cameraUrl, imgPath)
    heading = getHeading(cameraID)
    # read EXIF header for original timestamp and rename file
    img = Image.open(imgPath)
    imgExif = ('exif' in img.info) and img.info['exif']
    img.close()
    timeMatch = imgExif and re.findall('(\d+) UTC', imgExif.decode('utf-8','ignore'))
    if timeMatch and len(timeMatch):
        newTimestamp = int(timeMatch[0])
        if (newTimestamp > timestamp - 5*60) and (newTimestamp < timestamp + 5*60):
            newImgPath = getImgPath(imgDir, cameraID, newTimestamp)
            timestamp = newTimestamp
            os.rename(imgPath, newImgPath)
            imgPath = newImgPath
    return (imgPath, heading, timestamp)


def fetchCurrentFromDB(dbManager, cameraID, imgDir, timestamp):
    sqlTemplate = """SELECT i.heading as heading, i.maxts as maxts, o.fieldofview as fov, o.imagepath as maxpath, o.processed as processed
                       FROM archive o
                       INNER JOIN (SELECT heading, max(timestamp) as maxts
                                     FROM archive
                                     WHERE CameraID='%s' and imagepath != '' and timestamp >= %s and timestamp <= %s
                                     GROUP by heading) i
                       ON o.heading=i.heading and o.timestamp=i.maxts
                       WHERE o.CameraID='%s'
                       ORDER by i.maxts"""
    sqlStr = sqlTemplate % (cameraID, timestamp - 5*60, timestamp, cameraID)
    dbResult = dbManager.query(sqlStr)
    result = []
    for imgInfo in dbResult:
        if imgInfo['processed']: # skip already processed images
            continue
        srcFilePP = pathlib.PurePath(imgInfo['maxpath'])
        destPath = os.path.join(imgDir, srcFilePP.name)
        shutil.copy(imgInfo['maxpath'], destPath)
        result.append((destPath, imgInfo['heading'], imgInfo['maxts'], imgInfo['fov']))
    if len(result) > 0:
        return result
    else:
        return None


def fetchImageAndMeta(dbManager, cameraID, cameraUrl, imgDir, newOnly=False):
    """Fetch the image file and metadata for given camera

    Args:
        cameraID (str): ID of camera
        cameraUrl (str): URL with image and metadata
        imgDir (str): Output directory to store iamge

    Returns:
        Tuple containing filepath of the image, current heading and timestamp
    """
    fov = getCameraFov(cameraID)
    timestamp = int(time.time())
    imgPath = getImgPath(imgDir, cameraID, timestamp)
    # logging.warning('Fetching camera %s', cameraID)
    if newOnly:
        (imgPath, heading, timestamp) = fetchUrlHPWren(cameraID, cameraUrl, imgDir, timestamp, imgPath)
        return (imgPath, heading, timestamp, fov)
    else:
        result = fetchCurrentFromDB(dbManager, cameraID, imgDir, timestamp)
        if result:
            return result
        # try newOnly=True
        return fetchImageAndMeta(dbManager, cameraID, cameraUrl, imgDir, newOnly=True)


def getDBImages(dbManager, outputDir, cameraID, heading, startTimeDT, endTimeDT, gapMinutes):
    sqlTemplate = """SELECT timestamp, imagepath FROM archive
        where CameraID='%s' and heading=%s and timestamp >= %s and timestamp <= %s order by timestamp"""
    startTime = int(time.mktime(startTimeDT.timetuple()))
    endTime = int(time.mktime(endTimeDT.timetuple()))
    sqlStr = sqlTemplate % (cameraID, heading, startTime, endTime)
    # logging.warning('getDBImages camera %s query %s', cameraID, sqlStr)
    dbResult = dbManager.query(sqlStr)
    # logging.warning('getDBImages camera %s, reslen %s res %s', cameraID, len(dbResult), len(dbResult) and str(dbResult))
    result = []
    for imgInfo in dbResult:
        srcPath = imgInfo['imagepath']
        srcFilePP = pathlib.PurePath(srcPath)
        destPath = os.path.join(outputDir, srcFilePP.name)
        shutil.copy(srcPath, destPath)
        result.append(destPath)
    return result


def markImageProcessed(dbManager, cameraID, heading, timestamp):
    sqlTemplate = """UPDATE archive SET processed = 1
                        WHERE CameraID='%s' and heading=%s and timestamp = %s"""
    sqlStr = sqlTemplate % (cameraID, heading, timestamp)
    dbManager.execute(sqlStr)


def getImgPath(outputDir, cameraID, timestamp, cropCoords=None, diffMinutes=0):
    """Generate properly formatted image filename path following Firecam conventions
       E.g.: lo-s-mobo-c__2018-06-06T11;12;23_Diff1_Crop_627x632x1279x931.jpg

    Args:
        outputDir (str): Output directory
        cameraID (str): ID of camera
        timestamp (int): timestamp
        cropCoords (tuple): (x0, y0, x1, y1) coordinates of the crop rectangle
        diffMinutes (int): number of minutes separating the images (for subtracted images)

    Returns:
        String to full path name
    """
    timeStr = datetime.datetime.fromtimestamp(timestamp).isoformat()
    timeStr = timeStr.replace(':', ';') # make windows happy
    imgName = '__'.join([cameraID, timeStr])
    if diffMinutes:
        imgName += ('_Diff%d' % diffMinutes)
    if cropCoords:
        imgName += '_Crop_' + 'x'.join(list(map(lambda x: str(x), cropCoords)))
    imgPath = os.path.join(outputDir, imgName + '.jpg')
    return imgPath


def repackFileName(parsedName):
    """Generate properly formatted image filename following Firecam conventions
       based on information from parsedName dictionary
       E.g.: lo-s-mobo-c__2018-06-06T11;12;23_Diff1_Crop_627x632x1279x931.jpg

    Args:
        parsedName (dict): Dictionary containing various attributes of image
                            (likely result from earlier call to parseFilename())

    Returns:
        String to file name
    """
    cropCoords = None
    if 'minX' in parsedName:
        cropCoords=(parsedName['minX'], parsedName['minY'], parsedName['maxX'], parsedName['maxY'])
    return getImgPath('', parsedName['cameraID'], parsedName['unixTime'],
                      cropCoords=cropCoords,
                      diffMinutes=parsedName['diffMinutes'])


def parseFilename(fileName):
    """Parse the image source attributes given the properly formatted image filename

    Args:
        fileName (str):

    Returns:
        Dictionary with parsed out attributes
    """
    # regex to match names like Axis-BaldCA_2018-05-29T16_02_30_129496.jpg
    # and bm-n-mobo-c__2017-06-25z11;53;33.jpg
    regexExpanded = '([A-Za-z0-9-_]+[^_])_+(\d{4}-\d\d-\d\d)T(\d\d)[_;](\d\d)[_;](\d\d)'
    # regex to match diff minutes spec for subtracted images
    regexDiff = '(_Diff(\d+))?'
    # regex to match optional crop information e.g., Axis-Cowles_2019-02-19T16;23;49_Crop_270x521x569x820.jpg
    regexOptionalCrop = '(_Crop_(-?\d+)x(-?\d+)x(\d+)x(\d+))?'
    matchesExp = re.findall(regexExpanded + regexDiff + regexOptionalCrop, fileName)
    # regex to match names like 1499546263.jpg
    regexUnixTime = '(1\d{9})'
    matchesUnix = re.findall(regexUnixTime + regexDiff + regexOptionalCrop, fileName)
    cropInfo = None
    if len(matchesExp) == 1:
        match = matchesExp[0]
        parsed = {
            'cameraID': match[0],
            'date': match[1],
            'hours': match[2],
            'minutes': match[3],
            'seconds': match[4]
        }
        isoStr = '{date}T{hour}:{min}:{sec}'.format(date=parsed['date'],hour=parsed['hours'],min=parsed['minutes'],sec=parsed['seconds'])
        dt = dateutil.parser.parse(isoStr)
        unixTime = time.mktime(dt.timetuple())
        parsed['diffMinutes'] = int(match[6] or 0)
        cropInfo = match[-4:]
    elif len(matchesUnix) == 1:
        match = matchesUnix[0]
        unixTime = int(match[0])
        dt = datetime.datetime.fromtimestamp(unixTime)
        isoStr = datetime.datetime.fromtimestamp(unixTime).isoformat()
        parsed = {
            'cameraID': 'UNKNOWN_' + fileName,
            'date': dt.date().isoformat(),
            'hours': str(dt.hour),
            'minutes': str(dt.minute),
            'seconds': str(dt.second)
        }
        parsed['diffMinutes'] = int(match[2] or 0)
        cropInfo = match[-4:]
    else:
        logging.error('Failed to parse name %s', fileName)
        return None
    if cropInfo[0]:
        parsed['minX'] = int(cropInfo[0])
        parsed['minY'] = int(cropInfo[1])
        parsed['maxX'] = int(cropInfo[2])
        parsed['maxY'] = int(cropInfo[3])
    parsed['isoStr'] = isoStr
    parsed['unixTime'] = int(unixTime)
    return parsed


def getHeading(cameraID):
    """Return the heading (direction in degrees where 0 = North) of the given camera

    Args:
        cameraID: (string) camera ID (e.g. bh-w-mobo-c)

    Returns:
        Numerical heading or None
    """
    cardinalHeadings = {
        'n': 0,
        'e': 90,
        's': 180,
        'w': 270,
        'ne': 45,
        'se': 135,
        'sw': 225,
        'nw': 315,
    }
    regexDirMobo = '-([ns]?[ew]?)-mobo-c'
    matches = re.findall(regexDirMobo, cameraID)
    if len(matches) == 1:
        camDir = matches[0]
        if camDir in cardinalHeadings:
            return cardinalHeadings[camDir]
    return None


class HpwrenHTMLParser(HTMLParser):
    """Dervied class from HTMLParser to pull out file information from HTML directory listing pages
        Allows caller to specify fileType (extension) the caller cares about
    """
    def __init__(self, fileType):
        self.table = []
        self.filetype = fileType
        super().__init__()


    def handle_starttag(self, tag, attrs):
        """Handler for HTML starting tag (<).
           If the tag type is <a> and it contains an href link pointing to file of specified type,
           then save the name for extraction by getTable()

        """
        if (tag == 'a') and len(attrs) > 0:
            # print('Found <a> %s', len(attrs), attrs)
            for attr in attrs:
                # print('Found attr %s', len(attr), attr)
                if len(attr) == 2 and attr[0]=='href' and attr[1][-4:] == self.filetype:
                    self.table.append(attr[1])

    def getTable(self):
        return self.table


def parseDirHtml(dirHtml, fileType):
    """Wrapper around HpwrenHTMLParser to pull out entries of given fileType

    Args:
        dirHtml (str): HTML page for directory listing
        fileType (str): File extension (e.g.: '.jpg')

    Returns:
        List of file names matching extension
    """
    parser = HpwrenHTMLParser(fileType)
    parser.feed(dirHtml)
    return parser.getTable()


def fetchImgOrDir(url, verboseLogs):
    """Read the given URL and return the data.  Also note if data is an image

    Args:
        url (str): URL to read
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        Tuple indicating image or directory and the data
    """
    try:
        resp = urllib.request.urlopen(url)
    except Exception as e:
        if verboseLogs:
            logging.error('Result of fetch from %s: %s', url, str(e))
        return (None, None)
    if resp.getheader('content-type') == 'image/jpeg':
        return ('img', resp)
    else:
        return ('dir', resp)


def readUrlDir(urlPartsQ, verboseLogs, fileType):
    """Get the files of given fileType from the given HPWREN Q directory URL

    Args:
        urlPartsQ (list): HPWREN Q directory URL as list of string parts
        verboseLogs (bool): Write verbose logs for debugging
        fileType (str): File extension (e.g.: '.jpg')

    Returns:
        List of file names matching extension
    """
    # logging.warning('Dir URLparts %s', urlPartsQ)
    url = '/'.join(urlPartsQ)
    # logging.warning('Dir URL %s', url)
    (imgOrDir, resp) = fetchImgOrDir(url, verboseLogs)
    if not imgOrDir:
        return None
    assert imgOrDir == 'dir'
    dirHtml = resp.read().decode('utf-8')
    return parseDirHtml(dirHtml, fileType)


def listTimesinQ(urlPartsQ, verboseLogs):
    """Get the timestamps of images from the given HPWREN Q directory URL

    Args:
        urlPartsQ (list): HPWREN Q directory URL as list of string parts
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        List of timestamps
    """
    files = readUrlDir(urlPartsQ, verboseLogs, '.jpg')
    if files:
        return list(map(lambda x: {'time': int(x[:-4])}, files))
    return None


def downloadHttpFileAtTime(outputDir, urlPartsQ, cameraID, closestTime, verboseLogs):
    """Download HPWREN image from given HPWREN Q directory URL at given time

    Args:
        outputDir (str): Output directory path
        urlPartsQ (list): HPWREN Q directory URL as list of string parts
        cameraID (str): ID of camera
        closestTime (int): Desired timestamp
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        Local filesystem path to downloaded image
    """
    imgPath = getImgPath(outputDir, cameraID, closestTime)
    if verboseLogs:
        logging.warning('Local file %s', imgPath)
    if os.path.isfile(imgPath):
        logging.warning('File %s already downloaded', imgPath)
        return imgPath

    closestFile = str(closestTime) + '.jpg'
    urlParts = urlPartsQ[:] # copy URL parts array
    urlParts.append(closestFile)
    # logging.warning('File URLparts %s', urlParts)
    url = '/'.join(urlParts)
    logging.warning('File URL %s', url)

    # urllib.request.urlretrieve(url, imgPath)
    resp = requests.get(url, stream=True)
    with open(imgPath, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    resp.close()
    return imgPath


def downloadGCSFileAtTime(outputDir, closestEntry):
    """Download HPWREN image from GCS folder from ffmpeg Google Cloud Function

    Args:
        outputDir (str): Output directory path
        closestEntry (dict): Desired timestamp and GCS file

    Returns:
        Local filesystem path to downloaded image
    """
    imgPath = os.path.join(outputDir, closestEntry['name'])
    logging.warning('Local file %s', imgPath)
    if os.path.isfile(imgPath):
        logging.warning('File %s already downloaded', imgPath)
        return imgPath

    parsedPath = goog_helper.parseGCSPath(closestEntry['id'])
    goog_helper.downloadBucketFile(parsedPath['bucket'], parsedPath['name'], imgPath)
    return imgPath


def getMp4Url(urlPartsDate, qNum, verboseLogs):
    """Get the URL for the MP4 video for given Q

    Args:
        urlPartsDate (list): HPWREN date directory URL as list of string parts
        qNum (int): Q number (1-8) where each Q represents 3 hour period
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        URL to Q diretory
    """
    urlPartsMp4 = urlPartsDate[:] # copy URL
    urlPartsMp4.append('MP4')
    files = readUrlDir(urlPartsMp4, verboseLogs, '.mp4')
    if verboseLogs:
        logging.warning('MP4s %s', files)
    qMp4Name = 'Q' + str(qNum) + '.mp4'
    if files and (qMp4Name in files):
        urlPartsMp4.append(qMp4Name)
        return '/'.join(urlPartsMp4)
    return None


def gcfFfmpeg(gcfUrl, googleServices, hpwrenSource, qNum, folderID):
    """invoke the Google Cloud Function for ffpeg decompression with proper parameters and credentials

    Args:
        gcfUrl (str): URL for ffmpeg cloud function
        googleServices (): Google services and credentials
        hpwrenSource (dict): Dictionary containing various HPWREN source information
        qNum (int): Q number (1-8) where each Q represents 3 hour period
        folderID (str): google drive ID of folder where to extract images

    Returns:
        Cloud function result
    """
    gcfParams = {
        'hostName': hpwrenSource['server'],
        'cameraID': hpwrenSource['cameraID'],
        'archiveCamDir': hpwrenSource['urlParts'][1],
        'yearDir': hpwrenSource['year'],
        'dateDir': hpwrenSource['dateDirName'],
        'qNum': qNum,
        'uploadDir': folderID
    }
    maxRetries = 3
    retriesLeft = maxRetries
    while retriesLeft > 0:
        token = goog_helper.getIdToken(googleServices, gcfUrl, retriesLeft != maxRetries)
        headers = {'Authorization': 'bearer {}'.format(token)}
        rawResponse = requests.post(gcfUrl, headers=headers, data=gcfParams)
        response = rawResponse.content.decode()
        if response == 'done':
            return response
        retriesLeft -= 1
        logging.error('Error calling GCF. %d retries left.  resp=%s  raw=%s', retriesLeft, str(response), str(rawResponse))
        time.sleep(5) # wait 5 seconds before retrying
    return response


def getGCSMp4(googleServices, settings, hpwrenSource, qNum):
    """Extract images from Q MP4 video into GCS folder

    Args:
        googleServices (): Google services and credentials
        settings (): settings module
        hpwrenSource (dict): Dictionary containing various HPWREN source information
        qNum (int): Q number (1-8) where each Q represents 3 hour period

    Returns:
        list of files in GCS bucket with metadata
    """
    ffmpegParsedGCS = goog_helper.parseGCSPath(settings.ffmpegFolder)
    folderName = hpwrenSource['cameraID'] + '__' + hpwrenSource['dateDirName'] + 'Q' + str(qNum)
    folderPath = ffmpegParsedGCS['name'] + '/' + folderName
    files = goog_helper.listBucketEntries(ffmpegParsedGCS['bucket'], prefix=(folderPath + '/'))
    logging.warning('Found %d GCS files', len(files))
    if not files:
        logging.warning('Calling Cloud Function for folder %s', folderName)
        uploadDir = goog_helper.repackGCSPath(ffmpegParsedGCS['bucket'],folderPath)
        gcfRes = gcfFfmpeg(settings.ffmpegUrl, googleServices, hpwrenSource, qNum, uploadDir)
        logging.warning('Cloud function result %s', gcfRes)
        files = goog_helper.listBucketEntries(ffmpegParsedGCS['bucket'], prefix=(folderPath + '/'))
    # logging.warning('GDM4: files %d %s', len(files), files)
    imgTimes = []
    for filePath in files:
        fileName = filePath.split('/')[-1]
        nameParsed = parseFilename(fileName)
        imgTimes.append({
            'time': nameParsed['unixTime'],
            'id': goog_helper.repackGCSPath(ffmpegParsedGCS['bucket'], filePath),
            'name': fileName
        })
    return imgTimes


outputDirCheckOnly = '/CHECK:WITHOUT:DOWNLOAD'
def downloadFilesForDate(googleServices, settings, outputDir, hpwrenSource, gapMinutes, verboseLogs):
    """Download HPWREN images from given given date time range with specified gaps

    If outputDir is special value outputDirCheckOnly, then just check if files are retrievable

    Args:
        googleServices (): Google services and credentials
        settings (): settings module
        outputDir (str): Output directory path
        hpwrenSource (dict): Dictionary containing various HPWREN source information
        gapMinutes (int): Number of minutes of gap between images for downloading
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        List of local filesystem paths to downloaded images
    """
    startTimeDT = hpwrenSource['startTimeDT']
    endTimeDT = hpwrenSource['endTimeDT']
    dateDirName = '{year}{month:02d}{date:02d}'.format(year=startTimeDT.year, month=startTimeDT.month, date=startTimeDT.day)
    hpwrenSource['dateDirName'] = dateDirName
    urlPartsDate = hpwrenSource['urlParts'][:] # copy URL
    urlPartsDate.append(dateDirName)
    hpwrenSource['urlPartsDate'] = urlPartsDate

    timeGapDelta = datetime.timedelta(seconds = 60*gapMinutes)
    imgTimes = None
    lastQNum = 0 # 0 never matches because Q numbers start with 1
    curTimeDT = startTimeDT
    downloaded_files = []
    prevTime = None
    while curTimeDT <= endTimeDT:
        qNum = 1 + int(curTimeDT.hour/3)
        urlPartsQ = urlPartsDate[:] # copy URL
        urlPartsQ.append('Q' + str(qNum))
        if qNum != lastQNum:
            # List times of files in Q dir and cache
            useHttp = True
            imgTimes = listTimesinQ(urlPartsQ, verboseLogs)
            if not imgTimes:
                if verboseLogs:
                    logging.error('No images in Q dir %s', '/'.join(urlPartsQ))
                mp4Url = getMp4Url(urlPartsDate, qNum, verboseLogs)
                if not mp4Url:
                    return downloaded_files
                if outputDir != outputDirCheckOnly:
                    imgTimes = getGCSMp4(googleServices, settings, hpwrenSource, qNum)
                    useHttp = False
                    # logging.warning('imgTimes %d %s', len(imgTimes), imgTimes)
            lastQNum = qNum

        if outputDir == outputDirCheckOnly:
            downloaded_files.append(outputDirCheckOnly)
        else:
            desiredTime = time.mktime(curTimeDT.timetuple())
            closestEntry = min(imgTimes, key=lambda x: abs(x['time']-desiredTime))
            closestTime = closestEntry['time']
            downloaded = None
            if closestTime != prevTime: # skip if closest timestamp is still same as previous iteration
                prevTime = closestTime
                if useHttp:
                    downloaded = downloadHttpFileAtTime(outputDir, urlPartsQ, hpwrenSource['cameraID'], closestTime, verboseLogs)
                else:
                    downloaded = downloadGCSFileAtTime(outputDir, closestEntry)
            if downloaded and verboseLogs:
                logging.warning('Successful download for time %s', str(datetime.datetime.fromtimestamp(closestTime)))
            if downloaded:
                downloaded_files.append(downloaded)

        curTimeDT += timeGapDelta
    return downloaded_files


def downloadFilesHpwren(googleServices, settings, outputDir, hpwrenSource, gapMinutes, verboseLogs):
    """Download HPWREN images from given given date time range with specified gaps

    Calls downloadFilesForDate to do the heavy lifting, but first determines the hpwren server.
    First tries without year directory in URL path, and if that fails, then retries with year dir

    Args:
        googleServices (): Google services and credentials
        settings (): settings module
        outputDir (str): Output directory path
        hpwrenSource (dict): Dictionary containing various HPWREN source information
        gapMinutes (int): Number of minutes of gap between images for downloading
        verboseLogs (bool): Write verbose logs for debugging

    Returns:
        List of local filesystem paths to downloaded images
    """
    regexDir = '(c[12])/([^/]+)/large/?'
    matches = re.findall(regexDir, hpwrenSource['dirName'])
    if len(matches) != 1:
        logging.error('Could not parse dir: %s', hpwrenSource['dirName'])
        return None
    match = matches[0]
    (server, subdir) = match
    # hpwrenBase = 'http://{server}.hpwren.ucsd.edu/archive'.format(server=server)
    # hpwrenSource['server'] = server
    hpwrenBase = 'https://hpwren.ucsd.edu/cameras/archive'
    hpwrenSource['server'] = ''
    urlParts = [hpwrenBase, subdir, 'large']
    hpwrenSource['urlParts'] = urlParts

    # first try without year directory
    hpwrenSource['year'] = ''
    downloaded_files = downloadFilesForDate(googleServices, settings, outputDir, hpwrenSource, gapMinutes, verboseLogs)
    if downloaded_files:
        return downloaded_files
    # retry with year directory
    hpwrenSource['year'] = str(hpwrenSource['startTimeDT'].year)
    urlParts.append(hpwrenSource['year'])
    hpwrenSource['urlParts'] = urlParts
    return downloadFilesForDate(googleServices, settings, outputDir, hpwrenSource, gapMinutes, verboseLogs)


def getHpwrenCameraArchives(hpwrenArchivesPath):
    """Get the HPWREN camera archive directories from given file

    Args:
        hpwrenArchivesPath (str): path (local of GCS) to file with archive info

    Returns:
        List of archive directories
    """
    archiveData = goog_helper.readFile(hpwrenArchivesPath)
    camArchives = []
    for line in archiveData.split('\n'):
        camInfo = line.split(' ')
        # logging.warning('info %d, %s', len(camInfo), camInfo)
        if len(camInfo) != 2:
            logging.warning('Ignoring archive entry without two columns %s', camInfo)
            continue
        dirInfo = camInfo[1].split('/')
        if len(dirInfo) < 2:
            logging.warning('Ignoring archive entry without proper ID %s', dirInfo)
            continue
        cameraID = dirInfo[1]
        matchesID = list(filter(lambda x: cameraID == x['id'], camArchives))
        if matchesID:
            if camInfo[1] not in matchesID[0]['dirs']:
                matchesID[0]['dirs'].append(camInfo[1])
                # logging.warning('Merging duplicate ID dir %s, %s', camInfo[1], matchesID[0])
            continue
        preIndex = camInfo[0].find('pre')
        if preIndex > 0:
            searchName = camInfo[0][:(preIndex-1)]
            matchesName = list(filter(lambda x: searchName in x['name'], camArchives))
            for match in matchesName:
                if camInfo[1] not in match['dirs']:
                    match['dirs'].append(camInfo[1])
                    # logging.warning('Mergig pre dir %s to %s', camInfo[1], match)
            continue
        camData = {'id': cameraID, 'name': camInfo[0], 'dirs': [camInfo[1]]}
        # logging.warning('data %s', camData)
        camArchives.append(camData)
    logging.warning('Discovered total %d camera archive dirs', len(camArchives))
    return camArchives


def findCameraInArchive(camArchives, cameraID):
    """Find the entries in the camera archive directories for the given camera

    Args:
        camArchives (list): Result of getHpwrenCameraArchives() above
        cameraID (str): ID of camera to fetch images from

    Returns:
        List of archive dirs that matching camera
    """
    matchingCams = list(filter(lambda x: cameraID == x['id'], camArchives))
    # logging.warning('Found %d match(es): %s', len(matchingCams), matchingCams)
    if matchingCams:
        return matchingCams[0]['dirs']
    else:
        return []


def getHpwrenImages(googleServices, settings, outputDir, camArchives, cameraID, startTimeDT, endTimeDT, gapMinutes):
    """Download HPWREN images from given camera and date time range with specified gaps

    Iterates over all directories for given camera in the archives and then downloads the images
    by calling downloadFilesHpwren

    Args:
        googleServices (): Google services and credentials
        settings (): settings module
        outputDir (str): Output directory path or cache object
        camArchives (list): Result of getHpwrenCameraArchives() above
        cameraID (str): ID of camera to fetch images from
        startTimeDT (datetime): starting time of time range
        endTimeDT (datetime): ending time of time range
        gapMinutes (int): Number of minutes of gap between images for downloading

    Returns:
        List of local filesystem paths to downloaded images
    """
    # If outputDir is a cache object, fetch the real outputDir and set 'cache' variable
    cache = None
    if (not isinstance(outputDir, str)) and ('writeDir' in outputDir):
        cache = outputDir
        outputDir = cache['writeDir']

    # In cache mode, check local cache for existing files before checking remote archive
    if cache:
        curTimeDT = startTimeDT
        timeGapDelta = datetime.timedelta(seconds = 60*gapMinutes)
        downloaded_files = []
        while curTimeDT <= endTimeDT:
            filePath = cacheFindEntry(cache, cameraID, time.mktime(curTimeDT.timetuple()))
            if filePath:
                downloaded_files.append(filePath)
            else:
                downloaded_files = []
                break
            curTimeDT += timeGapDelta
        if len(downloaded_files) > 0:
            # all files are in cache, return results
            logging.warning('already downloaded: %s', downloaded_files)
            return downloaded_files

    matchingDirs = findCameraInArchive(camArchives, cameraID)
    found = None
    for matchingDir in matchingDirs:
        hpwrenSource = {
            'cameraID': cameraID,
            'dirName': matchingDir,
            'startTimeDT': startTimeDT,
            'endTimeDT': endTimeDT
        }
        logging.warning('Searching for files in dir %s', hpwrenSource['dirName'])
        found = downloadFilesHpwren(googleServices, settings, outputDir, hpwrenSource, gapMinutes, False)
        if found:
            break
    # If new files were added to cache directory, update cache object
    if cache and found and (cache['readDir'] == cache['writeDir']):
        for filePath in found:
            cacheInsert(cache, filePath)
    return found


def getArchiveImages(googleServices, settings, dbManager, outputDir, camArchives, cameraID, heading, startTimeDT, endTimeDT, gapMinutes):
    dbImages = getDBImages(dbManager, outputDir, cameraID, heading, startTimeDT, endTimeDT, gapMinutes)
    if dbImages:
        return dbImages
    result = getHpwrenImages(googleServices, settings, outputDir, camArchives, cameraID, startTimeDT, endTimeDT, gapMinutes)
    result = result or []
    return result


def cacheInsert(cache, fileName):
    """Insert given file into given cache object

    Args:
        cache (dict): Cache object created by cacheDir()
        fileName (str): name or path of file to insert
    """
    nameParsed = parseFilename(fileName)
    if nameParsed and nameParsed['cameraID']:
        cameraID = nameParsed['cameraID']
        unixTime = nameParsed['unixTime']
        if not cameraID in cache:
            cache[cameraID] = []
        cameraTimes = cache[cameraID]
        ppath = pathlib.PurePath(fileName)
        cameraTimes.append({'time': unixTime, 'fileName': str(ppath.name)})


def cacheFindEntry(cache, cameraID, desiredTime):
    """Search given cache for image from given camera at given timestamp (within 30 seconds)

    Args:
        cache (dict): Cache object created by cacheDir()
        cameraID (str): ID of camera to fetch images from
        desiredTime (int): unix time of desired image

    Returns:
        File path of image or None
    """
    if not cameraID in cache:
        return None
    cameraTimes = cache[cameraID]
    closestEntry = min(cameraTimes, key=lambda x: abs(x['time'] - desiredTime))
    if abs(closestEntry['time'] - desiredTime) < 30:
        # logging.warning('close: %s', str(closestEntry))
        return os.path.join(cache['readDir'], closestEntry['fileName'])
    else:
        # logging.warning('far: %s, %s', str(desiredTime), str(closestEntry))
        return None


def cacheFetchRange(cache, cameraID, maxTime, desiredOffset, minOffset):
    if not cameraID in cache:
        return None
    cameraTimes = cache[cameraID]
    minTime = maxTime + minOffset
    desiredTime = maxTime + desiredOffset
    allowedEntries = list(filter(lambda x: (x['time'] > minTime) and (x['time'] < maxTime), cameraTimes))
    if len(allowedEntries) == 0:
        return None
    sortedEntries = sorted(allowedEntries, key=lambda x: abs(x['time'] - desiredTime))
    return list(map(lambda x: os.path.join(cache['readDir'], x['fileName']), sortedEntries))


def cacheDir(readDirPath, writeDirPath=None):
    """Create a cache of iamges in given directory and return the cache object

    Args:
        readDirPath (str): path to directory containing images

    Returns:
        Cache object
    """
    imageFileNames = sorted(os.listdir(readDirPath))
    cache = {
        'readDir': readDirPath,
        'writeDir': writeDirPath or readDirPath
    }
    for fileName in imageFileNames:
        if fileName[-4:] != '.jpg':
            continue
        cacheInsert(cache, fileName)
    return cache


def findTranslationOffset(cvImgA, cvImgB, maxIterations, eps):
    if cvImgA.shape[0] > 1000:
        headerHeight = 250 # clouds, metadata, and watermark
        footerHeight = 250 # nearby trees moving with wind and shadows, metadata, and watermark
    elif cvImgA.shape[0] > 300:
        headerHeight = 100 # clouds, metadata, and watermark
        footerHeight = 100 # nearby trees moving with wind and shadows, metadata, and watermark
    else:
        headerHeight = 0 # too small for headers and footers
        footerHeight = 0
    footerPos = cvImgA.shape[0] - footerHeight
    grayA = cv2.cvtColor(cvImgA[headerHeight:footerPos], cv2.COLOR_BGR2GRAY)
    grayB = cv2.cvtColor(cvImgB[headerHeight:footerPos], cv2.COLOR_BGR2GRAY)
    warp_matrix = np.eye(2, 3, dtype=np.float32)

    try:
        # find optimal shifts limited to given maxIterations
        criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, maxIterations, eps)
        (cc0, warp_matrix0) = cv2.findTransformECC(grayA, grayB, warp_matrix, cv2.MOTION_TRANSLATION, criteria)

        # check another 10 iterations to determine if findTransformECC has converged
        criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 10, eps)
        (cc1, warp_matrix1) = cv2.findTransformECC(grayA, grayB, warp_matrix0, cv2.MOTION_TRANSLATION, criteria)
    except Exception as e:
        return (False, None, None) # alignment failed

    epsAllowance = 10 * eps # allow up 10 eps change to mean convergence (truly unaligned images are > 1000*eps)
    dx = warp_matrix1[0][2]
    dy = warp_matrix1[1][2]
    logging.warning('Translation: %s: %s, %s, %s , %s', cc0 >= cc1 - epsAllowance, round((cc1-cc0)/eps,1), round(cc1, 4), round(dx, 1), round(dy, 1))
    if (cc0 < cc1 - epsAllowance) or (cc1 < 0.85) or (abs(dx) > 20) or (abs(dy) > 10):
        return (False, None, None) # alignment failed
    return (True, dx, dy)


def alignImageObj(imgFileName, baseImgFileName, noShift=False):
    maxIterations = 40
    terminationEps = 1e-6
    imgCv = cv2.imread(imgFileName)
    baseImgCv = cv2.imread(baseImgFileName)
    (alignable, dx, dy) = findTranslationOffset(baseImgCv, imgCv, maxIterations, terminationEps)
    if alignable:
        if round(dx) == 0 and round(dy) == 0: # optimization for sub-pixel shifts
            return Image.open(imgFileName)
        if noShift:
            return None
        logging.warning('shifting image dx, dy: %s, %s', round(dx), round(dy))
        img = Image.open(imgFileName)
        shiftedImg = img.transform(img.size, Image.AFFINE, (1, 0, dx, 0, 1, dy))
        img.close()
        return shiftedImg
    return None


def alignImage(imgFileName, baseImgFileName):
    shiftedImg = alignImageObj(imgFileName, baseImgFileName)
    if shiftedImg:
        shiftedImg.load() # ensure file read before remove
        os.remove(imgFileName)
        shiftedImg.save(imgFileName, format='JPEG', quality=95)
        shiftedImg.close()
        return True
    return False


def diffImages(imgA, imgB):
    """Subtract two images (r-r, g-g, b-b), and add the absolute value of that difference
       in the red band while removing from green and blue to maintain same brightness level
       Out of range values (<0 and > 255) are moved to 0 and 255 by the convert('L') function

    Args:
        imgA: Pillow image object to subtract from
        imgB: Pillow image object to subtract

    Returns:
        Pillow image object containing the results of the subtraction with 128 mean
    """
    bandsImgA = imgA.split()
    bandsImgB = imgB.split()

    absDiff = ImageMath.eval("convert(abs(a0-b0) + abs(a1-b1) + abs(a2-b2), 'L')",
        a0 = bandsImgA[0], b0 = bandsImgB[0],
        a1 = bandsImgA[1], b1 = bandsImgB[1],
        a2 = bandsImgA[2], b2 = bandsImgB[2])
    bandsImgOut = [
        ImageMath.eval("convert(a + 2*diff, 'L')", a = bandsImgA[0], diff = absDiff),
        ImageMath.eval("convert(a - diff, 'L')", a = bandsImgA[1], diff = absDiff),
        ImageMath.eval("convert(a - diff, 'L')", a = bandsImgA[2], diff = absDiff),
    ]

    return Image.merge('RGB', bandsImgOut)


def smoothAndCache(imgPath, outputDir):
    ppath = pathlib.PurePath(imgPath)
    # add 's_' prefix to denote smoothed images
    smoothImgPath = os.path.join(outputDir, 's_' + str(ppath.name))
    if os.path.isfile(smoothImgPath): # smooth image already generated
        return smoothImgPath

    img = cv2.imread(imgPath)
    smoothImg = cv2.fastNlMeansDenoisingColored(img, None, 10,10,7,21)
    cv2.imwrite(smoothImgPath, smoothImg)
    return smoothImgPath


def diffSmoothImageFiles(imgAFile, imgBFile, cachedSmoothDir='.'):
    smoothImgAPath = smoothAndCache(imgAFile, cachedSmoothDir)
    smoothImgAPillow = Image.open(smoothImgAPath)

    smoothImgBPath = smoothAndCache(imgBFile, cachedSmoothDir)
    smoothImgBPillow = Image.open(smoothImgBPath)

    return diffImages(smoothImgAPillow, smoothImgBPillow)


def smoothImage(img):
    """Smooth the given image

    Args:
        img: Pillow image object

    Returns:
        Pillow image object after smoothing
    """
    # Pillow uses RGB and cv2 uses GBR, so have to convert before and after smoothing
    imgBGR = cv2.cvtColor(np.asarray(img), cv2.COLOR_BGR2RGB)
    # smoothImgBGR = cv2.fastNlMeansDenoisingColored(imgBGR, None, 10,10,7,21)
    smoothImgBGR = cv2.bilateralFilter(imgBGR, 9, 75, 75)
    smoothImgRGB = cv2.cvtColor(smoothImgBGR, cv2.COLOR_BGR2RGB)
    return Image.fromarray(smoothImgRGB)


def diffSmoothImages(imgA, imgB):
    """Subtract two images (r-r, g-g, b-b) after smoothing them first.

    Args:
        imgA: Pillow image object to subtract from
        imgB: Pillow image object to subtract

    Returns:
        Pillow image object containing the results of the subtraction with 128 mean
    """

    smoothImgA = smoothImage(imgA)
    smoothImgB = smoothImage(imgB)

    return diffImages(smoothImgA, smoothImgB)


def rescaleValues(img, ratios):
    bandsImg = img.split()
    evalCmds = [
        "convert(float(a)*%s, 'L')" % ratios[0],
        "convert(float(a)*%s, 'L')" % ratios[1],
        "convert(float(a)*%s, 'L')" % ratios[2],
    ]
    bandsImgOut = [
        ImageMath.eval(evalCmds[0], a = bandsImg[0]),
        ImageMath.eval(evalCmds[1], a = bandsImg[1]),
        ImageMath.eval(evalCmds[2], a = bandsImg[2]),
    ]
    return Image.merge('RGB', bandsImgOut)


def brightness(img):
    medians = ImageStat.Stat(img).median
    brightness = (medians[0] + medians[1] + medians[2]) / 3
    return max(brightness, .01) # to avoid div by 0


def diffWithChecks(baseImg, earlierImg):
    brightnessRatio = brightness(baseImg)/brightness(earlierImg)
    if (brightnessRatio < 0.92) or (brightnessRatio > 1.08): # large diffs hide the smoke
        logging.warning('Skipping extreme brigthness diff %s', brightnessRatio)
        return None
    diffImg = diffSmoothImages(baseImg, earlierImg)
    extremas = diffImg.getextrema()
    if (extremas[0][0] == 128 and extremas[0][1] == 128) or (extremas[1][0] == 128 and extremas[1][1] == 128) or (extremas[2][0] == 128 and extremas[2][1] == 128):
        logging.warning('Skipping no diffs %s', str(extremas))
        return None
    return diffImg


def getHeadingRange(centralHeading, fov, minX, maxX, imgSizeX):
    """Return heading (degrees 0 = North) and range of uncertainty of heading
       for the potential fire direction from given camera

    Args:
        centralHeading (int): direction camera is facing
        fov (int): degrees in field of view (horizontal)
        minX (int): fire segment minX
        maxX (int): fire segment maxX
        imgSizeX (int): image size in pixels in X (horizontal) dimension

    Returns:
        Tuple (int, int): heading and uncertainty of heading
    """
    degreesInView = fov
    degreesAlignmentError = 10 # camera directions are not perfectly calibrated to north pole

    # calculate heading
    centerX = (minX + maxX) / 2
    angleFromCenter = centerX / imgSizeX * degreesInView - degreesInView/2
    heading = (angleFromCenter + centralHeading) % 360

    # calculate rangeAngle
    range = (maxX - minX) / imgSizeX * degreesInView + degreesAlignmentError
    return (round(heading), math.ceil(range))


def intersectsAngleRange(heading1, range1, heading2, range2):
    minHeading1 = int(heading1 - range1 / 2) % 360
    maxHeading1 = math.ceil(heading1 + range1 / 2) % 360
    minHeading2 = int(heading2 - range2 / 2) % 360
    maxHeading2 = math.ceil(heading2 + range2 / 2) % 360
    if minHeading1 < maxHeading1 and minHeading2 < maxHeading2: # niether range straddle 0
        if (maxHeading2 > minHeading1 and minHeading2 < maxHeading1):
            return True
    elif minHeading1 < maxHeading1 or minHeading2 < maxHeading2: # one of the ranges doesn't straddle 0, other does
        if (maxHeading2 > minHeading1 or minHeading2 < maxHeading1):
            return True
    else: #both straddle 0
        return True
    return False


def findIgnoredView(ignoredViews, cameraID, heading, fov):
    camMatches = list(filter(lambda x: x['cameraid'] == cameraID, ignoredViews))
    for entry in camMatches:
        if intersectsAngleRange(heading, fov, entry['heading'], entry['angularwidth']):
            return entry
    return None


def findIgnoredViewHeading(ignoredViews, cameraID, heading, fov):
    entry = findIgnoredView(ignoredViews, cameraID, heading, fov)
    if entry:
        return entry['heading']
    return None


def unionAngleRanges(heading1, range1, heading2, range2):
    # assume overlap exists, will be verified later
    assert range1 > 0
    assert range2 > 0
    rotationBase = (heading1 - range1 / 2) % 360
    # rotate everything by rotationBase so angle1 goes from 0 -> range1 to simplify union logic
    maxHeading1 = range1
    minHeading2 = int(heading2 - range2 / 2 - rotationBase) % 360
    maxHeading2 = math.ceil(heading2 + range2 / 2 - rotationBase) % 360
    if minHeading2 < maxHeading2:  # angle2 doesn't straddle 0
        assert minHeading2 < maxHeading1
        minUnion = 0
        maxUnion = max(maxHeading1, maxHeading2)
        heading = round(maxUnion / 2)
        range = maxUnion
    else: # angle2 straddles 0
        minUnion = minHeading2
        maxUnion = max(maxHeading1, maxHeading2)
        range = maxUnion + 360 - minUnion
        heading = round(minUnion + range / 2)
    # rotate back by rotationBase to get back to original heading basis
    assert range > 0
    heading = round(heading + rotationBase) % 360
    range = math.ceil(min(range, 360))
    return (heading, range)


def pointInArea(leftLongitude, rightLongitude, topLatitude, bottomLatitude, latLong):
    vertexInRegion = (latLong[0] >= bottomLatitude) and (latLong[0] <= topLatitude) and \
                        (latLong[1] >= leftLongitude) and (latLong[1] <= rightLongitude)
    return vertexInRegion


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
