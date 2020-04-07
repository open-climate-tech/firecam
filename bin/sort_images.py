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

Displays all images in a folder or zip file for users to mark out smoke bounding box.  The images and coordinates are uploaded to google cloud.

The inputs are zip file, fire ID, camera ID, timestamp of minimal smoke, timestamp of significant enough smoke for cropping.
The zip file should include at least one picture from before minimal smoke and most pictures between minimal smoke and significant smoke, and some number of images after as well

This script will unzip the images, update the image metadata sheet, and upload the images to google drive

"""

import sys
import os
fuegoRoot = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper
import img_archive

sys.path.insert(0, os.path.join(settings.fuegoRoot, 'image_crop'))
import crop_single

import zipfile
import tempfile
import pathlib
import datetime
import dateutil.parser
import time
import re
import logging
import shutil

from googleapiclient.discovery import build
from httplib2 import Http
from tkinter.filedialog import askdirectory

def uploadToDrive(service, imgPath, cameraID, imgClass):
    parent = settings.IMG_CLASSES[imgClass]
    dirName = ''
    dirID = parent
    if cameraID != None:
        (dirID, dirName) = goog_helper.getDirForClassCamera(service, settings.IMG_CLASSES, imgClass, cameraID)

    goog_helper.uploadFile(service, dirID, imgPath)
    print('Uploaded file ', imgPath, ' to ', imgClass, dirName)


def getTimeFromName(imgName):
    return img_archive.parseFilename(imgName)


def renameToIso(dirName, imgName, times, cameraId):
    oldFullPath = os.path.join(dirName, imgName)
    newFullPath = img_archive.getImgPath(dirName, cameraId, times['unixTime'])
    print(oldFullPath, newFullPath)
    os.rename(oldFullPath, newFullPath)
    return newFullPath


def appendToMainSheet(service, imgPath, times, cameraID, imgClass, fireID):
    # result = service.spreadsheets().values().get(spreadsheetId=settings.imagesSheet,
    #                                             range=settings.imagesSheetAppendRange).execute()
    # print(result)
    # values = result.get('values', [])
    # print(values)

    imgName = pathlib.PurePath(imgPath).name
    timeStr = datetime.datetime.fromtimestamp(times['unixTime']).strftime('%F %T')

    value_input_option="USER_ENTERED" # vs "RAW"
    values = [[
        imgName,
        imgClass,
        fireID,
        cameraID,
        timeStr, #time
        "yes" if imgClass == 'smoke' else "no", #smoke boolean
        "no", #fog boolean
        "no", #rain boolean
        "no", #glare boolean
        "no" #snow boolean
        ]]
    body = {
        'values': values
    }
    result = service.spreadsheets().values().append(
        spreadsheetId=settings.imagesSheet, range=settings.imagesSheetAppendRange,
        valueInputOption=value_input_option, body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))


def appendToCropSheet(service, cropPath, coords, basePath):
    cropName = pathlib.PurePath(cropPath).name
    baseName = pathlib.PurePath(basePath).name
    value_input_option="USER_ENTERED" # vs "RAW"
    values = [[
        cropName,
        coords[0],
        coords[1],
        coords[2],
        coords[3],
        baseName
        ]]
    body = {
        'values': values
    }
    result = service.spreadsheets().values().append(
        spreadsheetId=settings.cropImagesSheet, range=settings.cropImagesSheetAppendRange,
        valueInputOption=value_input_option, body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))


def unzipFile(zipFile):
    tempDir = tempfile.TemporaryDirectory()
    print('tempDir', tempDir.name)
    zip_ref = zipfile.ZipFile(zipFile, "r")
    zip_ref.extractall(tempDir.name)
    return tempDir


def processFolder(imgDirectory, camera, fire, googleServices):
    temporaryDir = tempfile.TemporaryDirectory()
    imageFileNames = os.listdir(imgDirectory)
    # print('images', len(imageFileNames), imageFileNames)
    # discard files that don't match the expected file name pattern (e.g. .DS_Store)
    imageFileNames = list(filter(getTimeFromName, imageFileNames))
    # print('images2', len(imageFileNames), imageFileNames)
    # we want to process in time order, so first create tuples with associated time
    tuples=list(map(lambda x: (x,getTimeFromName(x)['unixTime']), imageFileNames))
    lastSmokeTimestamp=None
    for tuple in sorted(tuples, key=lambda x: x[1]):
        imgName=tuple[0]
        times = getTimeFromName(imgName)
        newPath = renameToIso(imgDirectory, imgName, times, camera)
        imgClass = 'smoke'
        print(imgClass, newPath)
        uploadToDrive(googleServices['drive'], newPath, camera, imgClass)
        appendToMainSheet(googleServices['sheet'], newPath, times, camera, imgClass, fire)
        if (lastSmokeTimestamp == None) or (times['unixTime'] - lastSmokeTimestamp >= settings.cropEveryNMinutes * 60):
            lastSmokeTimestamp = times['unixTime']
            result = crop_single.imageDisplay(newPath, temporaryDir.name, showSquaresArg=False)
            if len(result) > 0:
                for entry in result:
                    print('crop data', entry['name'], entry['coords'])
                    uploadToDrive(googleServices['drive'], entry['name'], None, 'cropSmoke')
                    appendToCropSheet(googleServices['sheet'], entry['name'], entry['coords'], newPath)

    imageFileNames = os.listdir(imgDirectory)
    print('images2', imageFileNames)
    shutil.rmtree(temporaryDir.name)


def main():
    reqArgs = [
        ["f", "fire", "ID of the fire in the images"],
        ["c", "camera", "ID of the camera used in the images"],
    ]
    optArgs = [
        ["z", "zipFile", "Name of the zip file containing the images"],
        ["d", "imgDirectory", "Name of the directory containing the images or ask:dir"],
    ]
    args = collect_args.collectArgs(reqArgs,  optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    imgDirectory = None
    if args.imgDirectory:
        imgDirectory = args.imgDirectory
        if imgDirectory == 'ask:dir':
            imgDirectory = askdirectory()
    elif args.zipFile:
        tempDir = unzipFile(args.zipFile)
        imgDirectory = tempDir.name

    if not imgDirectory:
        logging.error('Must specify either zipFile or imgDirectory')
        exit(1)

    googleServices = goog_helper.getGoogleServices(settings, args)
    processFolder(imgDirectory, args.camera, args.fire, googleServices)


if __name__=="__main__":
    main()
