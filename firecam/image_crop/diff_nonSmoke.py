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

Processes all the nonSmoke images to find earlier images to subtract and generate
diff images for training diff model

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper
import rect_to_squares
import img_archive

import datetime
import logging
import csv
import tkinter as tk
from PIL import Image, ImageTk



def main():
    reqArgs = [
        ["o", "outputDir", "local directory to save diff image segments"],
        ["i", "inputDir", "input local directory containing nonSmoke image segments"],
        ["m", "minusMinutes", "subtract images from given number of minutes ago"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    minusMinutes = int(args.minusMinutes)
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9

    googleServices = goog_helper.getGoogleServices(settings, args)
    cookieJar = None
    camArchives = None
    cookieJar = img_archive.loginAjax()
    camArchives = img_archive.getHpwrenCameraArchives(googleServices['sheet'], settings)
    timeGapDelta = datetime.timedelta(seconds = 60*minusMinutes)
    skippedBadParse = []
    skippedArchive = []
    imageFileNames = sorted(os.listdir(args.inputDir))
    rowIndex = -1
    for fileName in imageFileNames:
        rowIndex += 1

        if rowIndex < startRow:
            continue
        if rowIndex > endRow:
            print('Reached end row', rowIndex, endRow)
            break

        if (fileName[:3] == 'v2_') or (fileName[:3] == 'v3_'):
            continue # skip replicated files
        logging.warning('Processing row %d, file: %s', rowIndex, fileName)
        parsedName = img_archive.parseFilename(fileName)

        if (not parsedName) or parsedName['diffMinutes'] or ('minX' not in parsedName):
            logging.warning('Skipping file with unexpected parsed data: %s, %s', fileName, str(parsedName))
            skippedBadParse.append((rowIndex, fileName, parsedName))
            continue # skip files without crop info or with diff
        matchingCams = list(filter(lambda x: parsedName['cameraID'] == x['id'], camArchives))
        if len(matchingCams) != 1:
            logging.warning('Skipping camera without archive: %d, %s', len(matchingCams), str(matchingCams))
            skippedArchive.append((rowIndex, fileName, matchingCams))
            continue
        archiveDirs = matchingCams[0]['dirs']
        logging.warning('Found %s directories', archiveDirs)
        earlierImgPath = None
        dt = datetime.datetime.fromtimestamp(parsedName['unixTime'])
        dt -= timeGapDelta
        for dirName in archiveDirs:
            logging.warning('Searching for files in dir %s', dirName)
            imgPaths = img_archive.getFilesAjax(cookieJar, settings.downloadDir, parsedName['cameraID'], dirName, dt, dt, 1)
            if imgPaths:
                earlierImgPath = imgPaths[0]
                break # done
        if not earlierImgPath:
            logging.warning('Skipping image without prior image: %s, %s', str(dt), fileName)
            skippedArchive.append((rowIndex, fileName, dt))
            continue
        logging.warning('Subtracting old image %s', earlierImgPath)
        earlierImg = Image.open(earlierImgPath)
        print('CR', (parsedName['minX'], parsedName['minY'], parsedName['maxX'], parsedName['maxY']))
        croppedEarlyImg = earlierImg.crop((parsedName['minX'], parsedName['minY'], parsedName['maxX'], parsedName['maxY']))

        imgOrig = Image.open(os.path.join(args.inputDir, fileName))
        diffImg = img_archive.diffImages(imgOrig, croppedEarlyImg)
        parsedName['diffMinutes'] = minusMinutes
        diffImgPath = os.path.join(args.outputDir, img_archive.repackFileName(parsedName))
        logging.warning('Saving new image %s', diffImgPath)
        diffImg.save(diffImgPath, format='JPEG')
    logging.warning('Skipped bad parse %d, %s', len(skippedBadParse), str(skippedBadParse))
    logging.warning('Skipped images without archives %d, %s', len(skippedArchive), str(skippedArchive))

if __name__=="__main__":
    main()
