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

Processes all the nonSmoke images to find earlier images to subtract and generate
diff images for training diff model

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.lib import rect_to_squares

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
        ["f", "fullImages", "(optional) process full images vs cropped"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    minusMinutes = int(args.minusMinutes)
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    fullImages = True if args.fullImages else False

    googleServices = goog_helper.getGoogleServices(settings, args)
    camArchives = img_archive.getHpwrenCameraArchives(settings.hpwrenArchives)
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

        if (fileName[:3] == 'v2_') or (fileName[:3] == 'v3_') or (not 'mobo-c' in fileName):
            continue # skip replicated files
        logging.warning('Processing row %d, file: %s', rowIndex, fileName)
        parsedName = img_archive.parseFilename(fileName)

        if (not parsedName) or parsedName['diffMinutes'] or (('minX' not in parsedName) and not fullImages):
            logging.warning('Skipping file with unexpected parsed data: %s, %s', fileName, str(parsedName))
            skippedBadParse.append((rowIndex, fileName, parsedName))
            continue # skip files without crop info or with diff
        parsedName['unixTime'] -= 60*minusMinutes
        earlierName = img_archive.repackFileName(parsedName)
        earlierImgPath = os.path.join(settings.downloadDir, earlierName)
        if not os.path.isfile(earlierImgPath):# if file has not been downloaded by a previous iteration
            dt = datetime.datetime.fromtimestamp(parsedName['unixTime'])
            dt -= timeGapDelta
            files = img_archive.getHpwrenImages(googleServices, settings, settings.downloadDir, camArchives, parsedName['cameraID'], dt, dt, 1)
            if files:
                earlierImgPath = files[0]
            else:
                logging.warning('Skipping image without prior image: %s, %s', str(dt), fileName)
                skippedArchive.append((rowIndex, fileName, dt))
                continue
        logging.warning('Subtracting old image %s', earlierImgPath)
        imgOrig = Image.open(os.path.join(args.inputDir, fileName))
        earlierImg = Image.open(earlierImgPath)
        if fullImages:
            diffImg = img_archive.diffImages(imgOrig, earlierImg)
        else:
            croppedEarlyImg = earlierImg.crop((parsedName['minX'], parsedName['minY'], parsedName['maxX'], parsedName['maxY']))
            diffImg = img_archive.diffImages(imgOrig, croppedEarlyImg)
        extremas = diffImg.getextrema()
        if extremas[0][0] == 128 or extremas[0][1] == 128 or extremas[1][0] == 128 or extremas[1][1] == 128 or extremas[2][0] == 128 or extremas[2][1] == 128:
            logging.warning('Skipping no diffs %s, name=%s', str(extremas), fileName)
            skippedBadParse.append((rowIndex, fileName, extremas))
            continue
        parsedName['diffMinutes'] = minusMinutes
        diffImgPath = os.path.join(args.outputDir, img_archive.repackFileName(parsedName))
        logging.warning('Saving new image %s', diffImgPath)
        diffImg.save(diffImgPath, format='JPEG')
    logging.warning('Skipped bad parse %d, %s', len(skippedBadParse), str(skippedBadParse))
    logging.warning('Skipped images without archives %d, %s', len(skippedArchive), str(skippedArchive))

if __name__=="__main__":
    main()
