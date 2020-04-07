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

Reads data from csv export of Fuego Cropped Images sheet to find the original
entire image name and the manually selected rectangular bounding box. Then
downloads the entire image and recrops it by increasing size of the rectangle
by given growRatio and to exceed the specified minimums.  Also, very large
images are discarded (controlled by throwSize)

Optionally, for debuggins shows the boxes on screen (TODO: refactor display code)

"""

"""
#+##REPLACE DEP. GDRIVE W HPREWN#/#-##REPLACE DEP. GDRIVE W HPREWN# 
tags to mark changes to be implemented to ween off the dependency of sort_images.py to upload full photos to Gdrive.
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

def imageDisplay(imgOrig, title=''):
    rootTk = tk.Tk()
    rootTk.title('Fuego: ' + title)
    screen_width = rootTk.winfo_screenwidth() - 100
    screen_height = rootTk.winfo_screenheight() - 100

    print("Image:", (imgOrig.size[0], imgOrig.size[1]), ", Screen:", (screen_width, screen_height))
    scaleX = min(screen_width/imgOrig.size[0], 1)
    scaleY = min(screen_height/imgOrig.size[1], 1)
    scaleFactor = min(scaleX, scaleY)
    print('scale', scaleFactor, scaleX, scaleY)
    scaledImg = imgOrig
    if (scaleFactor != 1):
        scaledImg = imgOrig.resize((int(imgOrig.size[0]*scaleFactor), int(imgOrig.size[1]*scaleFactor)), Image.ANTIALIAS)
    imgPhoto = ImageTk.PhotoImage(scaledImg)
    canvasTk = tk.Canvas(rootTk, width=imgPhoto.width(), height=imgPhoto.height(), bg="light yellow")
    canvasTk.config(highlightthickness=0)

    aff=canvasTk.create_image(0, 0, anchor='nw', image=imgPhoto)
    canvasTk.focus_set()
    canvasTk.pack(side='left', expand='yes', fill='both')

    return (rootTk, canvasTk, imgPhoto, scaleFactor)


def buttonClick(event):
    exit()

# use multiple colors to make it slightly easier to see the overlapping boxes
colors = ['red', 'blue']

def displayImageWithScores(imgOrig, segments):
    (rootTk, canvasTk, imgPhoto, scaleFactor) = imageDisplay(imgOrig)
    canvasTk.bind("<Button-1>", buttonClick)
    canvasTk.bind("<Button-2>", buttonClick)
    canvasTk.bind("<Button-3> ", buttonClick)
    for counter, coords in enumerate(segments):
        (sx0, sy0, sx1, sy1) = coords
        offset = ((counter%2) - 0.5)*2
        x0 = sx0*scaleFactor + offset
        y0 = sy0*scaleFactor + offset
        x1 = sx1*scaleFactor + offset
        y1 = sy1*scaleFactor + offset
        color = colors[counter % len(colors)]
        canvasTk.create_rectangle(x0, y0, x1, y1, outline=color, width=2)
    rootTk.mainloop()


def getCameraDir(service, cameraCache, fileName):
    parsed = img_archive.parseFilename(fileName)
    cameraID = parsed['cameraID']
    dirID = cameraCache.get(cameraID)
    if not dirID:
        (dirID, dirName) = goog_helper.getDirForClassCamera(service, settings.IMG_CLASSES, 'smoke', cameraID)
        cameraCache[cameraID] = dirID
    return dirID


def expandMinAndMax(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
    """Expand the image dimension range

    Inceases the min/max of the range by growRatio while maintaining
    the same center. Also, ensures the range is at least minimumDiff.
    Finally, limits the increases to ensure values are still within
    the entire range of the image (minLimit, maxLimit)

    Args:
        val0 (int): starting (minimum) value of the input range
        val1 (int): ending (maximum) value of the input range
        minimumDiff (int): mimimum size of the output range
        growRatio (float): ratio (expected > 1) to expand the range by
        minLimit (int): absolute minimum value of the output range
        maxLimit (int): absolute maximum value of the output range

    Returns:
        (int, int): start, end of the adjusted range
    """
    val0 = max(val0, minLimit)
    val1 = min(val1, maxLimit)
    diff = val1 - val0
    center = val0 + int(diff/2)
    minimumDiff = max(minimumDiff, int(diff*growRatio))
    if diff < minimumDiff:
        if (center - int(minimumDiff/2)) < minLimit:   # left edge limited
            val0 = minLimit
            val1 = min(val0 + minimumDiff, maxLimit)
        elif (center + int(minimumDiff/2)) > maxLimit: # right edge limited
            val1 = maxLimit
            val0 = max(val1 - minimumDiff, minLimit)
        else:                                          # unlimited
            val0 = center - int(minimumDiff/2)
            val1 = min(val0 + minimumDiff, maxLimit)
    return (val0, val1)


def expandMax(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
    val0 = max(val0, minLimit)
    val1 = min(val1, maxLimit)
    diff = val1 - val0
    minimumDiff = max(minimumDiff, int(diff*growRatio))
    if diff < minimumDiff:
        if val0 + minimumDiff < maxLimit:
            minVal = val0
            maxVal = val0 + minimumDiff
        else:
            maxVal = maxLimit
            minVal = max(maxVal - minimumDiff, minLimit)
    else:
        minVal = val0
        maxVal = val1
    return (minVal, maxVal)


def expandMax75(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
    val0 = max(val0, minLimit)
    val1 = min(val1, maxLimit)
    diff = val1 - val0
    minimumDiff = max(minimumDiff, int(diff*growRatio))
    if (diff < minimumDiff/2):
        center = val0 + int(diff/2)
        minVal = max(center - int(minimumDiff/4), minLimit)
        maxVal = min(center + int(minimumDiff/4), maxLimit)
        return expandMax(minVal, maxVal, minimumDiff, growRatio, minLimit, maxLimit)
    else:
        return expandMax(val0, val1, minimumDiff, growRatio, minLimit, maxLimit)


def expandMin(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
    val0 = max(val0, minLimit)
    val1 = min(val1, maxLimit)
    diff = val1 - val0
    minimumDiff = max(minimumDiff, int(diff*growRatio))
    if diff < minimumDiff:
        if val1 - minimumDiff >= minLimit:
            maxVal = val1
            minVal = maxVal - minimumDiff
        else:
            minVal = minLimit
            maxVal = min(minVal + minimumDiff, maxLimit)
    else:
        minVal = val0
        maxVal = val1
    return (minVal, maxVal)


def expandMin75(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
    val0 = max(val0, minLimit)
    val1 = min(val1, maxLimit)
    diff = val1 - val0
    minimumDiff = max(minimumDiff, int(diff*growRatio))
    if (diff < minimumDiff/2):
        center = val0 + int(diff/2)
        minVal = max(center - int(minimumDiff/4), minLimit)
        maxVal = min(center + int(minimumDiff/4), maxLimit)
        return expandMin(minVal, maxVal, minimumDiff, growRatio, minLimit, maxLimit)
    else:
        return expandMin(val0, val1, minimumDiff, growRatio, minLimit, maxLimit)


def appendIfDifferent(array, newItem):
    hasAlready = list(filter(lambda x: x==newItem, array))
    if not hasAlready:
        array.append(newItem)


def getCropCoords(smokeCoords, minDiffX, minDiffY, growRatio, imgSize):
    cropCoords = []
    (minX, minY, maxX, maxY) = smokeCoords
    (imgSizeX, imgSizeY) = imgSize
    #centered box
    (newMinX, newMaxX) = expandMinAndMax(minX, maxX, minDiffX, growRatio, 0, imgSizeX)
    (newMinY, newMaxY) = expandMinAndMax(minY, maxY, minDiffY, growRatio, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #top left box
    (newMinX, newMaxX) = expandMax75(minX, maxX, minDiffX, growRatio, 0, imgSizeX)
    (newMinY, newMaxY) = expandMax75(minY, maxY, minDiffY, growRatio, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #top right box
    (newMinX, newMaxX) = expandMax75(minX, maxX, minDiffX, growRatio, 0, imgSizeX)
    (newMinY, newMaxY) = expandMin75(minY, maxY, minDiffY, growRatio, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #bottom left box
    (newMinX, newMaxX) = expandMin75(minX, maxX, minDiffX, growRatio, 0, imgSizeX)
    (newMinY, newMaxY) = expandMax75(minY, maxY, minDiffY, growRatio, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #bottom right box
    (newMinX, newMaxX) = expandMin75(minX, maxX, minDiffX, growRatio, 0, imgSizeX)
    (newMinY, newMaxY) = expandMin75(minY, maxY, minDiffY, growRatio, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    return cropCoords


def main():
    reqArgs = [
        ["o", "outputDir", "local directory to save images segments"],
        ["i", "inputCsv", "csvfile with contents of Fuego Cropped Images"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
        ["d", "display", "(optional) specify any value to display image and boxes"],
        ["x", "minDiffX", "(optional) override default minDiffX of 299"],
        ["y", "minDiffY", "(optional) override default minDiffY of 299"],
        ["a", "minArea", "(optional) override default throw away areas < 1/100 of 299x299"],
        ["t", "throwSize", "(optional) override default throw away size of 1000x1000"],
        ["g", "growRatio", "(optional) override default grow ratio of 1.2"],
        ["m", "minusMinutes", "(optional) subtract images from given number of minutes ago"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    minDiffX = int(args.minDiffX) if args.minDiffX else 299
    minDiffY = int(args.minDiffY) if args.minDiffY else 299
    throwSize = int(args.throwSize) if args.throwSize else 1000
    growRatio = float(args.growRatio) if args.growRatio else 1.2
    minArea = int(args.minArea) if args.minArea else int(299*2.99)
    minusMinutes = int(args.minusMinutes) if args.minusMinutes else 0

    googleServices = goog_helper.getGoogleServices(settings, args)
    camArchives = img_archive.getHpwrenCameraArchives(googleServices['sheet'], settings)
    if minusMinutes:
        timeGapDelta = datetime.timedelta(seconds = 60*minusMinutes)
    cameraCache = {}
    skippedTiny = []
    skippedHuge = []
    skippedArchive = []
    with open(args.inputCsv) as csvFile:
        csvreader = csv.reader(csvFile)
        for (rowIndex, csvRow) in enumerate(csvreader):
            if rowIndex < startRow:
                continue
            if rowIndex > endRow:
                print('Reached end row', rowIndex, endRow)
                break
            [cropName, minX, minY, maxX, maxY, fileName] = csvRow[:6]
            minX = int(minX)
            minY = int(minY)
            maxX = int(maxX)
            maxY = int(maxY)
            oldCoords = (minX, minY, maxX, maxY)
            if ((maxX - minX) > throwSize) and ((maxY - minY) > throwSize):
                logging.warning('Skip large image: dx=%d, dy=%d, name=%s', maxX - minX, maxY - minY, fileName)
                skippedHuge.append((rowIndex, fileName, maxX - minX, maxY - minY))
                continue
            if ((maxX - minX) * (maxY - minY)) < minArea:
                logging.warning('Skipping tiny image with area: %d, name=%s', (maxX - minX) * (maxY - minY), fileName)
                skippedTiny.append((rowIndex, fileName, (maxX - minX) * (maxY - minY)))
                continue
            # get base image from google drive that was uploaded by sort_images.py

            dirID = getCameraDir(googleServices['drive'], cameraCache, fileName)#-##REPLACE DEP. GDRIVE W HPREWN#
            localFilePath = os.path.join(settings.downloadDir, fileName)#sets a path for that image() not yet downloadedby this iteration
            print('local', localFilePath)
            if not os.path.isfile(localFilePath):# if file has not been downloaded by a previous iteration
                print('download', fileName)
                #+##REPLACE DEP. GDRIVE W HPREWN#nameParsed = img_archive.parseFilename(fileName)#parses file name into dictionary of parts name,unixtime,etc.
                #+##REPLACE DEP. GDRIVE W HPREWN#matchingCams = list(filter(lambda x: nameParsed['cameraID'] == x['id'], camArchives))#filter through camArchives for ids matching cameraid
                #+##REPLACE DEP. GDRIVE W HPREWN#if len(matchingCams) != 1:#if we cannot determine where the image will come from we cannot use the image
                #+##REPLACE DEP. GDRIVE W HPREWN#    logging.warning('Skipping camera without archive: %d, %s', len(matchingCams), str(matchingCams))
                #+##REPLACE DEP. GDRIVE W HPREWN#    skippedArchive.append((rowIndex, fileName, matchingCams))
                #+##REPLACE DEP. GDRIVE W HPREWN#    continue
                #+##REPLACE DEP. GDRIVE W HPREWN#archiveDirs = matchingCams[0]['dirs']
                #+##REPLACE DEP. GDRIVE W HPREWN#logging.warning('Found %s directories', archiveDirs)
                #+##REPLACE DEP. GDRIVE W HPREWN#time = datetime.datetime.fromtimestamp(nameParsed['unixTime'])
                #+##REPLACE DEP. GDRIVE W HPREWN#for dirName in archiveDirs:#search directories of camera for a time near
                #+##REPLACE DEP. GDRIVE W HPREWN#    logging.warning('Searching for files in dir %s', dirName)
                #+##REPLACE DEP. GDRIVE W HPREWN#    imgPaths = img_archive.downloadFilesHpwren(settings.downloadDir, nameParsed['cameraID'], dirName, time, time, 1, 0)
                #+##REPLACE DEP. GDRIVE W HPREWN#    if imgPaths:
                #+##REPLACE DEP. GDRIVE W HPREWN#        localFilePath = imgPaths[0]
                #+##REPLACE DEP. GDRIVE W HPREWN#        break
                #+##REPLACE DEP. GDRIVE W HPREWN#if not imgPaths:
                #+##REPLACE DEP. GDRIVE W HPREWN#    logging.warning('Skipping image not found: %s', fileName)
                #+##REPLACE DEP. GDRIVE W HPREWN#    skippedArchive.append((rowIndex, fileName, time))#archive that images were skipped
                #+##REPLACE DEP. GDRIVE W HPREWN#    continue
                goog_helper.downloadFile(googleServices['drive'], dirID, fileName, localFilePath)#-##REPLACE DEP. GDRIVE W HPREWN#
            imgOrig = Image.open(localFilePath)#opens image

            # if in subracted images mode, download an earlier image and subtract
            if minusMinutes:
                nameParsed = img_archive.parseFilename(fileName)#parses file name into dictionary of parts name,unixtime,etc.
                dt = datetime.datetime.fromtimestamp(nameParsed['unixTime'])
                dt -= timeGapDelta
                earlierImgPath = None
                files = img_archive.getHpwrenImages(googleServices, settings, settings.downloadDir, camArchives, nameParsed['cameraID'], dt, dt, 1)
                if files:
                    earlierImgPath = files[0]
                else:
                    logging.warning('Skipping image without prior image: %s, %s', str(dt), fileName)
                    skippedArchive.append((rowIndex, fileName, dt))
                    continue
                logging.warning('Subtracting old image %s', earlierImgPath)
                earlierImg = Image.open(earlierImgPath)
                diffImg = img_archive.diffImages(imgOrig, earlierImg)
                # realImgOrig = imgOrig # is this useful?
                imgOrig = diffImg
                fileNameParts = os.path.splitext(fileName)
                fileName = str(fileNameParts[0]) + ('_Diff%d' % minusMinutes) + fileNameParts[1]

            # crop the full sized image to show just the smoke, but shifted and flipped
            # shifts and flips increase number of segments for training and also prevent overfitting by perturbing data
            cropCoords = getCropCoords((minX, minY, maxX, maxY), minDiffX, minDiffY, growRatio, (imgOrig.size[0], imgOrig.size[1]))
            for newCoords in cropCoords:
                # XXXX - save work if old=new?
                print('coords old,new', oldCoords, newCoords)
                imgNameNoExt = str(os.path.splitext(fileName)[0])
                cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '.jpg'
                cropImgPath = os.path.join(args.outputDir, cropImgName)
                cropped_img = imgOrig.crop(newCoords)
                cropped_img.save(cropImgPath, format='JPEG')
                flipped_img = cropped_img.transpose(Image.FLIP_LEFT_RIGHT)
                flipImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '_Flip.jpg'
                flipImgPath = os.path.join(args.outputDir, flipImgName)
                flipped_img.save(flipImgPath, format='JPEG')
            print('Processed row: %s, file: %s' % (rowIndex, fileName))
            if args.display:
                displayCoords = [oldCoords] + cropCoords
                displayImageWithScores(imgOrig, displayCoords)
                imageDisplay(imgOrig)
    logging.warning('Skipped tiny images %d, %s', len(skippedTiny), str(skippedTiny))
    logging.warning('Skipped huge images %d, %s', len(skippedHuge), str(skippedHuge))
    logging.warning('Skipped images without archives %d, %s', len(skippedArchive), str(skippedArchive))

if __name__=="__main__":
    main()
