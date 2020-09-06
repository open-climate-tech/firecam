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

Reads data from csv export of Cropped Images sheet to find the original
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



import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.lib import rect_to_squares

import random
import datetime
import logging
import csv
import tkinter as tk
from PIL import Image, ImageTk

def imageDisplay(imgOrig, title=''):
    rootTk = tk.Tk()
    rootTk.title('Firecam: ' + title)
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


def randomInRange(borders, avoidCenter, size):
    """Return (x,y) random pair in range

    x,y values are randomly chosen between [0, size/2 - border] such that (x,y) represent
    a vector from the cneter (sizeX/2, sizeY/2) that still avoids the borders.
    Second constraint is that if (x,y) randomly fall inside avoidCenter rectangular region,
    then they set both set to max values (sizeX/2 - borderX, sizeY/2 - borderY)
    """
    (borderX, borderY) = borders
    (avoidCenterX, avoidCenterY) = avoidCenter
    (sizeX, sizeY) = size
    randX = int(random.random() * (sizeX/2 - borderX))
    # print('rx', randX, int(sizeX/2 - borderX))
    randY = int(random.random() * (sizeY/2 - borderY))
    # print('ry', randY, int(sizeY/2 - borderY))
    if (randX < avoidCenterX) and (randY < avoidCenterY):
        randX = int(sizeX/2 - borderX)
        randY = int(sizeY/2 - borderY)
        # print('rxy borders', randX, randY)
    return (randX, randY)


def appendIfDifferent(array, newItem):
    # print('aid', newItem)
    hasAlready = list(filter(lambda x: x==newItem, array))
    if not hasAlready:
        array.append(newItem)


def getCropCoords(smokeCoords, minSizeX, minSizeY, growRatio, imgSize, recropType):
    cropCoords = []
    (minX, minY, maxX, maxY) = smokeCoords
    (imgSizeX, imgSizeY) = imgSize
    # ensure bounds are within image size (no negatives or greater than image size)
    minX = max(minX, 0)
    minY = max(minY, 0)
    maxX = min(maxX, imgSizeX)
    maxY = min(maxY, imgSizeY)

    centerX = int((minX + maxX) / 2)
    centerY = int((minY + maxY) / 2)
    origSizeX = maxX - minX
    origSizeY = maxY - minY
    if recropType == 'shift':
        offsetX = int(random.random()*100) - 50
        offsetY = int(random.random()*100) - 50
        (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX + offsetX, origSizeX, 0, imgSizeX)
        (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY + offsetY, origSizeY, 0, imgSizeY)
        appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
        return cropCoords

    sizeX = max(minSizeX, int(origSizeX*growRatio))
    sizeY = max(minSizeY, int(origSizeY*growRatio))
    #centered box
    (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX, sizeX, 0, imgSizeX)
    (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY, sizeY, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    if recropType == 'center':
        return cropCoords


    borderX = int(origSizeX / 2)
    borderY = int(origSizeY / 2)
    avoidCenterX = int(origSizeX / 4)
    avoidCenterY = int(origSizeY / 4)

    #top left box
    (randX, randY) = randomInRange((borderX, borderY), (avoidCenterX, avoidCenterY), (sizeX, sizeY))
    (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX - randX, sizeX, 0, imgSizeX)
    (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY - randY, sizeY, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #top right box
    (randX, randY) = randomInRange((borderX, borderY), (avoidCenterX, avoidCenterY), (sizeX, sizeY))
    (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX + randX, sizeX, 0, imgSizeX)
    (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY - randY, sizeY, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #bottom left box
    (randX, randY) = randomInRange((borderX, borderY), (avoidCenterX, avoidCenterY), (sizeX, sizeY))
    (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX - randX, sizeX, 0, imgSizeX)
    (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY + randY, sizeY, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    #bottom right box
    (randX, randY) = randomInRange((borderX, borderY), (avoidCenterX, avoidCenterY), (sizeX, sizeY))
    (newMinX, newMaxX) = rect_to_squares.getRangeFromCenter(centerX + randX, sizeX, 0, imgSizeX)
    (newMinY, newMaxY) = rect_to_squares.getRangeFromCenter(centerY + randY, sizeY, 0, imgSizeY)
    appendIfDifferent(cropCoords, (newMinX, newMinY, newMaxX, newMaxY))
    return cropCoords


def main():
    reqArgs = [
        ["o", "outputDir", "local directory to save images segments"],
        ["i", "inputCsv", "csvfile with contents of Cropped Images"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
        ["d", "display", "(optional) specify any value to display image and boxes"],
        ["x", "minSizeX", "(optional) override default minSizeX of 299"],
        ["y", "minSizeY", "(optional) override default minSizeY of 299"],
        ["a", "minArea", "(optional) override default 0 for minimum area"],
        ["t", "throwSize", "(optional) override default throw away size of 598x598"],
        ["g", "growRatio", "(optional) override default grow ratio of 1.2"],
        ["m", "minusMinutes", "(optional) subtract images from given number of minutes ago"],
        ["r", "recropType", "recrop type: 'raw', 'center', 'shift', 'augment' (default)"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    minSizeX = int(args.minSizeX) if args.minSizeX else 299
    minSizeY = int(args.minSizeY) if args.minSizeY else 299
    throwSize = int(args.throwSize) if args.throwSize else 299*2
    growRatio = float(args.growRatio) if args.growRatio else 1.2
    minArea = int(args.minArea) if args.minArea else 0
    minusMinutes = int(args.minusMinutes) if args.minusMinutes else 0
    recropType = args.recropType if args.recropType else 'augment'

    random.seed(0)
    googleServices = goog_helper.getGoogleServices(settings, args)
    camArchives = img_archive.getHpwrenCameraArchives(settings.hpwrenArchives)
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
            [_unused_cropName, minX, minY, maxX, maxY, fileName] = csvRow[:6]
            minX = int(minX)
            minY = int(minY)
            maxX = int(maxX)
            maxY = int(maxY)
            oldCoords = (minX, minY, maxX, maxY)
            if ((maxX - minX) > throwSize) or ((maxY - minY) > throwSize):
                logging.warning('Skip large image: dx=%d, dy=%d, name=%s', maxX - minX, maxY - minY, fileName)
                skippedHuge.append((rowIndex, fileName, maxX - minX, maxY - minY))
                continue
            if ((maxX - minX) * (maxY - minY)) < minArea:
                logging.warning('Skipping tiny image with area: %d, name=%s', (maxX - minX) * (maxY - minY), fileName)
                skippedTiny.append((rowIndex, fileName, (maxX - minX) * (maxY - minY)))
                continue

            nameParsed = img_archive.parseFilename(fileName)
            imgDT = datetime.datetime.fromtimestamp(nameParsed['unixTime'])
            localFilePath = os.path.join(settings.downloadDir, fileName)
            if not os.path.isfile(localFilePath):# if file has not been downloaded by a previous iteration
                files = img_archive.getHpwrenImages(googleServices, settings, settings.downloadDir, camArchives, nameParsed['cameraID'], imgDT, imgDT, 1)
                localFilePath = files[0]
            imgOrig = Image.open(localFilePath)

            # if in subracted images mode, download an earlier image and subtract
            if minusMinutes:
                dt = imgDT - timeGapDelta
                nameParsed['unixTime'] -= 60*minusMinutes
                earlierName = img_archive.repackFileName(nameParsed)
                earlierImgPath = os.path.join(settings.downloadDir, earlierName)
                if not os.path.isfile(earlierImgPath):# if file has not been downloaded by a previous iteration
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
                extremas = diffImg.getextrema()
                if extremas[0][0] == 128 or extremas[0][1] == 128 or extremas[1][0] == 128 or extremas[1][1] == 128 or extremas[2][0] == 128 or extremas[2][1] == 128:
                    logging.warning('Skipping no diffs %s, name=%s', str(extremas), fileName)
                    skippedTiny.append((rowIndex, fileName, extremas))
                    continue
                # realImgOrig = imgOrig # is this useful?
                imgOrig = diffImg
                fileNameParts = os.path.splitext(fileName)
                fileName = str(fileNameParts[0]) + ('_Diff%d' % minusMinutes) + fileNameParts[1]

            if args.recropType == 'raw':
                cropCoords = [oldCoords]
            else:
                # crop the full sized image to show just the smoke, but shifted and flipped
                # shifts and flips increase number of segments for training and also prevent overfitting by perturbing data
                cropCoords = getCropCoords((minX, minY, maxX, maxY), minSizeX, minSizeY, growRatio, (imgOrig.size[0], imgOrig.size[1]), args.recropType)
            for newCoords in cropCoords:
                # XXXX - save work if old=new?
                logging.warning('coords old %s, new %s', str(oldCoords), str(newCoords))
                imgNameNoExt = str(os.path.splitext(fileName)[0])
                cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '.jpg'
                cropImgPath = os.path.join(args.outputDir, cropImgName)
                cropped_img = imgOrig.crop(newCoords)
                cropped_img.save(cropImgPath, format='JPEG')
                if args.recropType == 'augment':
                    flipped_img = cropped_img.transpose(Image.FLIP_LEFT_RIGHT)
                    flipImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '_Flip.jpg'
                    flipImgPath = os.path.join(args.outputDir, flipImgName)
                    flipped_img.save(flipImgPath, format='JPEG')
            logging.warning('Processed row: %d, file: %s', rowIndex, fileName)
            if args.display:
                displayCoords = [oldCoords] + cropCoords
                displayImageWithScores(imgOrig, displayCoords)
                imageDisplay(imgOrig)
    logging.warning('Skipped tiny images %d, %s', len(skippedTiny), str(skippedTiny))
    logging.warning('Skipped huge images %d, %s', len(skippedHuge), str(skippedHuge))
    logging.warning('Skipped images without archives %d, %s', len(skippedArchive), str(skippedArchive))

if __name__=="__main__":
    main()
