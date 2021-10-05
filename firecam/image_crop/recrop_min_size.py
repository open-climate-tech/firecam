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
from PIL import Image, ImageTk, ImageStat

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
        maxShift = 100
        offsetX = int(random.random()*maxShift) - maxShift/2
        offsetY = int(random.random()*maxShift) - maxShift/2
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


def isImageValid(img):
    try:
        img.load()
        return True
    except Exception as e:
        logging.warning('Error partial image. %s', str(e))
    return False


def getArchiveImage(googleServices, downloadDirCache, camArchives, cameraID, expectedFileName, imgDT):
    localFilePath = os.path.join(settings.downloadDir, expectedFileName)
    if not os.path.isfile(localFilePath):# if file has not been downloaded by a previous iteration
        files = img_archive.getHpwrenImages(googleServices, settings, downloadDirCache, camArchives, cameraID, imgDT, imgDT, 1)
        if not files or len(files) == 0:
            return (None, None)
        localFilePath = files[0]
    img = Image.open(localFilePath)
    if not isImageValid(img):  # retry download if image invalid
        os.remove(localFilePath)
        files = img_archive.getHpwrenImages(googleServices, settings, downloadDirCache, camArchives, cameraID, imgDT, imgDT, 1)
        localFilePath = files[0]
        img = Image.open(localFilePath)
    return (img, localFilePath)


def findAlignedImage(baseImgFilePath, filePaths):
    for filePath in filePaths:
        img = img_archive.alignImageObj(filePath, baseImgFilePath)
        if img:
            return img
    return None


def brightness(img):
    medians = ImageStat.Stat(img).median
    brightness = (medians[0] + medians[1] + medians[2]) / 3
    return max(brightness, .01) # to avoid div by 0


def main():
    reqArgs = [
        ["o", "outputDir", "local directory to save images segments"],
        ["i", "inputCsv", "csvfile with contents of Cropped Images"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
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
    downloadDirCache = img_archive.cacheDir(settings.downloadDir, settings.downloadDir)

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
            (imgOrig, imgFilePath) = getArchiveImage(googleServices, downloadDirCache, camArchives, nameParsed['cameraID'], fileName, imgDT)
            if not imgOrig:
                logging.warning('Skip image without archive: %s', fileName)
                skippedArchive.append((rowIndex, fileName, imgDT))
                continue

            # find coordinates for cropping
            if recropType == 'raw':
                cropCoords = [oldCoords]
            else:
                # crop the full sized image to show just the smoke, but shifted and flipped
                # shifts and flips increase number of segments for training and also prevent overfitting by perturbing data
                cropCoords = getCropCoords((minX, minY, maxX, maxY), minSizeX, minSizeY, growRatio, (imgOrig.size[0], imgOrig.size[1]), recropType)
            # find extrema (min/max) crop coordinates to crop the original image to speed up processing
            extremaCoords = list(cropCoords[0])
            for coords in cropCoords:
                extremaCoords[0] = min(extremaCoords[0], coords[0])
                extremaCoords[1] = min(extremaCoords[1], coords[1])
                extremaCoords[2] = max(extremaCoords[2], coords[2])
                extremaCoords[3] = max(extremaCoords[3], coords[3])
            imgOrig = imgOrig.crop(extremaCoords)

            # if in subracted images mode, download an earlier image and subtract
            if minusMinutes:
                if not img_archive.findCameraInArchive(camArchives, nameParsed['cameraID']):
                    earlierImg = None
                    files = img_archive.cacheFetchRange(downloadDirCache, nameParsed['cameraID'], nameParsed['unixTime'], -minusMinutes*60, -10*minusMinutes*60)
                    if files:
                        earlierImg = findAlignedImage(imgFilePath, files)
                    if not files or not earlierImg:
                        logging.warning('Skipping image without prior image: %s', fileName)
                        skippedArchive.append((rowIndex, fileName, None))
                        continue
                else:
                    nameParsed['unixTime'] -= 60*minusMinutes
                    earlierName = img_archive.repackFileName(nameParsed)
                    dt = imgDT - timeGapDelta
                    (earlierImg, _) = getArchiveImage(googleServices, downloadDirCache, camArchives, nameParsed['cameraID'], earlierName, dt)
                    if not earlierImg:
                        logging.warning('Skipping image without prior image: %s, %s', str(dt), fileName)
                        skippedArchive.append((rowIndex, fileName, dt))
                        continue
                    logging.warning('Subtracting old image %s', earlierName)

                earlierImg = earlierImg.crop(extremaCoords)
                brightnessRatio = brightness(imgOrig)/brightness(earlierImg)
                logging.warning('brightness ratio %s, %s, %s', round(brightnessRatio, 3), rowIndex, fileName)
                if (brightnessRatio < 0.92) or (brightnessRatio > 1.08): # large diffs hide the smoke
                    logging.warning('Skipping extreme brigthness diff %s, name=%s', brightnessRatio, fileName)
                    skippedTiny.append((rowIndex, fileName, brightnessRatio))
                    continue
                diffImg = img_archive.diffSmoothImages(imgOrig, earlierImg)
                extremas = diffImg.getextrema()
                if (extremas[0][0] == 128 and extremas[0][1] == 128) or (extremas[1][0] == 128 and extremas[1][1] == 128) or (extremas[2][0] == 128 and extremas[2][1] == 128):
                    logging.warning('Skipping no diffs %s, name=%s', str(extremas), fileName)
                    skippedTiny.append((rowIndex, fileName, extremas))
                    continue
                # realImgOrig = imgOrig # is this useful?
                imgOrig = diffImg
                fileNameParts = os.path.splitext(fileName)
                fileName = str(fileNameParts[0]) + ('_Diff%d' % minusMinutes) + fileNameParts[1]

            for newCoords in cropCoords:
                # XXXX - save work if old=new?
                logging.warning('coords old %s, new %s', str(oldCoords), str(newCoords))
                imgNameNoExt = str(os.path.splitext(fileName)[0])
                cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '.jpg'
                cropImgPath = os.path.join(args.outputDir, cropImgName)
                cropped_img = imgOrig.crop((newCoords[0] - extremaCoords[0], newCoords[1] - extremaCoords[1],
                                            newCoords[2] - extremaCoords[0], newCoords[3] - extremaCoords[1]))
                cropped_img.save(cropImgPath, format='JPEG', quality=95)
                if recropType == 'augment':
                    flipped_img = cropped_img.transpose(Image.FLIP_LEFT_RIGHT)
                    flipImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), newCoords))) + '_Flip.jpg'
                    flipImgPath = os.path.join(args.outputDir, flipImgName)
                    flipped_img.save(flipImgPath, format='JPEG', quality=95)
            logging.warning('Processed row: %d, file: %s', rowIndex, fileName)
    logging.warning('Skipped tiny images %d, %s', len(skippedTiny), str(skippedTiny))
    logging.warning('Skipped huge images %d, %s', len(skippedHuge), str(skippedHuge))
    logging.warning('Skipped images without archives %d, %s', len(skippedArchive), str(skippedArchive))

if __name__=="__main__":
    main()
