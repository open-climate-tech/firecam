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
downloads the entire image and recrops it by breaking that rectangle into square(s).

Optionally, for debuggins shows the boxes on screen (TODO: refactor display code)

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

import csv
import tkinter as tk
from PIL import Image, ImageTk

# minimum size for squares shown inside bounding box
MIN_SIZE = 150

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


def main():
    reqArgs = [
        ["o", "outputDir", "local directory to save images and segments"],
        ["i", "inputCsv", "csvfile with contents of Fuego Cropped Images"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
        ["d", "display", "(optional) specify any value to display image and boxes"]
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9

    googleServices = goog_helper.getGoogleServices(settings, args)
    cameraCache = {}
    with open(args.inputCsv) as csvFile:
        csvreader = csv.reader(csvFile)
        for (rowIndex, csvRow) in enumerate(csvreader):
            if rowIndex < startRow:
                continue
            if rowIndex > endRow:
                print('Reached end row', rowIndex, endRow)
                exit(0)
            [cropName, minX, minY, maxX, maxY, fileName] = csvRow[:6]
            minX = int(minX)
            minY = int(minY)
            maxX = int(maxX)
            maxY = int(maxY)
            dirID = getCameraDir(googleServices['drive'], cameraCache, fileName)
            localFilePath = os.path.join(args.outputDir, fileName)
            if not os.path.isfile(localFilePath):
                goog_helper.downloadFile(googleServices['drive'], dirID, fileName, localFilePath)
            imgOrig = Image.open(localFilePath)
            squareCoords = rect_to_squares.rect_to_squares(minX, minY, maxX, maxY, imgOrig.size[0], imgOrig.size[1], MIN_SIZE)
            # print(squareCoords)
            imgNameNoExt = str(os.path.splitext(fileName)[0])
            for coords in squareCoords:
                cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), coords))) + '.jpg'
                cropImgPath = os.path.join(args.outputDir, 'cropped', cropImgName)
                cropped_img = imgOrig.crop(coords)
                cropped_img.save(cropImgPath, format='JPEG')
            print('Processed row: %s, file: %s, num squares: %d' % (rowIndex, fileName, len(squareCoords)))
            if args.display:
                squareCoords.append((minX, minY, maxX, maxY))
                displayImageWithScores(imgOrig, squareCoords)
                imageDisplay(imgOrig)


if __name__=="__main__":
    main()
