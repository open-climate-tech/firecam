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

Show the ML scores for each square in an image

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import img_archive
from firecam.detection_policies import policies

import logging
import pathlib
import tkinter as tk
from PIL import Image, ImageTk, ImageDraw, ImageFont


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
    for counter, segmentInfo in enumerate(segments):
        offset = ((counter%2) - 0.5)*2
        x0 = segmentInfo['MinX']*scaleFactor + offset
        y0 = segmentInfo['MinY']*scaleFactor + offset
        x1 = segmentInfo['MaxX']*scaleFactor + offset
        y1 = segmentInfo['MaxY']*scaleFactor + offset
        centerX = (x0 + x1)/2
        centerY = (y0 + y1)/2
        color = colors[counter % len(colors)]
        scoreStr = '%.2f' % segmentInfo['score']
        canvasTk.create_text(centerX, centerY, fill=color, font="Arial 50", text=scoreStr)
        canvasTk.create_rectangle(x0, y0, x1, y1, outline=color, width=2)
    rootTk.mainloop()


def drawRect(imgDraw, x0, y0, x1, y1, width, color):
    for i in range(width):
        imgDraw.rectangle((x0+i,y0+i,x1-i,y1-i),outline=color)


def drawBoxesAndScores(imgOrig, segments):
    imgDraw = ImageDraw.Draw(imgOrig)
    for counter, segmentInfo in enumerate(segments):
        offset = ((counter%2) - 0.5)*2
        x0 = segmentInfo['MinX'] + offset
        y0 = segmentInfo['MinY'] + offset
        x1 = segmentInfo['MaxX'] + offset
        y1 = segmentInfo['MaxY'] + offset
        color = colors[counter % len(colors)]
        lineWidth=3
        drawRect(imgDraw, x0, y0, x1, y1, lineWidth, color)
        centerX = (x0 + x1)/2
        centerY = (y0 + y1)/2
        fontSize=60
        fontPath = os.path.join(str(pathlib.Path(__file__).parent.parent), 'firecam/data/Roboto-Regular.ttf')
        font = ImageFont.truetype(fontPath, size=fontSize)
        scoreStr = '%.2f' % segmentInfo['score']
        textSize = imgDraw.textsize(scoreStr, font=font)
        centerX -= textSize[0]/2
        centerY -= textSize[1]/2
        imgDraw.text((centerX,centerY), scoreStr, font=font, fill=color)


def main():
    reqArgs = [
        ["i", "image", "filename of the image"],
        ["o", "output", "output directory name"],
    ]
    optArgs = [
        ["m", "model", "model file generated during retraining"],
        ["y", "startY", "(optional) startY override", int],
        ["z", "endY", "(optional) endY override", int],
        ["x", "startX", "(optional) startY override", int],
        ["e", "endX", "(optional) endY override", int],
        ["d", "display", "(optional) specify any value to display image and boxes"]
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    model_file = args.model if args.model else settings.model_file
    DetectionPolicyClass = policies.get_policies()[settings.detectionPolicy]
    detectionPolicy = DetectionPolicyClass(args, None, 0, stateless=True, modelLocation=model_file)
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

    nameParsed = img_archive.parseFilename(args.image)
    image_spec = [{}]
    image_spec[-1]['path'] = args.image
    image_spec[-1]['timestamp'] = nameParsed['unixTime']
    image_spec[-1]['cameraID'] = nameParsed['cameraID']
    if args.startY:
        image_spec[-1]['startY'] = args.startY
    if args.endY:
        image_spec[-1]['endY'] = args.endY
    if args.startX:
        image_spec[-1]['startX'] = args.startX
    if args.endX:
        image_spec[-1]['endX'] = args.endX
    detectionResult = detectionPolicy.detect(image_spec)

    for segmentInfo in detectionResult['segments']:
        # print(segmentInfo['imgPath'], segmentInfo['score'])
        print(segmentInfo['MinX'], segmentInfo['MinY'], segmentInfo['score'])
    if args.display:
        imgOrig = Image.open(args.image)
        drawBoxesAndScores(imgOrig, detectionResult['segments'])
        displayImageWithScores(imgOrig, [])


if __name__=="__main__":
    main()
