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
# -*- coding: utf-8 -*-
"""

Divides a single image into an array of squares segments.  It works by first
dividing the image into rows, and then using dividing each row into squares
using rect_to_squares.

Optionally also shows the image split into squares
The displayed image is shrunk to fit screen, but the cropped image are still at full resolution

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import rect_to_squares

#modules for GUI
#from tkinter import *
import tkinter as tk

#modules for image processing
from PIL import Image, ImageTk


def buttonClick(event):
    exit()

def imageDisplay(imgOrig):
    global cadre, fen, photo2, scaleFactor

    fen = tk.Tk()
    fen.title('Very Fast Multiple Cropping Tool')
    screen_width = fen.winfo_screenwidth() - 100
    screen_height = fen.winfo_screenheight() - 100

    print("Image:", (imgOrig.size[0], imgOrig.size[1]), ", Screen:", (screen_width, screen_height))
    scaleX = min(screen_width/imgOrig.size[0], 1)
    scaleY = min(screen_height/imgOrig.size[1], 1)
    scaleFactor = min(scaleX, scaleY)
    print('scale', scaleFactor, scaleX, scaleY)
    img = imgOrig
    if (scaleFactor != 1):
        img = imgOrig.resize((int(imgOrig.size[0]*scaleFactor), int(imgOrig.size[1]*scaleFactor)), Image.ANTIALIAS)
    photo2 = ImageTk.PhotoImage(img)
    cadre = tk.Canvas(fen, width=photo2.width(), height=photo2.height(), bg="light yellow")
    
    cadre.config(highlightthickness=0)
    
    aff=cadre.create_image(0, 0, anchor='nw', image=photo2)
    cadre.bind("<Button-1>", buttonClick)
    cadre.bind("<Button-2>", buttonClick)
    cadre.bind("<Button-3> ", buttonClick)
    cadre.focus_set()
    cadre.pack(side='left', expand='yes', fill='both')


# use multiple colors to make it slightly easier to see the overlapping boxes
colors = ['red', 'green', 'yellow', 'blue']
colorIndex = 0

def showSquares(coords):
    global cadre, scaleFactor, colors, colorIndex

    scaled = list(map(lambda x: int(x*scaleFactor), coords))
    (sx0, sy0, sx1, sy1) = scaled
    offset = ((colorIndex%2) - 0.5)*2 # stagger to avoid overlap of lines
    square = cadre.create_rectangle(sx0 + offset, sy0 + offset, sx1 + offset, sy1 + offset, outline=colors[colorIndex], width=2)
    colorIndex = (colorIndex + 1 ) % len(colors)


def main():
    global fen
    reqArgs = [
        ["i", "image", "filename of the image"],
        ["o", "output", "output directory name"],
    ]
    optArgs = [
        ["d", "display", "(optional) specify any value to display image and boxes"],
        ["f", "fixedSize", "(optional) use 299x299 segments"]
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    # print(args)
    imgOrig = Image.open(args.image)
    callBackFn = None
    if args.display:
        imageDisplay(imgOrig)
        callBackFn = showSquares
    if args.fixedSize:
        rect_to_squares.cutBoxesFixed(imgOrig, args.output, args.image, callBackFn)
    else:
        rect_to_squares.cutBoxes(imgOrig, args.output, args.image, callBackFn)
    if args.display:
        fen.mainloop()


# for testing
if __name__=="__main__":
    main()
