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

Allow user to crop a bounding rectangle in an image
Shrinks the displayed image to fit screen, but the cropped image are still at full resolution

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
from tkinter.messagebox import showwarning

#modules for image processing
from PIL import Image, ImageTk

#module for Manage files and use system commands
import pathlib

import math

#===============
#variables declaration
#===============

#Current bounding box position
global x1,x2,y1,y2
#List of rectangles displays on screen
global rectangleShapes, rectangleCoords
#List of square displays on screen
global squareShapes
#Final result returned to caller
global result
#Boolean to control whether to show squares
global showSquares
#===============

# minimum size for squares shown inside bounding box
MIN_SIZE = 150

def leftClick(event):
    global chaine, cadre, selectionBox, x1, y1
    chaine.configure(text = str(event.x)+" "+str(event.y))
    x1=cadre.canvasx(event.x)
    y1=cadre.canvasy(event.y)


def holdLeftClick(event):
    global chaine, cadre, selectionBox, x1, y1, x2, y2
    x2=cadre.canvasx(event.x)
    y2=cadre.canvasy(event.y)

    chaine.configure(text = ' '.join(list(map(lambda x: str(int(x)), [x1, y1, x2, y2]))) + " Frame object number "+str(len(rectangleShapes)))
    cadre.coords(selectionBox, x1,y1,x2,y2)

    # delete old squares
    for square in squareShapes:
        cadre.delete(square)

    # make new squares
    if showSquares:
        squareCoords = rect_to_squares.rect_to_squares(x1, y1, x2, y2, img.size[0], img.size[1], MIN_SIZE)
        for coords in squareCoords:
            (sx0, sy0, sx1, sy1) = coords
            square = cadre.create_rectangle(sx0, sy0, sx1, sy1, outline='red', width=2)
            squareShapes.append(square)


def releaseLeftClick(event):
    global chaine, cadre, selectionBox, x1, y1, x2, y2, rectangleShapes
    cadre.coords(selectionBox, 0, 0, 0, 0)
    x2=cadre.canvasx(event.x)
    y2=cadre.canvasy(event.y)
    minX = int(min(x1, x2))
    maxX = int(max(x1, x2))
    minY = int(min(y1, y2))
    maxY = int(max(y1, y2))
    area = (minX, minY, maxX, maxY)
    if (minX == maxX) or (minY == maxY):
        showwarning("must be at least a pixel wide" + str(area))
        return
    chaine.configure(text = ' '.join(list(map(lambda x: str(int(x)), area))) + " Number of frames "+str(len(rectangleShapes)+1))
    rectangleCoords.append(area)
    rectangleShapes.append(cadre.create_rectangle(minX, minY, maxX, maxY))


def middleClick(event):
    global chaine, cadre, selectionBox, x1, y1, x2, y2, photo2,img, imgOrig, rectangleShapes, rectangleCoords, aff, fen, result, imageFileName, outputDirectory, scaleFactor

    imgName = pathlib.PurePath(imageFileName).name
    imgNameNoExt = str(os.path.splitext(imgName)[0])
    for rectNum in range(len(rectangleShapes)):
        area = rectangleCoords[rectNum]
        print('raw coords:', area, ', scale:', scaleFactor)
        area = list(map(lambda x: int(x/scaleFactor), area))
        area[0] = max(area[0], 0)
        area[1] = max(area[1], 0)
        area[2] = min(area[2], imgOrig.size[0])
        area[3] = min(area[3], imgOrig.size[1])
        cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), area))) + '.jpg'
        cropImgPath = os.path.join(outputDirectory, cropImgName)
        print('coords:'+str(area), cropImgName)
        cropped_img = imgOrig.crop(area)
        cropped_img.save(cropImgPath, format='JPEG')
        cadre.delete(rectangleShapes[rectNum])
        result.append({
            'name': cropImgPath,
            'coords': area
        })

    cadre.delete(aff)
    cadre.create_image(0, 0, anchor='nw', image=photo2)
    selectionBox=cadre.create_rectangle(0,0,0,0)
    rectangleShapes=[]
    rectangleCoords=[]
    fen.destroy()


def rightClick(event):
    global chaine, cadre, rectangleShapes
    if len(rectangleShapes) > 0:
        chaine.configure(text = "Erasing frame number ="+str(len(rectangleShapes)))
        cadre.delete(rectangleShapes[len(rectangleShapes)-1])
        del rectangleShapes[len(rectangleShapes)-1]
    else:
        chaine.configure(text = "Nothing to erase")



def imageDisplay(name, outputDir, showSquaresArg=True):
    global chaine, cadre, selectionBox, aff, fen, img, imgOrig, photo2, x1,x2,y1,y2, rectangleShapes, rectangleCoords, squareShapes, result, imageFileName, outputDirectory, scaleFactor, showSquares
    x1,x2,y1,y2=0,0,0,0
    rectangleShapes = []
    rectangleCoords = []
    squareShapes = []
    result = []
    showSquares = showSquaresArg

    fen = tk.Tk()
    fen.title('Very Fast Multiple Cropping Tool')
    screen_width = fen.winfo_screenwidth() - 100
    screen_height = fen.winfo_screenheight() - 100

    imageFileName = name
    outputDirectory = outputDir
    imgOrig = Image.open(name)
    print("Image:", (imgOrig.size[0], imgOrig.size[1]), ", Screen:", (screen_width, screen_height))
    scaleX = min(screen_width/imgOrig.size[0], 1)
    scaleY = min(screen_height/imgOrig.size[1], 1)
    scaleFactor = min(scaleX, scaleY)
    print('scale', scaleFactor, scaleX, scaleY)
    img = imgOrig
    if (scaleFactor != 1):
        img = imgOrig.resize((int(imgOrig.size[0]*scaleFactor), int(imgOrig.size[1]*scaleFactor)), Image.ANTIALIAS)
    print(name)
    photo2 = ImageTk.PhotoImage(img)
    cadre = tk.Canvas(fen, width=photo2.width(), height=photo2.height(), bg="light yellow")
    
    cadre.config(highlightthickness=0)
    
    sbarV = tk.Scrollbar(fen, orient='vertical')
    sbarH = tk.Scrollbar(fen, orient='horizontal')
    
    sbarV.config(command=cadre.yview)
    sbarH.config(command=cadre.xview)
    
    cadre.config(yscrollcommand=sbarV.set)
    cadre.config(xscrollcommand=sbarH.set)
    
    width=photo2.width()
    height=photo2.height()
    cadre.config(scrollregion=(0,0,width,height))
    
    aff=cadre.create_image(0, 0, anchor='nw', image=photo2) #BUG
    cadre.bind("<Button-1>", leftClick)
    cadre.bind("<B1-Motion>", holdLeftClick)
    cadre.bind("<ButtonRelease-1>", releaseLeftClick)
    cadre.bind("<Button-2>", middleClick)
    cadre.bind("<space>", middleClick)
    cadre.bind("<Return>", middleClick)
    cadre.bind("<ButtonRelease-3>", rightClick)
    cadre.bind("u", rightClick) # undo
    cadre.bind("U", rightClick) # Undo
    cadre.focus_set()
    
    chaineLabels = tk.Label(fen)
    chaine = tk.Label(fen)
    chaineLabels.configure(text = 'crop')
    chaineLabels.pack(side='bottom')
    chaine.pack()
    
    sbarV.pack(side='right', fill='y')
    sbarH.pack(side='bottom', fill='x')
    cadre.pack(side='left', expand='yes', fill='both')
    
    selectionBox=cadre.create_rectangle(0,0,0,0)
    fen.mainloop()
    return result


def main():
    argDefs = [
        ["i", "image", "filename of the image"],
        ["o", "output", "output directory name"],
    ]
    args = collect_args.collectArgs(argDefs)
    print(args)
    imageDisplay(args.image, args.output)


# for testing
if __name__=="__main__":
    main()
