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

Show the ML scores for each square in an image

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import os
fuegoRoot = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import rect_to_squares
import tf_helper

import pathlib
import subprocess
import re
import numpy as np
import tensorflow as tf
import math
import tkinter as tk
from PIL import Image, ImageTk, ImageDraw, ImageFont


# alternate version that uses in-memory image segments without persisting to disk
from skimage import io
import numpy as np

def read_tensor_from_array(data,
                            input_height=299,
                            input_width=299,
                            input_mean=0,
                            input_std=255):
  float_caster = data
  dims_expander = tf.expand_dims(float_caster, 0)
  resized = tf.image.resize_bilinear(dims_expander, [input_height, input_width])
  normalized = tf.divide(tf.subtract(resized, [input_mean]), [input_std])
  sess = tf.Session()
  result = sess.run(normalized)

  return result


def calcScoresInMemory(model_file, label_file, imgPath):
    # XXXX
    config = tf.ConfigProto()
    config.gpu_options.per_process_gpu_memory_fraction = 0.1

    graph_def = tf.GraphDef()
    with tf.gfile.FastGFile(model_file, 'rb') as f:
        graph_def.ParseFromString(f.read())
        tf.import_graph_def(graph_def, name='')

    # Unpersists graph from file
    # with tf.Session(config=config) as sess:  #config=tf.ConfigProto(log_device_placement=True)
    with tf.Session(config=config) as sess:

        image = io.imread(imgPath, plugin='matplotlib')
        print(image.shape)
        # hardcoded for testing the code path.
        coords=[
            ( 0,0,341,339 ),
            ( 341,0,682,339 ),
            ( 682,0,1024,339 ),
            ( 0,290,341,631 ),
            ( 341,290,682,631 ),
            ( 682,290,1024,631 ),
            ( 0,597,341,938 ),
            ( 341,597,682,938 ),
            ( 682,597,1024,938 ),
        ]
        for (minX,minY,maxX,maxY) in coords:
            print('coord', (minX,minY,maxX,maxY))
            cropped_image=image[minY:maxY, minX:maxX]
            print('shape', cropped_image.shape)
            image_data = np.array(cropped_image)[:,:,0:3]
            res = read_tensor_from_array(image_data)
            # Feed the image_data as input to the graph and get first prediction
            softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
            try:
                predictions = sess.run(softmax_tensor, {'Placeholder:0': res})
                print('preds', predictions)
            except:
                print("image fault", imgPath, (minX,minY,maxX,maxY))
                #Caused by image not decoding properly, even though it was a .jpg. Crashed the entire session.
                print(image_data.shape)
                continue


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
        font = ImageFont.truetype(os.path.join(settings.fuegoRoot, 'lib/Roboto-Regular.ttf'), size=fontSize)
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
        ["l", "labels", "labels file generated during retraining"],
        ["m", "model", "model file generated during retraining"],
        ["d", "display", "(optional) specify any value to display image and boxes"]
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    model_file = args.model if args.model else settings.model_file
    labels_file = args.labels if args.labels else settings.labels_file

    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    graph = tf_helper.load_graph(model_file)
    labels = tf_helper.load_labels(labels_file)
    segments = []
    with tf.Session(graph=graph) as tfSession:
        if True: # chops image in segment files and classifies each segment
            imgOrig = Image.open(args.image)
            segments = rect_to_squares.cutBoxes(imgOrig, args.output, args.image)
            tf_helper.classifySegments(tfSession, graph, labels, segments)

        if False: # version that classifies entire image without cropping
            imgOrig = Image.open(args.image)
            segments = [{'imgPath': args.image}]
            tf_helper.classifySegments(tfSession, graph, labels, segments)

        if False: # chops image into in-memory segments and classifies each segment
            calcScoresInMemory(args.model, args.labels, args.image)

        for segmentInfo in segments:
            print(segmentInfo['imgPath'], segmentInfo['score'])
        if args.display:
            drawBoxesAndScores(imgOrig, segments)
            displayImageWithScores(imgOrig, [])


if __name__=="__main__":
    main()
