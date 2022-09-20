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

Flip images horizontally

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.lib import rect_to_squares

import logging
import random
from PIL import Image, ImageTk, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

def main():
    reqArgs = [
        ["i", "inputDir", "input local directory containing nonSmoke image segments"],
        ["o", "outputDir", "output local directory to store flipped images"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
        ["r", "randomRatio", "(optional) only flip randomRatio fraction of images", float],
        ["l", "listFiles", "(optional) filename containing list of files to flip"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    randomRatio = args.randomRatio if args.randomRatio else 1
    if args.listFiles:
        with open(args.listFiles, "r") as fh:
            fileListStr = fh.read()
        imageFileNames = list(filter(lambda x: x, fileListStr.split('\n')))
        imageFileNames = sorted(imageFileNames)
    else:
        imageFileNames = sorted(os.listdir(args.inputDir))
    rowIndex = -1
    for fileName in imageFileNames:
        rowIndex += 1
        if fileName[-4:] != '.jpg':
            continue
        if rowIndex < startRow:
            continue
        if rowIndex > endRow:
            print('Reached end row', rowIndex, endRow)
            break
        if random.random() > randomRatio:
            continue
        img = Image.open(os.path.join(args.inputDir,fileName))
        flipped_img = img.transpose(Image.Transpose.FLIP_LEFT_RIGHT)
        flippedPath = os.path.join(args.outputDir,os.path.splitext(fileName)[0] + '_Flip.jpg')
        flipped_img.save(flippedPath, format='JPEG', quality=95)
        logging.warning('Processed row: %d, file: %s', rowIndex, fileName)
    return


if __name__=="__main__":
    main()
