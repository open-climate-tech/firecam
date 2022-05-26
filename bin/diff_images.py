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

Diff two images

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive

import logging
from PIL import Image
import cv2
import numpy as np


def safeRemove(fileName):
    if os.path.isfile(fileName):
        os.remove(fileName)


def main():
    reqArgs = [
        ["a", "imgA", "image to subtract from (newer)"],
        ["b", "imgB", "image to subtract (earlier)"],
        ["o", "imgOutput", "output image"],
    ]
    optArgs = [
        ["m", "maxIterations", "(optional) iteration count", int],
        ["e", "eps", "(optional) eps", float],
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])

    imgA = Image.open(args.imgA)
    imgB = Image.open(args.imgB)
    diffImg = img_archive.diffWithChecks(imgA, imgB)
    diffImg.save(args.imgOutput, format='JPEG', quality=95)
    # cvImgA = cv2.imread(args.imgA)
    # cvImgB = cv2.imread(args.imgB)
    # maxIterations = args.maxIterations if args.maxIterations else 40
    # eps = args.eps if args.eps else 1e-6
    # (algined, dx, dy) = img_archive.findTranslationOffset(cvImgA, cvImgB, maxIterations, eps)
    # if algined:
    #     logging.warning('final dx, dy: %s, %s', round(dx), round(dy))
    #     pImgA = Image.open(args.imgA)
    #     pImgB = Image.open(args.imgB)
    #     shiftedImg = pImgB.transform(pImgB.size, Image.AFFINE, (1, 0, dx, 0, 1, dy))
    #     safeRemove('shiftedB.jpg')
    #     shiftedImg.save('shiftedB.jpg', format='JPEG', quality=95)
    #     diffImg = img_archive.diffSmoothImages(pImgA, shiftedImg)
    #     safeRemove(args.imgOutput)
    #     diffImg.save(args.imgOutput, format='JPEG', quality=95)
    #     diffImg.close()
    #     shiftedImg.close()
    #     pImgA.close()
    #     pImgB.close()
    # else:
    #     logging.warning('too different to diff')


if __name__=="__main__":
    main()
