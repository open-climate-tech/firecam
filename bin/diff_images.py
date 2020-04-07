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

Diff two images with offset to avoid negative numbers
result = (128 + a/2) - b/2

"""

import sys
import os
fuegoRoot = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper
import img_archive

import logging
from PIL import Image

def main():
    reqArgs = [
        ["a", "imgA", "image to subtract from"],
        ["b", "imgB", "image to subtract"],
        ["o", "imgOutput", "output image"],
    ]
    optArgs = [
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    imgA = Image.open(args.imgA)
    imgB = Image.open(args.imgB)

    imgOut = img_archive.diffImages(imgA, imgB)
    imgOut.save(args.imgOutput, format='JPEG')


if __name__=="__main__":
    main()
