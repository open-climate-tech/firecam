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

Simple utility to break up rectangle into squares

"""

import os
import pathlib
import math
import logging
import numpy as np

def getSegmentRanges(fullSize, segmentSize):
    """Break the given fullSize into ranges of segmentSize

    Divide the range (0,fullSize) into multiple ranges of size
    segmentSize that are equally spaced apart and have approximately
    10% overlap (overlapRatio)

    Args:
        fullSize (int): size of the full range (0, fullSize)
        segmentSize (int): size of each segment

    Returns:
        (list): list of tuples (start, end) marking each segment's range
    """
    overlapRatio = 1.1
    if fullSize <= segmentSize:
        return []  # all segments must be exactly segmentSize
    firstCenter = int(segmentSize/2)
    lastCenter = fullSize - int(segmentSize/2)
    assert lastCenter > firstCenter
    flexSize = lastCenter - firstCenter
    numSegments = math.ceil(flexSize / (segmentSize/overlapRatio))
    offset = flexSize / numSegments
    ranges = []
    for i in range(numSegments):
        center = firstCenter + round(i * offset)
        start = center - int(segmentSize/2)
        if (start + segmentSize) > fullSize:
            break
        ranges.append((start,start + segmentSize))
    ranges.append((fullSize - segmentSize, fullSize))
    # print('ranges', fullSize, segmentSize, ranges)
    # lastC = 0
    # for i, r in enumerate(ranges):
    #     c = (r[0] + r[1])/2
    #     print(i, r[0], r[1], c, c - lastC)
    #     lastC = c
    return ranges


def cutBoxesFiles(imgOrig, outputDirectory, imageFileName, callBackFn=None):
    """Cut the given image into fixed size boxes and store to files

    Divide the given image into square segments of 299x299 (segmentSize below)
    to match the size of images used by InceptionV3 image classification
    machine learning model.  This function uses the getSegmentRanges() function
    above to calculate the exact start and end of each square

    Args:
        imgOrig (Image): Image object of the original image
        outputDirectory (str): name of directory to store the segments
        imageFileName (str): nane of image file (used as segment file prefix)
        callBackFn (function): callback function that's called for each square

    Returns:
        (list): list of segments with filename and coordinates
    """
    segmentSize = 299
    segments = []
    imgName = pathlib.PurePath(imageFileName).name
    imgNameNoExt = str(os.path.splitext(imgName)[0])
    xRanges = getSegmentRanges(imgOrig.size[0], segmentSize)
    yRanges = getSegmentRanges(imgOrig.size[1], segmentSize)

    for yRange in yRanges:
        for xRange in xRanges:
            coords = (xRange[0], yRange[0], xRange[1], yRange[1])
            if callBackFn != None:
                skip = callBackFn(coords)
                if skip:
                    continue
            # output cropped image
            cropImgName = imgNameNoExt + '_Crop_' + 'x'.join(list(map(lambda x: str(x), coords))) + '.jpg'
            cropImgPath = os.path.join(outputDirectory, cropImgName)
            cropped_img = imgOrig.crop(coords)
            cropped_img.save(cropImgPath, format='JPEG')
            cropped_img.close()
            segments.append({
                'imgPath': cropImgPath,
                'MinX': coords[0],
                'MinY': coords[1],
                'MaxX': coords[2],
                'MaxY': coords[3]
            })
    return segments


def cutBoxesArray(imgOrig):
    """Cut the given image into fixed size boxes, normalize data, and return as np arrays

    Divide the given image into square segments of 299x299 (segmentSize below)
    to match the size of images used by InceptionV3 image classification
    machine learning model.  This function uses the getSegmentRanges() function
    above to calculate the exact start and end of each square

    Args:
        imgOrig (Image): Image object of the original image

    Returns:
        (list, list): pair of lists (cropped numpy arrays) and (metadata on boundaries)
    """
    segmentSize = 299
    xRanges = getSegmentRanges(imgOrig.size[0], segmentSize)
    yRanges = getSegmentRanges(imgOrig.size[1], segmentSize)

    crops = []
    segments = []
    imgNpArray = np.asarray(imgOrig, dtype=np.float32)
    imgNormalized = np.divide(np.subtract(imgNpArray,128),128)

    for yRange in yRanges:
        for xRange in xRanges:
            crops.append(imgNormalized[yRange[0]:yRange[1], xRange[0]:xRange[1]])
            coords = (xRange[0], yRange[0], xRange[1], yRange[1])
            coordStr = 'x'.join(list(map(lambda x: str(x), coords)))
            segments.append({
                'coords': coords,
                'coordStr': coordStr,
                'MinX': coords[0],
                'MinY': coords[1],
                'MaxX': coords[2],
                'MaxY': coords[3]
            })
    crops = np.array(crops)

    return crops, segments
