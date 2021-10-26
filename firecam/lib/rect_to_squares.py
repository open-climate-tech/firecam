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
    15% overlap (overlapRatio)

    Args:
        fullSize (int): size of the full range (0, fullSize)
        segmentSize (int): size of each segment

    Returns:
        (list): list of tuples (start, end) marking each segment's range
    """
    overlapRatio = 1.15
    if fullSize < segmentSize:
        return []  # all segments must be exactly segmentSize
    elif fullSize == segmentSize:
        return [(0,segmentSize)]
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


def getRangeFromCenter(center, size, minLimit, maxLimit):
    """Get linear range from center given constraints

    Return (min,max) pair with given range within (minLimit, maxLimit)
    ideally centered at given center

    Args:
        cneter (int): desired center
        size (int): size of the output range
        minLimit (int): absolute minimum value of the output range
        maxLimit (int): absolute maximum value of the output range

    Returns:
        (int, int): start, end of the range
    """
    if (center - int(size/2)) <= minLimit:   # left edge limited
        val0 = minLimit
        val1 = min(val0 + size, maxLimit)
        # print('left', val0, val1, center, size)
    elif (center + int(size/2)) >= maxLimit: # right edge limited
        val1 = maxLimit
        val0 = max(val1 - size, minLimit)
        # print('right', val0, val1, center, size)
    else:                                   # unlimited
        val0 = center - int(size/2)
        val1 = min(val0 + size, maxLimit)
        # print('center', val0, val1, center, size)
    return (val0, val1)


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
            cropped_img.save(cropImgPath, format='JPEG', quality=95)
            cropped_img.close()
            segments.append({
                'imgPath': cropImgPath,
                'MinX': coords[0],
                'MinY': coords[1],
                'MaxX': coords[2],
                'MaxY': coords[3]
            })
    return segments


def cutBoxesArray(imgOrig, startX=0, endX=None, startY=0, endY=None):
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

    if endX == None:
        endX = imgOrig.size[0]
    elif endX < 0:
        endX = imgOrig.size[0] + endX
    startX = max(0, startX)
    endX = min(endX, imgOrig.size[0])
    xRanges = getSegmentRanges(endX - startX, segmentSize)
    xRanges = list(map(lambda x: (x[0] + startX, x[1] + startX), xRanges))

    if endY == None:
        endY = imgOrig.size[1]
    elif endY < 0:
        endY = imgOrig.size[1] + endY
    startY = max(0, startY)
    endY = min(endY, imgOrig.size[1])
    yRanges = getSegmentRanges(endY - startY, segmentSize)
    yRanges = list(map(lambda x: (x[0] + startY, x[1] + startY), yRanges))

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
