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

Move images from cameras with too many images

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive

import datetime
import logging
import numpy as np


def updateDistances(cameraTimes):
    for counter, entry in enumerate(cameraTimes):
        prevTime = cameraTimes[counter - 1]['time'] if counter > 0 else 0
        nextTime = cameraTimes[counter + 1]['time'] if counter < len(cameraTimes)-1 else 0
        distance = abs(entry['time'] - prevTime) + abs(entry['time'] - nextTime)
        entry['distance'] = distance



def main():
    reqArgs = [
        ["i", "inputDir", "input local directory containing images"],
    ]
    optArgs = [
        ["t", "threshold", "(optional) threshold for image count above which to delete", int],
        ["o", "outputDir", "directory to move extraneous images"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    cacheDir = img_archive.cacheDir(args.inputDir)
    cameraCounts = []
    for cameraID in cacheDir:
        if isinstance(cacheDir[cameraID], list):
            cameraCounts.append((cameraID, len(cacheDir[cameraID])))
            # logging.warning('cam %s, images %s', cameraID, len(cacheDir[cameraID]))
    counts = list(map(lambda x: x[1], cameraCounts))
    logging.warning('counts: %s', counts)
    logging.warning('mean: %s, median: %s', round(np.mean(counts)), round(np.median(counts)))
    logging.warning('mean+std: %s', round(np.mean(counts) + np.std(counts)))

    if args.threshold:
        threshold = args.threshold
    else:
        threshold = np.mean(counts) + np.std(counts)
    camsAboveThreshold = list(filter(lambda x: x[1] > threshold, cameraCounts))
    camsAboveThreshold = sorted(camsAboveThreshold, key=lambda x: x[1])
    logging.warning('above: %s: %s', len(camsAboveThreshold), camsAboveThreshold)
    if not args.outputDir:
        return

    for (cameraID, count) in camsAboveThreshold:
        logging.warning('ID, count: %s, %s', cameraID, count)
        cameraTimes = cacheDir[cameraID]
        for x in range(count - int(threshold)):
            updateDistances(cameraTimes)
            minDist = min(cameraTimes, key=lambda x: x['distance'])
            logging.warning('min dist %s', minDist)
            if args.outputDir:
                oldPath = os.path.join(args.inputDir, minDist['fileName'])
                newPath = os.path.join(args.outputDir, minDist['fileName'])
                os.rename(oldPath, newPath)
            cameraTimes.remove(minDist)

    return


if __name__=="__main__":
    main()
