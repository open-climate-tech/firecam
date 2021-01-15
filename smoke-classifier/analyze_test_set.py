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
Evaluate the model on test set and generate metrics

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import tf_helper
from firecam.lib import rect_to_squares
from firecam.lib import img_archive
from firecam.detection_policies import policies

import time
import random
import numpy as np
import logging
import pathlib
import gc
import tensorflow as tf
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True


def listJpegs(dirName):
    allEntries = os.listdir(dirName)
    jpegs=[]
    for x in allEntries:
        if x[-4:] == '.jpg':
            jpegs += [os.path.join(dirName, x)]
    return jpegs

def segmentImage(imgPath):
    img = Image.open(imgPath)
    return rect_to_squares.cutBoxesArray(img)

    
def classifyImages(detectionPolicy, checkShifts, imageList, className, outFile):
    count = 0
    positives = []
    negatives = []
    mixed = []
    for image in imageList:
        t0 = time.time()
        ppath = pathlib.PurePath(image)
        nameParsed = img_archive.parseFilename(image)

        image_spec = [{}]
        image_spec[-1]['path'] = image
        image_spec[-1]['timestamp'] = nameParsed['unixTime']
        image_spec[-1]['cameraID'] = nameParsed['cameraID']

        detectionResult = detectionPolicy.detect(image_spec, checkShifts=checkShifts, silent=True)
        # logging.warning('dr %s', str(detectionResult))
        image_spec[-1]['startY'] = 140
        image_spec[-1]['endY'] = -140
        detectionResultOffset = detectionPolicy.detect(image_spec, checkShifts=checkShifts, silent=True)
        if len(detectionResultOffset['segments']) == 0: # happens with tiny images
            detectionResultOffset = detectionResult
        scores = [detectionResult['segments'][0]['score'], detectionResultOffset['segments'][0]['score']]
        if detectionResult['fireSegment'] and detectionResultOffset['fireSegment']:
            status = 'smoke'
            positives.append(ppath.name)
        elif (not detectionResult['fireSegment']) and (not detectionResultOffset['fireSegment']):
            status = 'other'
            negatives.append(ppath.name)
        else:
            status = 'mixed'
            mixed.append(ppath.name)

        t2 = time.time()
        count += 1
        sys.stdout.write('\r>> Caclulated %d/%d of class %s' % (
            count, len(imageList), className))
        # logging.warning('Timing %f: %f, %f' % (t2-t0, t1-t0, t2-t1))
        sys.stdout.flush()
        outFile.write('%s file %s classified as %s: %s\n' % (
            className, ppath.name, status, str(scores)))

        detectionResult = None
        detectionResultOffset = None
        gc.collect()
    sys.stdout.write('\n')
    sys.stdout.flush()
    return (positives, negatives, mixed)


def safeDiv(dividend, divisor):
    if divisor == 0:
        return 0
    return dividend / divisor


def doubleOut(outFile, msg):
    logging.warning(msg)
    outFile.write(msg + '\n')


def main():
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' # quiet down tensorflow logging

    reqArgs = [
        ["d", "directory", "directory containing the image sets"],
        ["o", "outputFile", "output file name"],
    ]
    optArgs = [
        ["l", "labels", "labels file generated during retraining"],
        ["m", "model", "model file generated during retraining"],
        ["c", "checkShifts", "(optional) override default value 1 for checkShifts"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    model_file = args.model if args.model else settings.model_file
    labels_file = args.labels if args.labels else settings.labels_file
    checkShifts = bool(int(args.checkShifts)) if args.checkShifts else True
    DetectionPolicyClass = policies.get_policies()[settings.detectionPolicy]
    detectionPolicy = DetectionPolicyClass(args, None, 0, stateless=True, modelLocation=model_file)

    smokeDir = os.path.join(args.directory, 'test_set_smoke')
    smoke_image_list = listJpegs(smokeDir)
    logging.warning('Found %d images of smoke', len(smoke_image_list))
    nonSmokeDir = os.path.join(args.directory, 'test_set_other')
    other_image_list = listJpegs(nonSmokeDir)
    logging.warning('Found %d images of nonSmoke', len(other_image_list))

    outFile = open(args.outputFile, 'w')
    doubleOut(outFile, 'Checking model %s' % model_file)

    (positives, negatives, mixed) = classifyImages(detectionPolicy, checkShifts, smoke_image_list, 'smoke', outFile)
    logging.warning('Done with smoke images')
    doubleOut(outFile, 'Smoke counts (pos,neg,mixed): %d, %d, %d' % (len(positives), len(negatives), len(mixed)))
    truePositive = len(positives)
    falseNegative = len(negatives) + len(mixed)
    logging.warning('True Positive: %d', truePositive)
    logging.warning('False Negative: %d', falseNegative)
    outFile.write('True Positives: ' + ', '.join(positives) + '\n')
    outFile.write('False Negative: ' + ', '.join(negatives) + '\n')
    outFile.write('Mixed smoke: ' + ', '.join(mixed) + '\n')

    (positives, negatives, mixed) = classifyImages(detectionPolicy, checkShifts, other_image_list, 'other', outFile)
    logging.warning('Done with nonSmoke images')
    doubleOut(outFile, 'nonSmoke counts (pos,neg,mixed): %d, %d, %d' % (len(positives), len(negatives), len(mixed)))
    falsePositive = len(positives) + len(mixed)
    trueNegative = len(negatives)
    logging.warning('False Positive: %d', falsePositive)
    logging.warning('True Negative: %d', trueNegative)
    outFile.write('False Positives: ' + ', '.join(positives) + '\n')
    outFile.write('True Negative: ' + ', '.join(negatives) + '\n')
    outFile.write('Mixed nonSmoke: ' + ', '.join(mixed) + '\n')

    accuracy = safeDiv(truePositive + trueNegative, truePositive + trueNegative + falsePositive + falseNegative)
    doubleOut(outFile, 'Accuracy: %f' % accuracy)
    precision = safeDiv(truePositive, truePositive + falsePositive)
    doubleOut(outFile, 'Precision: %f' % precision)
    recall = safeDiv(truePositive, truePositive + falseNegative)
    doubleOut(outFile, 'Recall: %f' % recall)
    f1 = safeDiv(2 * precision*recall, precision + recall)
    doubleOut(outFile, 'F1: %f' % f1)

    outFile.close()
    print("DONE")


if __name__=="__main__":
    main()
