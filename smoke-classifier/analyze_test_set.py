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

    
def classifyImages(detectionPolicy, imageList, className, outFile):
    count = 0
    image_name = []
    crop_name = []
    score_name = []
    class_name = []
    positives = []
    negatives = []
    try:
        for image in imageList:
            t0 = time.time()
            isPositive = False
            ppath = pathlib.PurePath(image)
            nameParsed = img_archive.parseFilename(image)

            image_spec = [{}]
            image_spec[-1]['path'] = image
            image_spec[-1]['timestamp'] = nameParsed['unixTime']
            image_spec[-1]['cameraID'] = nameParsed['cameraID']

            try:
                detectionResult = detectionPolicy.detect(image_spec)
                if detectionResult['fireSegment']:
                    isPositive = True
                if detectionResult['segments']:
                    segments = detectionResult['segments']
                    for i in range(len(segments)):
                        image_name += [ppath.name]
                        crop_name += [segments[i]['coordStr']]
                        # for testing
                        # segments[i]['score'] = random.random()*.55
                        score_name += [segments[i]['score']]
                        class_name += [className]
                        if segments[i]['score'] > .5:
                            isPositive = True

            except Exception as e:
                logging.error('FAILURE processing %s. Count: %d, Error: %s', image, count, str(e))
                test_data = [image_name, crop_name, score_name, class_name]
                np.savetxt(outFile + '-ERROR-' + image + '.txt', np.transpose(test_data), fmt = "%s")
                sys.exit()

            t2 = time.time()

            count += 1
            if isPositive:
                positives.append(ppath.name)
            else:
                negatives.append(ppath.name)
            sys.stdout.write('\r>> Caclulated %d/%d of class %s' % (
                count, len(imageList), className))
            # logging.warning('Timing %f: %f, %f' % (t2-t0, t1-t0, t2-t1))
            sys.stdout.flush()
    except Exception as e:
        logging.error('Failure after %d images of class %s. Error: %s', count, className, str(e))
        try:
            test_data = [image_name, crop_name, score_name, class_name]
            np.savetxt(outFile + '-ERROR.txt', np.transpose(test_data), fmt = "%s")
        except Exception as e:
            logging.error('Total Failure, Moving On. Error: %s', str(e))
    sys.stdout.write('\n')
    sys.stdout.flush()
    return (image_name, crop_name, score_name, class_name, positives, negatives)


def safeDiv(dividend, divisor):
    if divisor == 0:
        return 0
    return dividend / divisor


def main():
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' # quiet down tensorflow logging

    reqArgs = [
        ["d", "directory", "directory containing the image sets"],
        ["o", "outputFile", "output file name"],
    ]
    optArgs = [
        ["l", "labels", "labels file generated during retraining"],
        ["m", "model", "model file generated during retraining"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    model_file = args.model if args.model else settings.model_file
    labels_file = args.labels if args.labels else settings.labels_file
    DetectionPolicyClass = policies.get_policies()[settings.detectionPolicy]
    detectionPolicy = DetectionPolicyClass(args, None, 0, stateless=True, modelLocation=model_file)

    test_data = []

    image_name = []
    crop_name = []
    score_name = []
    class_name = []

    image_name += ["Image"]
    crop_name += ["Crop"]
    score_name += ["Score"]
    class_name += ["Class"]

    smokeDir = os.path.join(args.directory, 'test_set_smoke')
    smoke_image_list = listJpegs(smokeDir)
    logging.warning('Found %d images of smoke', len(smoke_image_list))
    nonSmokeDir = os.path.join(args.directory, 'test_set_other')
    other_image_list = listJpegs(nonSmokeDir)
    logging.warning('Found %d images of nonSmoke', len(other_image_list))

    smokeFile = os.path.join(args.directory, 'test_smoke.txt')
    np.savetxt(smokeFile, smoke_image_list, fmt = "%s")
    nonSmokeFile = os.path.join(args.directory, 'test_other.txt')
    np.savetxt(nonSmokeFile, other_image_list, fmt = "%s")
    outFile = open(args.outputFile, 'w')

    (i,cr,s,cl, positives, negatives) = classifyImages(detectionPolicy, smoke_image_list, 'smoke', args.outputFile)
    image_name += i
    crop_name += cr
    score_name += s
    class_name += cl
    logging.warning('Done with smoke images')
    truePositive = len(positives)
    falseNegative = len(smoke_image_list) - len(positives)
    logging.warning('True Positive: %d', truePositive)
    logging.warning('False Negative: %d', falseNegative)
    outFile.write('True Positives: ' + ', '.join(positives) + '\n')
    outFile.write('False Negative: ' + ', '.join(negatives) + '\n')

    (i,cr,s,cl, positives, negatives) = classifyImages(detectionPolicy, other_image_list, 'other', args.outputFile)
    image_name += i
    crop_name += cr
    score_name += s
    class_name += cl
    logging.warning('Done with nonSmoke images')
    falsePositive = len(positives)
    trueNegative = len(other_image_list) - len(positives)
    logging.warning('False Positive: %d', falsePositive)
    logging.warning('True Negative: %d', trueNegative)
    outFile.write('False Positives: ' + ', '.join(positives) + '\n')
    outFile.write('True Negative: ' + ', '.join(negatives) + '\n')

    accuracy = safeDiv(truePositive + trueNegative, truePositive + trueNegative + falsePositive + falseNegative)
    logging.warning('Accuracy: %f', accuracy)
    precision = safeDiv(truePositive, truePositive + falsePositive)
    logging.warning('Precision: %f', precision)
    recall = safeDiv(truePositive, truePositive + falseNegative)
    logging.warning('Recall: %f', recall)
    f1 = safeDiv(2 * precision*recall, precision + recall)
    logging.warning('F1: %f', f1)

    test_data = [image_name, crop_name, score_name, class_name]
    np.savetxt(outFile, np.transpose(test_data), fmt = "%s")
    outFile.close()
    print("DONE")


if __name__=="__main__":
    main()
