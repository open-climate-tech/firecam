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

import numpy as np
import logging
import pathlib
import tensorflow as tf
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

config = tf.ConfigProto()
config.gpu_options.per_process_gpu_memory_fraction = 0.1

def listJpegs(dirName):
    allEntries = os.listdir(dirName)
    jpegs=[]
    for x in allEntries:
        if x[-4:] == '.jpg':
            jpegs += [os.path.join(dirName, x)]
    return jpegs

def segmentImage(imgPath):
    img = Image.open(imgPath)
    ppath = pathlib.PurePath(imgPath)    
    return rect_to_squares.cutBoxes(img, str(ppath.parent), imgPath)

    
def deleteImageFiles(segments):
    for segmentInfo in segments:
        os.remove(segmentInfo['imgPath'])


def classifyImages(graph, labels, imageList, className, outFile):
    numPositive = 0
    count = 0
    image_name = []
    crop_name = []
    score_name = []
    class_name = []
    try:
        config = tf.ConfigProto()
        config.gpu_options.per_process_gpu_memory_fraction = 0.1 #hopefully reduces segfaults
        with tf.Session(graph=graph, config=config) as tfSession:
            for image in imageList:
                isPositive = False
                segments = segmentImage(image)
                try:
                    tf_helper.classifySegments(tfSession, graph, labels, segments)
                    for i in range(len(segments)):
                        image_name += [image[35:]]
                        crop_name += [segments[i]['imgPath'][35:]]
                        score_name += [segments[i]['score']]
                        class_name += [className]
                        if segments[i]['score'] > .5:
                            isPositive = True

                except Exception as e:
                    logging.error('FAILURE processing %s. Count: %d, Error: %s', image, count, str(e))
                    test_data = [image_name, crop_name, score_name, class_name]
                    np.savetxt(outFile + '-ERROR-' + image + '.txt', np.transpose(test_data), fmt = "%s")
                    deleteImageFiles(segments)
                    sys.exit()
                
                deleteImageFiles(segments)
                count += 1
                if isPositive:
                    numPositive += 1
                sys.stdout.write('\r>> Caclulated %d/%d of class %s' % (
                    count, len(imageList), className))
                sys.stdout.flush()
    except Exception as e:
        logging.error('Failure after %d images of class %s. Error: %s', count, className, str(e))
        try:
            test_data = [image_name, crop_name, score_name, class_name]
            np.savetxt(outFile + '-ERROR.txt', np.transpose(test_data), fmt = "%s")
            deleteImageFiles(segments)
        except Exception as e:
            logging.error('Total Failure, Moving On. Error: %s', str(e))
    sys.stdout.write('\n')
    sys.stdout.flush()
    return (image_name, crop_name, score_name, class_name, numPositive)


def main():
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

    test_data = []

    image_name = []
    crop_name = []
    score_name = []
    class_name = []

    image_name += ["Image"]
    crop_name += ["Crop"]
    score_name += ["Score"]
    class_name += ["Class"]

    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' # quiet down tensorflow logging
    graph = tf_helper.load_graph(model_file)
    labels = tf_helper.load_labels(labels_file)

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

    (i,cr,s,cl, numPositive) = classifyImages(graph, labels, smoke_image_list, 'smoke', args.outputFile)
    image_name += i
    crop_name += cr
    score_name += s
    class_name += cl
    logging.warning('Done with smoke images')
    truePositive = numPositive
    falseNegative = len(smoke_image_list) - numPositive
    logging.warning('True Positive: %d', truePositive)
    logging.warning('False Negative: %d', falseNegative)

    (i,cr,s,cl, numPositive) = classifyImages(graph, labels, other_image_list, 'other', args.outputFile)
    image_name += i
    crop_name += cr
    score_name += s
    class_name += cl
    logging.warning('Done with nonSmoke images')
    falsePositive = numPositive
    trueNegative = len(other_image_list) - numPositive
    logging.warning('False Positive: %d', falsePositive)
    logging.warning('True Negative: %d', trueNegative)

    accuracy = (truePositive + trueNegative)/(truePositive + trueNegative + falsePositive + falseNegative)
    logging.warning('Accuracy: %f', accuracy)
    precision = truePositive/(truePositive + falsePositive)
    logging.warning('Precision: %f', precision)
    recall = truePositive/(truePositive + falseNegative)
    logging.warning('Recall: %f', recall)
    f1 = 2 * precision*recall/(precision + recall)
    logging.warning('F1: %f', f1)

    test_data = [image_name, crop_name, score_name, class_name]
    np.savetxt(args.outputFile, np.transpose(test_data), fmt = "%s")
    print("DONE")


if __name__=="__main__":
    main()
