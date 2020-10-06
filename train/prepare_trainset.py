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
# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
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

Shuffle and package training data into TFRecords format for training

This file leverages some code from following tensorflow slim files.
Adding their copyright message as well
https://github.com/tensorflow/models/blob/master/research/slim/datasets/download_and_convert_flowers.py
https://github.com/tensorflow/models/blob/master/research/slim/datasets/dataset_utils.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os, sys
from firecam.lib import collect_args

import math
import random
import logging

import tensorflow as tf


def int64_feature(values):
    """Returns a TF-Feature of int64s.

    Args:
        values: A scalar or list of values.

    Returns:
        A TF-Feature.
    """
    if not isinstance(values, (tuple, list)):
        values = [values]
    return tf.train.Feature(int64_list=tf.train.Int64List(value=values))


def bytes_feature(values):
    """Returns a TF-Feature of bytes.

    Args:
        values: A string.

    Returns:
        A TF-Feature.
    """
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[values]))


def image_to_tfexample(image_data, image_format, height, width, class_id):
    return tf.train.Example(features=tf.train.Features(feature={
        'image/encoded': bytes_feature(image_data),
        'image/format': bytes_feature(image_format),
        'image/class/label': int64_feature(class_id),
        'image/height': int64_feature(height),
        'image/width': int64_feature(width),
    }))


LABELS_FILENAME = 'labels.txt'
def write_label_file(labels_to_class_names, dataset_dir,
                     filename=LABELS_FILENAME):
    """Writes a file with the list of class names.

    Args:
        labels_to_class_names: A map of (integer) labels to class names.
        dataset_dir: The directory in which the labels file should be written.
        filename: The filename where the class names are written.
    """
    labels_filename = os.path.join(dataset_dir, filename)
    with tf.io.gfile.GFile(labels_filename, 'w') as f:
        for label in labels_to_class_names:
            class_name = labels_to_class_names[label]
            f.write('%d:%s\n' % (label, class_name))


def _get_filenames_and_classes(dataset_dir):
    """Returns a list of filenames and inferred class names.

    Args:
        dataset_dir: A directory containing a set of subdirectories representing
        class names. Each subdirectory should contain PNG or JPG encoded images.

    Returns:
        A list of image file paths, relative to `dataset_dir` and the list of
        subdirectories, representing class names.
    """
    directories = []
    class_names = []
    for filename in os.listdir(dataset_dir):
        path = os.path.join(dataset_dir, filename)
        if os.path.isdir(path):
            directories.append(path)
            class_names.append(filename)

    photo_filenames = []
    for directory in directories:
        for filename in os.listdir(directory):
            path = os.path.join(directory, filename)
            photo_filenames.append(path)

    return photo_filenames, sorted(class_names)


def _get_dataset_filename(dataset_dir, split_name, shard_id, numShards):
    output_filename = 'firecam_%s_%05d-of-%05d.tfrecord' % (
        split_name, shard_id, numShards)
    return os.path.join(dataset_dir, output_filename)


def _convert_dataset(split_name, filenames, class_names_to_ids, dataset_dir):
    """Converts the given filenames to a TFRecord dataset.

    Args:
        split_name: The name of the dataset, either 'train' or 'validation'.
        filenames: A list of absolute paths to png or jpg images.
        class_names_to_ids: A dictionary from class names (strings) to ids
        (integers).
        dataset_dir: The directory where the converted datasets are stored.
    """
    assert split_name in ['train', 'validation']

    numShards = int(math.ceil(len(filenames) / 9000)) # 9000 images results in ~90MB shards
    num_per_shard = int(math.ceil(len(filenames) / float(numShards)))

    for shard_id in range(numShards):
        output_filename = _get_dataset_filename(
            dataset_dir, split_name, shard_id, numShards)

        with tf.io.TFRecordWriter(output_filename) as tfrecord_writer:
            start_ndx = shard_id * num_per_shard
            end_ndx = min((shard_id+1) * num_per_shard, len(filenames))
            for i in range(start_ndx, end_ndx):
                sys.stdout.write('\r>> Converting image %d/%d shard %d' % (
                    i+1, len(filenames), shard_id))
                sys.stdout.flush()

                image_data = tf.io.gfile.GFile(filenames[i], 'rb').read()
                image_shape = tf.image.decode_jpeg(image_data).shape
                class_name = os.path.basename(os.path.dirname(filenames[i]))
                class_id = class_names_to_ids[class_name]

                example = image_to_tfexample(
                    image_data, b'jpg', image_shape[0], image_shape[1], class_id)
                tfrecord_writer.write(example.SerializeToString())

    sys.stdout.write('\n')
    sys.stdout.flush()


def writeTFRecords(inputDir, outputDir, trainPercentage):
    """Converts images to TFRecord dataset.

    Args:
        inputDir (str): The directory containing images in subdirs with class labels
        outputDir (str): The directory where the converted datasets are stored.
        trainPercentage (int): Percentage of data to use for training vs. validation
    """
    image_filenames, class_names = _get_filenames_and_classes(inputDir)
    logging.warning('Processing %d files in %d classes', len(image_filenames), len(class_names))
    class_names_to_ids = dict(zip(class_names, range(len(class_names))))

    # Divide into train and test:
    randomSeed = 0 # Seed for repeatability
    random.seed(randomSeed)
    random.shuffle(image_filenames)
    numTrainingImages = int(trainPercentage * len(image_filenames) / 100)
    training_filenames = image_filenames[:numTrainingImages]
    validation_filenames = image_filenames[numTrainingImages:]
    logging.warning('Splitting into %d for training and %d for validation', len(training_filenames), len(validation_filenames))

    # First, convert the training and validation sets.
    _convert_dataset('train', training_filenames, class_names_to_ids, outputDir)
    _convert_dataset('validation', validation_filenames, class_names_to_ids, outputDir)

    # Finally, write the labels file:
    labels_to_class_names = dict(zip(range(len(class_names)), class_names))
    write_label_file(labels_to_class_names, outputDir)


def main():
    reqArgs = [
        ["i", "inputDir", "local directory containing both smoke and nonSmoke images"],
        ["o", "outputDir", "local directory to write out TFRecords files"],
    ]
    optArgs = [
        ["t", "trainPercentage", "percentage of data to use for training vs. validation (default 90)"]
    ]
    
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    trainPercentage = int(args.trainPercentage) if args.trainPercentage else 90

    writeTFRecords(args.inputDir, args.outputDir, trainPercentage)


if __name__=="__main__":
    main()
