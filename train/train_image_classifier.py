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

Training code using Keras for TF2

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import tf_helper

import glob
import tensorflow as tf
from tensorflow import keras
import logging
import datetime

def _parse_function(example_proto):
    """
    Function for converting TFRecordDataset to uncompressed image pixels + labels
    :return:
    """
    feature_description = {
    'image/class/label': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'image/encoded': tf.io.FixedLenFeature([], tf.string, default_value=''),
    'image/format': tf.io.FixedLenFeature([], tf.string, default_value=''),
    'image/height': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'image/width': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    }

    # Parse the input `tf.Example` proto using the dictionary above.
    example = tf.io.parse_single_example(example_proto, feature_description)
    image = tf.image.decode_image(example['image/encoded'], channels=3)

    #Resizing images in training set because they are apprently rectangular much fo the time
    if example['image/height'] != 299 or example['image/width'] != 299:
        image = tf.image.resize(tf.reshape(image, [example['image/height'], example['image/width'], 3]), [299, 299])
        image = tf.cast(image, tf.uint8)

    image = tf.reshape(image, [299, 299, 3]) #weird workaround because decode image doesnt get shape
    label = tf.one_hot(example['image/class/label'], depth=2)

    image = (tf.cast(image, tf.float32) - 128) / 128.0
    return [image, label]


class LRTensorBoard(keras.callbacks.TensorBoard):
    def __init__(self, log_dir, **kwargs):  # add other arguments to __init__ if you need
        super().__init__(log_dir=log_dir, **kwargs)

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        logs.update({'lr': keras.backend.eval(self.model.optimizer.lr)})
        super().on_epoch_end(epoch, logs)


def main():
    logging.warning('h0')
    reqArgs = [
        ["i", "inputDir", "directory containing TFRecord files"],
        ["o", "outputDir", "directory to write out checkpoints and tensorboard logs"],
        ["a", "algorithm", "adam, nadam, or rmsprop"],
    ]
    optArgs = [
        ["r", "resumeModel", "resume training from given saved model"],
        ["s", "startEpoch", "epoch to resume from (epoch from resumeModel)"],
        ["t", "stepsPerEpoch", "(optional) number of steps per epoch", int],
        ["v", "valStepsPerEpoch", "(optional) number of validation steps per epoch", int],
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])

    batch_size = 64
    max_epochs = 1000
    steps_per_epoch = args.stepsPerEpoch if args.stepsPerEpoch else 2000
    overshoot_epochs = 30 #number of epochs over which validation loss hasnt decreased to stop training at
    val_steps = args.valStepsPerEpoch if args.valStepsPerEpoch else 200
    #val_steps only needed for now because of a bug in tf2.0, which should be fixed in next version
    #TODO: either set this to # of validation examples /batch size (i.e. figure out num validation examples)
    #or upgrade to TF2.1 when its ready and automatically go thorugh the whole set

    train_filenames = glob.glob(os.path.join(args.inputDir, 'firecam_train_*.tfrecord'))
    val_filenames = glob.glob(os.path.join(args.inputDir, 'firecam_validation_*.tfrecord'))
    logging.warning('Found %d training files, and %d validation files', len(train_filenames), len(val_filenames))
    if (len(train_filenames) == 0) or (len(val_filenames) == 0):
        logging.error('Could not find data in %s', args.inputDir)
        exit(1)

    raw_dataset_train = tf.data.TFRecordDataset(train_filenames)
    raw_dataset_val = tf.data.TFRecordDataset(val_filenames)

    dataset_train = raw_dataset_train.map(_parse_function).repeat(max_epochs * steps_per_epoch).shuffle(batch_size * 5).batch(batch_size)
    dataset_val = raw_dataset_val.map(_parse_function).repeat().batch(batch_size)

    if args.resumeModel:
        inception = tf_helper.loadModel(args.resumeModel)
        assert int(args.startEpoch) > 0
        initial_epoch = int(args.startEpoch)
    else:
        inception = keras.applications.inception_v3.InceptionV3(weights=None, include_top=True, input_tensor=None,
                                                                classes=2)
        initial_epoch = 0
    if args.algorithm == "adam":
        # optimizer = tf.keras.optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)
        optimizer = tf.keras.optimizers.Adam(decay=1e-06, amsgrad=True)
    elif args.algorithm == "nadam":
        optimizer = tf.keras.optimizers.Nadam()
    elif args.algorithm == "rmsprop":
        optimizer = tf.keras.optimizers.RMSprop(decay=1e-06)
    else:
        logging.error('Unsupported algo %s', args.algorithm)
        exit(1)

    inception.compile(optimizer=optimizer, loss=tf.keras.losses.BinaryCrossentropy(), metrics=['accuracy'])

    logdir = os.path.join(args.outputDir, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    callbacks = [keras.callbacks.EarlyStopping(monitor='val_loss', patience=overshoot_epochs),
                 keras.callbacks.ModelCheckpoint(filepath=os.path.join(args.outputDir, 'model_{epoch}'),
                                                 monitor='val_loss', save_best_only=True),
                 LRTensorBoard(log_dir=logdir)]

    logging.warning('Start training')
    inception.fit(dataset_train, validation_data=dataset_val,
                  epochs=max_epochs, initial_epoch=initial_epoch,
                  steps_per_epoch=steps_per_epoch, validation_steps=val_steps,
                  callbacks=callbacks)
    logging.warning('Done training')


if __name__ == "__main__":
    main()
