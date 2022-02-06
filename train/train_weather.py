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

Train ML model incorporating weather data

"""

import os, sys

from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import weather

import logging
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
# from tensorflow.keras.layers.experimental import preprocessing


def main():
    reqArgs = [
        ["i", "inputCsv", "csvfile with normalized fire and weather data"],
        ["o", "outputDir", "directory to write out checkpoints"],
    ]
    optArgs = [
        ["l", "levels", "(optional) number of levels (default 1)", int],
        ["m", "maxEpochs", "(optional) max number of epochs (default 1000)", int],
        ["t", "trainPercentage", "percentage of data to use for training vs. validation (default 70)", int]
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])

    numLevels = int(args.levels) if args.levels else 1
    max_epochs = args.maxEpochs if args.maxEpochs else 1000
    trainPercentage = int(args.trainPercentage) if args.trainPercentage else 70
    trainRatio = trainPercentage / 100

    (all_features, all_labels) = weather.readWeatherCsv(args.inputCsv)

    # train_size=len(raw_dataset)
    train_size = int(len(all_features)*trainRatio)
    val_size = len(all_features) - train_size

    train_features = all_features.head(train_size)
    train_labels = all_labels.head(train_size)

    val_features = all_features.tail(val_size)
    val_labels = np.array(all_labels.tail(val_size))

    if numLevels == 1:
        weather_model = keras.Sequential([layers.Dense(1,input_dim=20,activation='sigmoid')])
    elif numLevels == 2:
        weather_model = tf.keras.Sequential([
            layers.Dense(8,input_dim=20,activation='relu'),
            layers.Dense(1,input_dim=8,activation='sigmoid')
            ])
    elif numLevels == 3:
        weather_model = tf.keras.Sequential([
            layers.Dense(10,input_dim=20,activation='relu'),
            layers.Dropout(0.1),
            layers.Dense(4,input_dim=10,activation='relu'),
            layers.Dense(1,input_dim=4,activation='sigmoid')
            ])
    elif numLevels == 4:
        weather_model = tf.keras.Sequential([
            layers.Dense(12,input_dim=20,activation='relu'),
            layers.Dropout(0.2),
            layers.Dense(8,input_dim=12,activation='relu'),
            layers.Dropout(0.1),
            layers.Dense(4,input_dim=8,activation='relu'),
            layers.Dense(1,input_dim=4,activation='sigmoid')
            ])
    elif numLevels == 5:
        weather_model = tf.keras.Sequential([
            layers.Dense(14,input_dim=20,activation='relu'),
            layers.Dropout(0.2),
            layers.Dense(10,input_dim=14,activation='relu'),
            layers.Dropout(0.1),
            layers.Dense(6,input_dim=10,activation='relu'),
            layers.Dense(4,input_dim=6,activation='relu'),
            layers.Dense(1,input_dim=4,activation='sigmoid')
            ])
    else:
        logging.error('Unsupported levels %s', numLevels)
        exit(1)
    weather_model.compile(loss = keras.losses.BinaryCrossentropy(), optimizer = tf.optimizers.Adam(), metrics=['accuracy'])

    callback = keras.callbacks.ModelCheckpoint(filepath=os.path.join(args.outputDir, 'model_{epoch}'),
                                               monitor='loss', save_best_only=True)

    weather_model.fit(train_features, train_labels,
                      epochs=max_epochs, callbacks=[callback])
    weights = weather_model.get_weights()
    logging.warning('weights %s', weights)

    logging.warning('train %s, val %s', train_size, val_size)
    val_predict = np.reshape(weather_model.predict(val_features), val_size)
    weather.measureTrueFalse(val_labels, val_predict, 0.3)
    weather.measureTrueFalse(val_labels, val_predict, 0.4)
    weather.measureTrueFalse(val_labels, val_predict, 0.5)
    return


if __name__ == "__main__":
    main()
