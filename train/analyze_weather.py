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

Aanlyze ML model for weather data at different thresholds

"""

import os, sys
from firecam.lib import settings
from firecam.lib import weather
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import tf_helper

import logging
import numpy as np

def main():
    reqArgs = [
        ["i", "inputCsv", "csvfile with normalized fire and weather data"],
        ["m", "model", "weather model"],
    ]
    optArgs = [
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    (features, labels) = weather.readWeatherCsv(args.inputCsv)
    labels = np.array(labels)

    weather_model = tf_helper.loadModel(args.model)
    logging.warning('num data %s', len(features))
    predictions = np.reshape(weather_model.predict(features), len(features))

    weather.measureTrueFalse(labels, predictions, 0.25)
    weather.measureTrueFalse(labels, predictions, 0.3)
    weather.measureTrueFalse(labels, predictions, 0.4)
    weather.measureTrueFalse(labels, predictions, 0.5)
    return


if __name__ == "__main__":
    main()
