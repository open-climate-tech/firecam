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

Helper functions for tensorflow

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from firecam.lib import goog_helper

import logging
import tempfile
import tensorflow as tf

def loadModel(modelPath):
    """Load from given keras model

    Args:
        modelPath (str): path to model dir

    Returns:
        Model object
    """
    # if model is on GCS, download it locally first
    gcsModel = goog_helper.parseGCSPath(modelPath)
    if gcsModel:
        tmpDir = tempfile.TemporaryDirectory()
        goog_helper.downloadBucketDir(gcsModel['bucket'], gcsModel['name'], tmpDir.name)
        localPath = tmpDir.name
    else:
        localPath = modelPath
    return tf.keras.models.load_model(localPath)


def classifySegments(model, cropsNormalized, segments):
    """Classify even segment with given model.  Segments are specified by two parallel list
       (one with raw data, other with metadata)

    Args:
        model: model object from loadModel call above
        cropsNormalized (list): list of np arrays containing normalized image data
        segments (list): parallel list of metadata associated with each cropNormalized array

    Returns:
        list of results of classification
    """
    # assuming crops is alrady normalized (done by cutBoxesArray)
    results = model.predict(cropsNormalized)
    # logging.warning('Results: %s', str(results))
    for i,scores in enumerate(results):
        segments[i]['score'] = scores[1]
    return results


# "frozen" variant is experimental code
def _wrap_frozen_graph(graph_def, inputs, outputs):
    def _imports_graph_def():
        tf.compat.v1.import_graph_def(graph_def, name="")
    wrapped_import = tf.compat.v1.wrap_function(_imports_graph_def, [])
    import_graph = wrapped_import.graph
    return wrapped_import.prune(
        tf.nest.map_structure(import_graph.as_graph_element, inputs),
        tf.nest.map_structure(import_graph.as_graph_element, outputs))


def loadFrozenModelTf2(model_file):
    with tf.io.gfile.GFile(model_file, "rb") as f:
        graph_def = tf.compat.v1.GraphDef()
        loaded = graph_def.ParseFromString(f.read())

    # Wrap frozen graph to ConcreteFunctions
    frozen_func = _wrap_frozen_graph(graph_def=graph_def,
                                    inputs=["x:0"],
                                    outputs=["Identity:0"])
    return frozen_func


def classifyFrozenTf2(model, cropsNormalized, segments):
    cropsNormalized = tf.convert_to_tensor(cropsNormalized)
    results = model(x=cropsNormalized)
    # logging.warning('Results: %s', str(results))
    for i,scores in enumerate(results[0]):
        segments[i]['score'] = scores[1].numpy()
    return results


def safeDiv(dividend, divisor):
    if divisor == 0:
        return 0
    return dividend / divisor


def confusionStats(truePositive, trueNegative, falsePositive, falseNegative):
    precision = safeDiv(truePositive, truePositive + falsePositive)
    recall = safeDiv(truePositive, truePositive + falseNegative)
    f1 = safeDiv(2 * precision*recall, precision + recall)
    accuracy = safeDiv(truePositive + trueNegative, truePositive + trueNegative + falsePositive + falseNegative)
    return (precision, recall, f1, accuracy)
