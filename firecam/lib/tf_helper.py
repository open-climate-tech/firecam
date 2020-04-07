# Copyright 2018 The Fuego Authors.
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

import numpy as np
import tensorflow as tf

def load_graph(model_file):
    graph = tf.Graph()
    graph_def = tf.GraphDef()

    with open(model_file, "rb") as f:
        graph_def.ParseFromString(f.read())
    with graph.as_default():
        tf.import_graph_def(graph_def)

    return graph


def load_labels(label_file):
    label = []
    proto_as_ascii_lines = tf.gfile.GFile(label_file).readlines()
    for l in proto_as_ascii_lines:
        label.append(l.rstrip())
    return label

def classifySegments(tfSession, graph, labels, segments):
    input_height = 299
    input_width = 299
    # These commented out values are appropriate for tf_retrain
    # https://github.com/tensorflow/hub/raw/master/examples/image_retraining/retrain.py

    # input_mean = 0
    # input_std = 255
    # input_layer = "Placeholder"
    # output_layer = "final_result"

    # These values we're using now are appropriate for the fine-tuning and full training models
    # https://github.com/tensorflow/models/tree/master/research/slim
    input_mean = 128
    input_std = 128
    input_layer = "input"
    output_layer = "InceptionV3/Predictions/Reshape_1"

    input_name = "import/" + input_layer
    output_name = "import/" + output_layer
    input_operation = graph.get_operation_by_name(input_name)
    output_operation = graph.get_operation_by_name(output_name)

    with tf.Graph().as_default():
        input_name = "file_reader"
        output_name = "normalized"
        file_name_placeholder = tf.placeholder(tf.string, shape=[])
        file_reader = tf.read_file(file_name_placeholder, input_name)
        image_reader = tf.image.decode_jpeg(file_reader, channels=3, name="jpeg_reader",dct_method="INTEGER_ACCURATE")
        float_caster = tf.cast(image_reader, tf.float32)
        dims_expander = tf.expand_dims(float_caster, 0)
        resized = tf.image.resize_bilinear(dims_expander, [input_height, input_width])
        normalized = tf.divide(tf.subtract(resized, [input_mean]), [input_std])

        with tf.Session() as sess:
            for segmentInfo in segments:
                imgPath = segmentInfo['imgPath']
                # print(imgPath)
                t = sess.run(normalized, {file_name_placeholder: imgPath})

                results = tfSession.run(output_operation.outputs[0], {
                    input_operation.outputs[0]: t
                })
                results = np.squeeze(results)

                top_k = results.argsort()[-5:][::-1]
                # for i in top_k:
                #     print(labels[i], results[i])
                smokeIndex = labels.index('smoke')
                # print(imgPath, results[smokeIndex])
                segmentInfo['score'] = results[smokeIndex]
