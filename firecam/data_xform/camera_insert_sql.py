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

Take the output of hpwren_kml_parse.py and write the data to 'cameras' table in sqlite DB

"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import db_manager

import datetime
import ast

fileName = '../cameras-hpwren.txt'
manager = db_manager.DbManager(os.path.join(settings.fuegoRoot, 'resources/local.db'))

lineNumber = 1
skipped=[]
with open(fileName, 'r') as myfile:
    for line in myfile:
        # print("raw", line)
        parsed = ast.literal_eval(line)
        # print("parsed", parsed)
        parsed.pop('urls', None)
        # print("parsed2", lineNumber, parsed)
        lineNumber += 1
        manager.add_data('cameras', parsed)


print(skipped)
