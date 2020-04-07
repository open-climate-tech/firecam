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

Takes csv export of Fuego images table and push it to sqlite DB

"""

import sys
import os
fuegoRoot = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import db_manager
import collect_args

import time
import csv
import dateutil.parser

manager = db_manager.DbManager(os.path.join(settings.fuegoRoot + 'resources/local.db'))

def insert_entire_images(csvFile):
    csvreader = csv.reader(csvFile)
    for row in csvreader:
        dt = dateutil.parser.parse(row[4])
        unixTime = int(time.mktime(dt.timetuple()))
        parsed = {
            'ImageID': row[0],
            'ImageClass': row[1],
            'FireName': row[2],
            'CameraName': row[3],
            'Timestamp': unixTime,
            'Smoke': row[5],
            'Fog': row[6],
            'Rain': row[7],
            'Glare': row[8],
            'Snow': row[9],
        }
        manager.add_data('images', parsed, commit=False)
    manager.commit()

def insert_cropped_images(csvFile):
    csvreader = csv.reader(csvFile)
    for row in csvreader:
        parsed = {
            'CroppedID': row[0],
            'MinX': int(row[1]),
            'MinY': int(row[2]),
            'MaxX': int(row[3]),
            'MaxY': int(row[4]),
            'EntireImageID': row[5],
        }
        manager.add_data('cropped', parsed, commit=False)
    manager.commit()


def main():
    optArgs = [
        ["e", "entireImage", "csv filename with data on entire images (Fuego Images)"],
        ["c", "croppedImages", "csv filename with data on cropped images (Fuego Cropped Images)"],
    ]
    args = collect_args.collectArgs([], optionalArgs=optArgs)
    if args.entireImage:
        with open(args.entireImage) as csvfile:
            insert_entire_images(csvfile)
    if args.croppedImages:
        with open(args.croppedImages) as csvfile:
            insert_cropped_images(csvfile)


if __name__=="__main__":
    main()
