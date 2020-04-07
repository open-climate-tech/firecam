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

Take the output of fire_coords.py and write the data to 'fires' table in sqlite DB
With the fire and camera data in the DB, matches can be found with following query:

select fires.name, (fires.Latitude-cameras.Latitude)*(fires.Latitude-cameras.Latitude)+(fires.Longitude-cameras.Longitude)*(fires.Longitude-cameras.Longitude) as distance, fires.Started, fires.Updated, fires.Url, fires.Location, fires.County, fires.Latitude, fires.Longitude, cameras.Name, cameras.Latitude, cameras.Longitude from fires cross join cameras where fires.started is not null and distance < 0.02 order by distance;

version 2
HPWREN with IDs:
select fires.name, round((fires.Latitude-cameras.Latitude)*(fires.Latitude-cameras.Latitude)+(fires.Longitude-cameras.Longitude)*(fires.Longitude-cameras.Longitude),4) as distance, fires.Started, cameras.cameraIDs, case when fires.latitude > cameras.latitude then 'north' else 'south' end, round(fires.latitude - cameras.latitude,3), case when fires.longitude > cameras.longitude then 'east' else 'west' end, round(fires.longitude - cameras.longitude, 3) from fires cross join cameras where fires.started is not null and fires.adminunit is not 'MVU' and fires.adminunit is not 'NEU' and cameras.network='HPWREN' and distance < 0.08 order by distance;
"""

import os
import sys
fuegoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper
import db_manager

import datetime
import ast
import sys


def insertFires(dbManager, fileName):
    lineNumber = 1
    skipped=[]
    with open(fileName, 'r') as myfile:
        for line in myfile:
            # print("raw", line)
            parsed = ast.literal_eval(line)
            # print("parsed", parsed)
            url = parsed.get('href')
            if (url != None):
                parsed['url'] = url
            parsed.pop('href', None)
            parsed.pop('Extra', None)
            # print("parsed2", lineNumber, parsed)
            lineNumber += 1
            dbManager.add_data('fires', parsed)

    print('Skipped:', skipped)


def main():
    reqArgs = [
        ["f", "fileName", "name of file containing fire_coords.py output"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file)
    insertFires(dbManager, args.fileName)


if __name__=="__main__":
    main()
