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

Take the output of calfire_parse.py and send the location information to
google's geocoding API to get the latitude and longitude.  Then output
the augmented data.

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

import ast
import googlemaps

def getCoords(gmaps, fileName):
    lineNumber = 1
    skipped=[]
    with open(fileName, 'r') as myfile:
        for line in myfile:
            # print("raw", line)
            parsed = ast.literal_eval(line)
            # print("parsed", parsed['Name'], parsed['Location'] + ',' + parsed['County'])
            geocode_result = gmaps.geocode(parsed['Location'] + ',' + parsed['County'])
            # print(geocode_result)
            if (len(geocode_result) != 0):
                parsed['Latitude'] = geocode_result[0]['geometry']['location']['lat']
                parsed['Longitude'] = geocode_result[0]['geometry']['location']['lng']
            else:
                skipped.append([lineNumber, parsed['Name']])
            print(parsed)
            lineNumber += 1

    print('Skipped:', skipped)


def main():
    reqArgs = [
        ["k", "key", "api key for google geocoding service"],
        ["f", "fileName", "name of file containing calfire_parse.py output"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
    gmaps = googlemaps.Client(key=args.key)
    getCoords(gmaps, args.fileName)


if __name__=="__main__":
    main()
