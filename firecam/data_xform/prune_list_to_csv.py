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

Take the 'ls' output of a pruned crops directory and generate cropped.csv to feed
to recrop_min_size.py

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import img_archive
import logging


def parseToCsv(intput, output, startRow, endRow):
    skipped=[]
    outFile = open(output, 'w')
    with open(intput, 'r') as myfile:
        for (rowIndex, line) in enumerate(myfile):
            if rowIndex < startRow:
                continue
            if rowIndex > endRow:
                logging.warning('Reached end row %d', rowIndex)
                break

            # print("raw", line)
            parsed = img_archive.parseFilename(line)
            if not parsed:
                continue
            # print("parsed", parsed)
            outArray = [
                line.rstrip(),
                str(parsed['minX']),
                str(parsed['minY']),
                str(parsed['maxX']),
                str(parsed['maxY']),
            ]
            del parsed['minX']
            outArray.append(img_archive.repackFileName(parsed))
            # print("parsed2", ','.join(outArray))
            outFile.write(','.join(outArray) + '\n')

    print('Skipped:', skipped)


def main():
    reqArgs = [
        ["i", "intput", "name of intput file containing ls output"],
        ["o", "output", "name of output CSV file"],
    ]
    optArgs = [
        ["s", "startRow", "starting row"],
        ["e", "endRow", "ending row"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    startRow = int(args.startRow) if args.startRow else 0
    endRow = int(args.endRow) if args.endRow else 1e9
    parseToCsv(args.intput, args.output, startRow, endRow)


if __name__=="__main__":
    main()
