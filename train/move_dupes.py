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

Moves duplicate files to given directory

"""

import os, sys
from firecam.lib import collect_args
from firecam.lib import goog_helper

import re
import shutil

def checkDupes(fileName, destDir):
    lineNumber = 0
    skipped=[]
    lastSum = None
    lastImg = None
    with open(fileName, 'r') as myfile:
        for line in myfile:
            lineNumber += 1
            # print("raw", line)
            regex = '^(\S+) \*(\S+\.jpg)'
            matches = re.findall(regex, line)
            if len(matches) != 1:
                print('Skipping line', lineNumber, line)
                skipped.append(line)
                continue
            (sum,imgName) = matches[0]
            if sum != lastSum:
                lastSum = sum
                lastImg = imgName
                continue
            # duplicate
            shutil.move(imgName, os.path.join(destDir, imgName))
            print('Found duplicate', lineNumber, imgName, lastImg, sum)

    print('Skipped:', skipped)


def main():
    reqArgs = [
        ["f", "fileName", "name of file containing 'md5sum |sort' output "],
        ["d", "destDir", "name of directory where to move dupes"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
    checkDupes(args.fileName, args.destDir)


if __name__=="__main__":
    main()
