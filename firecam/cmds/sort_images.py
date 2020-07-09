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

Displays all images in a folder or zip file for users to mark out smoke bounding box.
The coordinates are uploaded to google cloud function.

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.image_crop import crop_single

import zipfile
import tempfile
import pathlib
import logging
import shutil
import requests

from tkinter.filedialog import askdirectory

def unzipFile(zipFile):
    tempDir = tempfile.TemporaryDirectory()
    print('tempDir', tempDir.name)
    zip_ref = zipfile.ZipFile(zipFile, "r")
    zip_ref.extractall(tempDir.name)
    return tempDir


def uploadCoords(coords, newPath, googleServices, notes):
    token = goog_helper.getIdToken(googleServices, settings.gcfLabelUrl)
    headers = {'Authorization': 'bearer {}'.format(token)}
    imgName = pathlib.PurePath(newPath).name
    gcfParams = {
        'type': 'bbox',
        'fileName': imgName,
        'minX': coords[0],
        'minY': coords[1],
        'maxX': coords[2],
        'maxY': coords[3],
        'notes': notes,
    }
    rawResponse = requests.post(settings.gcfLabelUrl, headers=headers, data=gcfParams)
    response = rawResponse.content.decode()
    if response != 'done':
        raise ValueError('Failed to upload to cloud (%s, %s).  Please retry' % (response, rawResponse))


def processFolder(imgDirectory, googleServices, notes):
    temporaryDir = tempfile.TemporaryDirectory()
    imageFileNames = os.listdir(imgDirectory)
    # print('images', len(imageFileNames), imageFileNames)
    # discard files that don't match the expected file name pattern (e.g. .DS_Store)
    imageFileNames = list(filter(img_archive.parseFilename, imageFileNames))
    # print('images2', len(imageFileNames), imageFileNames)
    # we want to process in time order, so first create tuples with associated time
    tuples=list(map(lambda x: (x,img_archive.parseFilename(x)['unixTime']), imageFileNames))
    for tuple in sorted(tuples, key=lambda x: x[1]):
        imgName=tuple[0]
        imgPath = os.path.join(imgDirectory, imgName)
        nameParsed = img_archive.parseFilename(imgName)
        assert nameParsed['cameraID']
        result = crop_single.imageDisplay(imgPath, temporaryDir.name)
        if len(result) > 0:
            for entry in result:
                print('crop data', entry['coords'])
                uploadCoords(entry['coords'], imgName, googleServices, notes)

def main():
    reqArgs = [
    ]
    optArgs = [
        ["n", "notes", "(optional) notes/comments (e.g., test) to associate with data"],
        ["z", "zipFile", "Name of the zip file containing the images"],
        ["d", "imgDirectory", "Name of the directory containing the images or ask:dir"],
    ]
    args = collect_args.collectArgs(reqArgs,  optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    imgDirectory = None
    if args.imgDirectory:
        imgDirectory = args.imgDirectory
        if imgDirectory == 'ask:dir':
            imgDirectory = askdirectory()
    elif args.zipFile:
        tempDir = unzipFile(args.zipFile)
        imgDirectory = tempDir.name

    if not imgDirectory:
        logging.error('Must specify either zipFile or imgDirectory')
        exit(1)

    googleServices = goog_helper.getGoogleServices(settings, args)
    processFolder(imgDirectory, googleServices, args.notes)


if __name__=="__main__":
    main()
