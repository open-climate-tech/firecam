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

Download, upload, list, or remove files from google drive

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import os
fuegoRoot = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(fuegoRoot, 'lib'))
sys.path.insert(0, fuegoRoot)
import settings
settings.fuegoRoot = fuegoRoot
import collect_args
import goog_helper

import logging
import time

# function for handling failures from batch deletion
def delete_file(request_id, response, exception):
    if exception is not None:
        logging.error('Encountered error %d: %s', delete_file.numErrors, str(exception))
        delete_file.numErrors += 1
        time.sleep(1) # the errors are usually about rate limits, so slowing down to reduce errors
delete_file.numErrors = 0


def main():
    reqArgs = [
        ["d", "dirID", "ID of google drive directory"],
        ["f", "fileName", "fileName of google drive file"],
    ]
    optArgs = [
        ["u", "upload", "(optional) performs upload vs. download"],
        ["s", "startTime", "(optional) performs search with modifiedTime > startTime"],
        ["e", "endTime", "(optional) performs search with modifiedTime < endTime"],
        ["l", "listOnly", "(optional) list vs. download"],
        ["r", "remove", "(optional) performs remove/delete vs. download (value must be 'delete')"],
        ["m", "maxFiles", "override default of 100 for max number of files to operate on"],
    ]
    
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    maxFiles = int(args.maxFiles) if args.maxFiles else 100
    googleServices = goog_helper.getGoogleServices(settings, args)

    # default mode is to download a single file
    operation = 'download'
    multipleFiles = False
    batchMode = True
    MAX_BATCH_SIZE = 25  # increasing size beyond 60 tends to generate http 500 errors

    if args.upload:
        operation = 'upload'
    elif args.remove:
        if args.remove != 'delete':
            logging.error("value for remove must be 'delete', but instead is %s", args.remove)
            exit(1)
        operation = 'delete'
    elif args.listOnly:
        operation = 'list'

    if args.startTime or args.endTime:
        multipleFiles = True

    if not multipleFiles:
        if operation == 'upload':
            goog_helper.uploadFile(googleServices['drive'], args.dirID, args.fileName)
        else:
            assert operation == 'download'
            goog_helper.downloadFile(googleServices['drive'], args.dirID, args.fileName, args.fileName)
    else:
        nextPageToken = 'init'
        processedFiles = 0
        while True:
            batch = None
            batchCount = 0

            (items, nextPageToken) = goog_helper.searchFiles(googleServices['drive'], args.dirID, args.startTime, args.endTime, args.fileName, npt=nextPageToken)
            firstLast = ''
            if len(items) > 0:
                firstLast = str(items[0]) + ' to ' + str(items[-1])
            logging.warning('Found %d files: %s', len(items), firstLast)

            if operation == 'list':
                logging.warning('All files: %s', items)
            for item in items:
                if operation == 'delete':
                    if batchMode:
                        if not batch:
                            batch = googleServices['drive'].new_batch_http_request(callback = delete_file)
                        batch.add(googleServices['drive'].files().delete(fileId=item["id"], supportsTeamDrives=True))
                        batchCount += 1
                        if batchCount == MAX_BATCH_SIZE:
                            logging.warning('Execute batch with %d items', batchCount)
                            batch.execute()
                            batch = None
                            batchCount = 0
                    else:
                        googleServices['drive'].files().delete(fileId=item["id"], supportsTeamDrives=True).execute()
                elif operation == 'download':
                    goog_helper.downloadFileByID(googleServices['drive'], item['id'], item['name'])
            if batch and batchCount:
                batch.execute()
                batch = None
            processedFiles += len(items)
            logging.warning('Processed %d of max %d. NextToken: %s', processedFiles, maxFiles, bool(nextPageToken))
            if (processedFiles >= maxFiles) or not nextPageToken:
                break # exit if we processed enough files or no files left
        logging.warning('Done')

if __name__=="__main__":
    main()
