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
Helper functions for google cloud APIs (drive, sheets)
"""

import os, sys
from firecam.lib import settings
from firecam.lib import img_archive

import re
import io
import shutil
import pathlib
import logging
import time, datetime, dateutil.parser

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from apiclient.http import MediaIoBaseDownload
from apiclient.http import MediaFileUpload

from google.cloud import storage

# If modifying these scopes, delete the file token.json.
# TODO: This is getting too big.  We should ask for different subsets for each app
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/gmail.send',
    'profile' # to get id_token for gcf_ffmpeg
]


def getCreds(settings, args):
    """Get Google credentials (access token and ID token) and refresh if needed

    Args:
        settings: settings module with pointers to credential files
        args: arguments associated with credentials

    Returns:
        Google credentials object
    """
    store = file.Storage(settings.googleTokenFile)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(settings.googleCredsFile, ' '.join(SCOPES))
        creds = tools.run_flow(flow, store, args)
    creds.get_access_token() # refresh access token if expired
    return creds


def getGoogleServices(settings, args):
    """Get Google services for drive and sheet, and the full credentials

    Args:
        settings: settings module with pointers to credential files
        args: arguments associated with credentials

    Returns:
        Dictionary with service tokens
    """
    creds = getCreds(settings, args)
    return {
        'drive': build('drive', 'v3', http=creds.authorize(Http())),
        'sheet': build('sheets', 'v4', http=creds.authorize(Http())),
        'storage': build('storage', 'v1', http=creds.authorize(Http())),
        'mail': build('gmail', 'v1', http=creds.authorize(Http())),
        'creds': creds
    }


def createFolder(service, parentDirID, folderName):
    """Create Google drive folder with given name in given parent folder

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        parentDirID (str): Drive folder ID of parent where to create new folder
        folderName (str): Name of new folder

    Returns:
        Drive folder ID of newly created folder or None (on failure)
    """
    file_metadata = {
        'name': folderName,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parentDirID]
    }
    retriesLeft = 5
    while retriesLeft > 0:
        retriesLeft -= 1
        try:
            folder = service.files().create(body=file_metadata,
                                            supportsTeamDrives=True,
                                            fields='id').execute()
            return folder['id']
        except Exception as e:
            logging.warning('Error creating folder %s. %d retries left. %s', folderName, retriesLeft, str(e))
            if retriesLeft > 0:
                time.sleep(5) # wait 5 seconds before retrying
    logging.error('Too many create folder failures')
    return None


def deleteItem(service, itemID):
    """Delete Google drive folder or file with given ID

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        itemID (str): Drive ID of item to be deleted

    Returns:
        Drive API response
    """
    return service.files().delete(fileId=itemID, supportsTeamDrives=True).execute()


def driveListFilesQueryWithNextToken(service, parentID, customQuery=None, pageToken=None):
    """Internal function to search items in drive folders

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        parentID (str): Drive folder ID of parent where to search
        customQuery (str): optional custom query parameters
        pageToken (str): optional page token for paging results of large sets

    Returns:
        Tuple (items, nextPageToken) containing the items found and pageToken to retrieve
        remaining data
    """
    param = {}
    param['q'] = "'" + parentID + "' in parents and trashed = False"
    if customQuery:
        param['q'] += " and " + customQuery
    param['fields'] = 'nextPageToken, files(id, name)'
    param['pageToken'] = pageToken
    param['supportsTeamDrives'] = True
    param['includeTeamDriveItems'] = True
    # print(param)
    retriesLeft = 5
    while retriesLeft > 0:
        retriesLeft -= 1
        try:
            results = service.files().list(**param).execute()
            items = results.get('files', [])
            nextPageToken = results.get('nextPageToken')
            # print('Files: ', items)
            return (items, nextPageToken)
        except Exception as e:
            logging.warning('Error listing drive. %d retries left. %s', retriesLeft, str(e))
            if retriesLeft > 0:
                time.sleep(5) # wait 5 seconds before retrying
    logging.error('Too many list failures')
    return None


def driveListFilesQuery(service, parentID, customQuery=None):
    # Simple wrapper around driveListFilesQueryWithNextToken without page token
    (items, nextPageToken) = driveListFilesQueryWithNextToken(service, parentID, customQuery)
    return items


def driveListFilesByName(service, parentID, searchName=None):
    # Wrapper around driveListFilesQuery to search for items with given name
    if searchName:
        customQuery = "name = '" + searchName + "'"
    else:
        customQuery = None
    return driveListFilesQuery(service, parentID, customQuery)


def searchFiles(service, parentID, minTime=None, maxTime=None, prefix=None, npt=None):
    """Search for items in drive folder with given name and time range

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        parentID (str): Drive folder ID of parent where to search
        minTime (str): optional ISO datetime that items must be modified after
        maxTime (str): optional ISO datetime that items must be modified before
        prefix (str): optional string that must be part of the name
        npt (str): optional page token for paging results of large sets

    Returns:
        Tuple (items, nextPageToken) containing the items found and pageToken to retrieve
        remaining data
    """
    constraints = []
    if minTime:
        constraints.append(" modifiedTime > '" + minTime + "' ")
    if maxTime:
        constraints.append(" modifiedTime < '" + maxTime + "' ")
    if prefix:
        constraints.append(" name contains '" + prefix + "' ")
    customQuery = ' and '.join(constraints)
    # logging.warning('Query %s', customQuery)
    if npt:
        if npt == 'init': # 'init' is special value to indicate desire to page but with exiting token
            npt = None
        return driveListFilesQueryWithNextToken(service, parentID, customQuery, npt)
    else:
        return driveListFilesQuery(service, parentID, customQuery)


def searchAllFiles(service, parentID, minTime=None, maxTime=None, prefix=None):
    # Wrapper around searchFiles that will iterate over all pages to retrieve all items
    allItems = []
    nextPageToken = 'init'
    while nextPageToken:
        (items, nextPageToken) = searchFiles(service, parentID, minTime, maxTime, prefix, nextPageToken)
        allItems += items
    return allItems


def downloadFileByID(service, fileID, localFilePath):
    """Download Googld drive file given ID and save to given local filePath

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        fileID (str): drive ID for file
        localFilePath (str): path to local file where to store the data
    """
    # download file from drive to memory object
    request = service.files().get_media(fileId=fileID)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download", int(status.progress() * 100))

    # store memory object data to local file
    fh.seek(0)
    with open(localFilePath, 'wb') as f:
        shutil.copyfileobj(fh, f)


def downloadFile(service, dirID, fileName, localFilePath):
    """Download Googld drive file given folder ID and file nam and save to given local filePath

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        dirID (str): drive ID for folder containing file
        fileName (str): filename of file in drive folder
        localFilePath (str): path to local file where to store the data
    """
    files = driveListFilesByName(service, dirID, fileName)
    if len(files) != 1:
        print('Expected 1 file but found', len(files), files)
    if len(files) < 1:
        exit(1)
    fileID = files[0]['id']
    fileName = files[0]['name']
    print(fileID, fileName)

    downloadFileByID(service, fileID, localFilePath)


def uploadFile(service, dirID, localFilePath):
    """Upload file to to given Google drive folder ID

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        dirID (str): destination drive ID of folder
        localFilePath (str): path to local file where to read the data from

    Returns:
        Drive API upload result
    """
    file_metadata = {'name': pathlib.PurePath(localFilePath).name, 'parents': [dirID]}
    media = MediaFileUpload(localFilePath, mimetype = 'image/jpeg')
    retriesLeft = 5
    while retriesLeft > 0:
        retriesLeft -= 1
        try:
            file = service.files().create(body=file_metadata,
                                            media_body=media,
                                            supportsTeamDrives=True,
                                            fields='id').execute()
            return file
        except Exception as e:
            logging.warning('Error uploading image %s. %d retries left. %s', localFilePath, retriesLeft, str(e))
            if retriesLeft > 0:
                time.sleep(5) # wait 5 seconds before retrying
    logging.error('Too many upload failures')
    return None


def getDirForClassCamera(service, classLocations, imgClass, cameraID):
    """Find Google drive folder ID & name for given camera in Fuego Cropping/Pictures/<imgClass> folder

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        classLocations (dict): Dict of immgClass -> drive folder ID
        imgClass (str): image class (smoke, nonSmoke, etc..)
        cameraID (str): ID of the camera

    Returns:
        Tuple (ID, name) containing the folder ID and name
    """
    parent = classLocations[imgClass]
    dirs = driveListFilesByName(service, parent, cameraID)
    if len(dirs) != 1:
        logging.error('Expected 1 directory with name %s, but found %d: %s', cameraID, len(dirs), dirs)
        logging.error('Searching in dir: %s', parent)
        logging.error('ImgClass %s, locations: %s', imgClass, classLocations)
        raise Exception('getDirForClassCam: Directory not found')
    dirID = dirs[0]['id']
    dirName = dirs[0]['name']
    return (dirID, dirName)


def downloadClassImage(service, classLocations, imgClass, fileName, outputDirectory):
    """Download image file with given name from given image class from Fuego Cropping/Pictures/<imgClass> folder

    Args:
        service: Drive service (from getGoogleServices()['drive'])
        classLocations (dict): Dict of immgClass -> drive folder ID
        imgClass (str): image class (smoke, nonSmoke, etc..)
        fileName (str): Name of image file
        outputDirectory (str): Local directory where to store the file

    Returns:
        Local file system path to downloaded file
    """
    localFilePath = os.path.join(outputDirectory, fileName)
    if os.path.isfile(localFilePath):
        return localFilePath # already downloaded, nothing to do

    # parse cameraID from fileName
    parsed = img_archive.parseFilename(fileName)
    cameraID = parsed['cameraID']

    # find subdir for camera
    (dirID, dirName) = getDirForClassCamera(service, classLocations, imgClass, cameraID)

    # find file in camera subdir
    downloadFile(service, dirID, fileName, localFilePath)
    return localFilePath


def readFromSheet(service, sheetID, cellRange):
    """Read data from Google sheet for given cell range

    Args:
        service: Google sheet service (from getGoogleServices()['sheet'])
        sheetID (str): Google sheet ID
        cellRange (str): Cell Range (e.g., A1:B3) to read

    Returns:
        Values from the sheet
    """
    result = service.spreadsheets().values().get(spreadsheetId=sheetID,
                                                range=cellRange).execute()
    # print(result)
    values = result.get('values', [])
    return values


def getParentParser():
    """Get the parent argparse object needed by Google APIs
    """
    return tools.argparser



GS_URL_REGEXP = '^gs://([a-z0-9_.-]+)/(.+)$'
def parseGCSPath(path):
    """Parse GCS bucket and path names out of given gs:// style full path

    Args:
        path (str): full path

    Returns:
        Dict with bucket and name
    """
    matches = re.findall(GS_URL_REGEXP, path)
    if matches and (len(matches) == 1):
        return {
            'bucket': matches[0][0],
            'name': matches[0][1],
        }
    return None


def repackGCSPath(bucketName, fileName):
    """Reverse of parseGCSPath() above: package given bucket and file names into GCS name

    Args:
        bucketName (str): Cloud Storage bucket name
        fileName (str): file path inside bucket

    Returns:
        GCS path
    """
    return 'gs://' + bucketName + '/' + fileName


def getStorageClient():
    """Get an authenticated GCS client

    Returns:
        Authenticated GCP Storage client
    """
    if getStorageClient.cachedClient:
        return getStorageClient.cachedClient
    if settings.gcpServiceKey:
        storageClient = storage.Client.from_service_account_json(settings.gcpServiceKey)
    else:
        storageClient = storage.Client()
    getStorageClient.cachedClient = storageClient
    return storageClient
getStorageClient.cachedClient = None


def listBuckets():
    """List all Cloud storage buckets in given client

    Returns:
        List of bucket names
    """
    storageClient = getStorageClient()
    return [bucket.name for bucket in storageClient.list_buckets()]


def listBucketFiles(bucketName, prefix='', deep=False):
    """List all files in given Google Cloud Storage bucket matching given prefix and getDirs

    Args:
        bucketName (str): Cloud Storage bucket name
        prefix (str): optional string that must be at start of filename
        deep (bool): if true, return all files in "deeply" nested "folders"

    Returns:
        List of file names (note names are full paths in cloud storage)
    """
    storageClient = getStorageClient()
    delimiter = '' if deep else '/'
    blobs = storageClient.list_blobs(bucketName, prefix=prefix, delimiter=delimiter)
    return [blob.name for blob in blobs]


def getBucketFile(bucketName, fileID):
    """Get given file from given GCS bucket

    Args:
        bucketName (str): Cloud Storage bucket name
        fileID (str): file path inside bucket
    """
    storageClient = getStorageClient()
    bucket = storageClient.bucket(bucketName)
    blob = bucket.blob(fileID)
    return blob


def readBucketFile(bucketName, fileID):
    """Read contents of the given file in given bucket

    Args:
        bucketName (str): Cloud Storage bucket name
        fileID (str): file path inside bucket

    Returns:
        string content of the file
    """
    blob = getBucketFile(bucketName, fileID)
    return blob.download_as_string().decode()


def downloadBucketFile(bucketName, fileID, localFilePath):
    """Download the given file in given bucket into local file with given path

    Args:
        bucketName (str): Cloud Storage bucket name
        fileID (str): file path inside bucket
        localFilePath (str): path to local file where to store the data
    """
    if os.path.isfile(localFilePath):
        return # already downloaded, nothing to do

    blob = getBucketFile(bucketName, fileID)
    blob.download_to_filename(localFilePath)


def uploadBucketFile(bucketName, fileID, localFilePath):
    """Upload the given file to given bucket

    Args:
        bucketName (str): Cloud Storage bucket name
        fileID (str): file path inside bucket
        localFilePath (str): path to local file where to read the data from
    """
    blob = getBucketFile(bucketName, fileID)
    blob.upload_from_filename(localFilePath)


def deleteBucketFile(bucketName, fileID):
    """Delete the given file from given bucket

    Args:
        bucketName (str): Cloud Storage bucket name
        fileID (str): file path inside bucket
    """
    blob = getBucketFile(bucketName, fileID)
    blob.delete()


def readFile(filePath):
    """Read contents of the given file (possibly on GCS or local path)

    Args:
        filePath (str): file path

    Returns:
        string content of the file
    """
    parsedPath = parseGCSPath(filePath)
    dataStr = ''
    if parsedPath:
        dataStr = readBucketFile(parsedPath['bucket'], parsedPath['name'])
    else:
        with open(filePath, "r") as fh:
            dataStr = fh.read()
    return dataStr


def copyFile(srcFilePath, destDir):
    """Copy given local source file to given destion directory (possibly on GCS or local path)

    Args:
        srcFilePath (str): local source file path
        destDir (str): destination file path (local or GCS)
    """
    parsedPath = parseGCSPath(srcFilePath)
    assert not parsedPath # srcFilePath must be local
    parsedPath = parseGCSPath(destDir)
    srcFilePP = pathlib.PurePath(srcFilePath)
    if parsedPath:
        if parsedPath['name'][-1] == '/':
            gcsName = parsedPath['name'] + srcFilePP.name
        else:
            gcsName = parsedPath['name'] + '/' + srcFilePP.name
        uploadBucketFile(parsedPath['bucket'], gcsName, srcFilePath)
        destPath = repackGCSPath(parsedPath['bucket'], gcsName)
    else:
        if not os.path.exists(destDir):
            pathlib.Path(destDir).mkdir(parents=True, exist_ok=True)
        destPath = os.path.join(destDir, srcFilePP.name)
        shutil.copy(srcFilePath, destPath)
    return destPath
