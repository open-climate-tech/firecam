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
Helper functions for google cloud APIs (auth, GCS, pubsub)
"""

import os, sys
from firecam.lib import settings

import re
import shutil
import pathlib
import logging
import time, datetime, dateutil.parser
import json
import requests

from oauth2client import file, client, tools

from google.cloud import storage
from google.cloud import pubsub_v1

from google.auth.transport.requests import Request
import google.oauth2.id_token

# If modifying these scopes, delete the file token.json.
# TODO: This is getting too big.  We should ask for different subsets for each app
SCOPES = [
    # 'https://www.googleapis.com/auth/gmail.send',
    'email', # to check any malicious activity in GCF callers
    'profile' # to get id_token for GCF calls
]


def getCreds(clientCredsFile, userTokenFile, args):
    """Get Google credentials (access token and ID token) and refresh if needed

    Args:
        clientCredsFile (str): path to file containing client/app credentials
        userTokenFile (str): path to file containing user credentials
        args: arguments associated with credentials

    Returns:
        Google credentials object
    """
    store = file.Storage(userTokenFile)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(clientCredsFile, ' '.join(SCOPES))
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
    try:
        creds = getCreds(settings.googleCredsFile, settings.googleTokenFile, args)
    except Exception as e:
        logging.warning('Error fetching oauth user credentials. %s', str(e))
        creds = None
    return {
        # 'mail': build('gmail', 'v1', http=creds.authorize(Http())),
        'creds': creds,
        'time': time.time()
    }


def getServiceIdToken(audience):
    """Get ID token for service account for given audience.  Caches the value per audience for performance

    Args:
        audience (str): target audience (e.g. cloud function URL)

    Returns:
        token string
    """
    if audience in getServiceIdToken.cached:
        return getServiceIdToken.cached[audience]
    getServiceIdToken.cached[audience] = google.oauth2.id_token.fetch_id_token(Request(), audience)
    return getServiceIdToken.cached[audience]
getServiceIdToken.cached = {}


def getIdToken(googleServices, url, forceRefresh=False):
    """Get an ID token usable for GCF calls

    Args:
        googleServices (): Google services and credentials

    Returns:
        ID token string
    """
    if googleServices['creds']:
        creds = googleServices['creds']
        if forceRefresh or ((time.time() - googleServices['time']) > 30*60):  # refresh access token if older than half hour
            logging.warning('Refreshing access token')
            creds.get_access_token()
            googleServices['time'] = time.time()
        token = creds.id_token_jwt
    else:
        token = getServiceIdToken(url)
    return token


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
        name = matches[0][1]
        if name[-1] == '/': # strip trailing slash
            name = name[0:-1]
        return {
            'bucket': matches[0][0],
            'name': name,
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
    """Get an authenticated GCS client (caches result for performance)

    Returns:
        Authenticated GCP Storage client
    """
    if getStorageClient.cachedClient:
        return getStorageClient.cachedClient
    if getattr(settings, 'gcpServiceKey', None):
        storageClient = storage.Client.from_service_account_json(settings.gcpServiceKey)
    else:
        try:
            storageClient = storage.Client()
        except Exception as e:
            logging.warning('Using anon GCS access')
            storageClient = storage.Client.create_anonymous_client()

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


def firstItem(iter):
    for x in iter:
        return x


def listBucketEntries(bucketName, prefix='', getDirs=False, deep=False):
    """List all files or dirs in given Google Cloud Storage bucket matching given prefix, getDirs, deep

    Args:
        bucketName (str): Cloud Storage bucket name
        prefix (str): optional string that must be at start of filename
        getDirs (bool): if true, return all subdirs vs. files in given prefix
        deep (bool): if true, return all files in "deeply" nested "folders"

    Returns:
        List of file names (note names are full paths in cloud storage)
    """
    storageClient = getStorageClient()
    if getDirs:
        assert not deep # can't combine directory listen with deep traversal
    delimiter = '' if deep else '/'
    blobs = storageClient.list_blobs(bucketName, prefix=prefix, delimiter=delimiter)
    if getDirs:
        firstItem(blobs) # for some reason 'prefixes' is not filled until iterator is started
        return [prefix[0:-1] for prefix in blobs.prefixes] # remove the trailing '/'
    else:
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


def downloadBucketDir(bucketName, dirID, localDirPath):
    """Recursively download all files in given bucket/dirID into local directry with given path

    Args:
        bucketName (str): Cloud Storage bucket name
        dirID (str): dir path inside bucket
        localDirPath (str): path to local directry where to store the data
    """
    if not os.path.exists(localDirPath):
        os.makedirs(localDirPath)
    # ensure trailing /
    if dirID[-1] != '/':
        dirID += '/'
    # download files at current directory level
    files = listBucketEntries(bucketName, prefix=dirID)
    for f in files:
        name = f.split('/')[-1]
        localFilePath = os.path.join(localDirPath, name)
        downloadBucketFile(bucketName, f, localFilePath)
    # recursively download directories
    dirs = listBucketEntries(bucketName, prefix=dirID, getDirs=True)
    for d in dirs:
        name = d.split('/')[-1]
        nextPath = os.path.join(localDirPath, name)
        downloadBucketDir(bucketName, d, nextPath)


def dateSubDir(parentPath):
    """Return a directory path under given parentPath with todays date as subdir.
       Also add a subdir level for year

    Args:
        parentPath (str): path under which to add date subdir

    Returns:
        directory path
    """
    dateSubdir = datetime.datetime.fromtimestamp(time.time()).strftime('%Y/%Y-%m-%d')
    if parentPath[-1] == '/':
        fullPath = parentPath + dateSubdir
    else:
        fullPath = parentPath + '/' + dateSubdir
    return fullPath


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
    """Copy given local source file to given destination directory (possibly on GCS or local path)

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


def getPubsubClient():
    """Get an authenticated GCP pubsub client (caches result for performance)

    Returns:
        Authenticated GCP pubsub client
    """
    if getPubsubClient.cachedClient:
        return getPubsubClient.cachedClient
    if settings.gcpServiceKey:
        pubsubClient = pubsub_v1.PublisherClient.from_service_account_json(settings.gcpServiceKey)
    else:
        pubsubClient = pubsub_v1.PublisherClient()
    getPubsubClient.cachedClient = pubsubClient
    return pubsubClient
getPubsubClient.cachedClient = None


def publish(data):
    """Publish given data wrapped as JSON on GCP pubsub topic

    Args:
        msg (str): message data

    Returns:
        pubsub result - message ID
    """
    if not settings.pubsubTopic:
        return

    pubsubClient = getPubsubClient()
    topic_path = pubsubClient.topic_path(settings.gcpProject, settings.pubsubTopic)
    future = pubsubClient.publish(topic_path, json.dumps(data).encode('utf-8'))
    return future.result()


def getInstanceGroup():
    """Get the GCP managed instance group that created this VM instance

    Returns:
        string ID Of the instance group
    """
    metadata_server = "http://metadata/computeMetadata/v1/instance/"
    metadata_flavor = {'Metadata-Flavor' : 'Google'}
    groupName = None
    try:
        groupName = requests.get(metadata_server + 'attributes/created-by', headers = metadata_flavor).text
    except Exception as e:
        logging.warning('Error fetching GCP MIG name. %s', str(e))
    return groupName
