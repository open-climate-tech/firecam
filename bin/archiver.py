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

Short-term image archive and periodic tasks

"""

import os, sys

from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper
from firecam.lib import img_archive
from firecam.lib import db_manager

import time, datetime, dateutil.parser
import random
import logging
import threading
from google.cloud import compute_v1

MIN_CYCLE_SECONDS = 13 # some cameras have new views every 13-14 seconds
def chooseCamera(dbManager, cameras, stateless):
    chooseCamera.counter += 1
    if stateless:
        camera = cameras[int(len(cameras)*random.random())]
    else:
        index = chooseCamera.counter % len(cameras)
        camera = cameras[index]
        if index == 0:
            timestamp = int(time.time())
            if timestamp - chooseCamera.cycleTime < MIN_CYCLE_SECONDS:
                logging.warning('Sleeping for %s', MIN_CYCLE_SECONDS - (timestamp - chooseCamera.cycleTime))
                time.sleep(MIN_CYCLE_SECONDS - (timestamp - chooseCamera.cycleTime))
            chooseCamera.cycleTime = int(time.time())
    return camera
chooseCamera.counter = -1
chooseCamera.cycleTime = 0


def getFetchInfo(dbManager, cameraInfo, timestamp):
    sqlTemplate = """SELECT heading,timestamp FROM archive
        where CameraID='%s' and timestamp > %s order by timestamp desc limit 1"""
    sqlStr = sqlTemplate % (cameraInfo['name'], timestamp - 60*60)
    dbResult = dbManager.query(sqlStr)
    # logging.warning('arch dbR %s: %s', len(dbResult), dbResult)
    if len(dbResult) == 0:
        return 0
    fetchTime = dbResult[0]['timestamp']
    return fetchTime


def fetchImage(dbManager, cameraInfo, lastFetchTime, dirName):
    # fetch image to given path
    imgPath = None
    heading = None
    timestamp = None
    try:
        imgInfo = img_archive.fetchImageAndMeta(dbManager, cameraInfo['name'], cameraInfo['url'], dirName, newOnly=True)
        (imgPath, heading, timestamp, fov) = imgInfo
    except Exception as e:
        logging.error('Error fetching image from %s %s', cameraInfo['name'], str(e))
        return
    if lastFetchTime == timestamp:
        # XXX should current timestamp be stored to indicate last time it was checked?
        # XXX filename should be same, so no need to delete
        return
    dbRow = {
        'CameraId': cameraInfo['name'],
        'Heading': heading,
        'Timestamp': timestamp,
        'ImagePath': imgPath,
        'FieldOfView': fov,
        'Processed': 0,
    }
    if imgPath == None or heading == None or timestamp == None:
        logging.error('Image or metadata unavailable for %s', cameraInfo['name'])
        # update timestamp so camera isn't retried for a while
        dbRow['Timestamp'] = int(time.time())
        dbRow['Heading'] = 999
        dbRow['ImagePath'] = ''
        dbRow['FieldOfView'] = 0
    # update DB
    dbManager.add_data('archive', dbRow)
    return


DELETE_CHECK_INTERVAL = 2*60  # 2 minutes
DELETE_AFTER = 1*60*60 # 1 hour
def deleteOldFiles(dbManager):
    timestamp = int(time.time())
    # run every N minutes
    # logging.warning('Delete %s: %s, %s', (timestamp - deleteOldFiles.lastRun) < DELETE_CHECK_INTERVAL, timestamp - deleteOldFiles.lastRun, DELETE_CHECK_INTERVAL)
    if (timestamp - deleteOldFiles.lastRun) < DELETE_CHECK_INTERVAL:
        return
    deleteOldFiles.lastRun = timestamp

    # check DB for old images
    sqlTemplate = """SELECT imagepath FROM archive
        where imagepath != '' and timestamp < %s"""
    sqlStr = sqlTemplate % (timestamp - DELETE_AFTER)
    dbResult = dbManager.query(sqlStr)
    logging.warning('delete dbR %s: top %s', len(dbResult), len(dbResult) and dbResult[0])

    # XXX TODO prevent deletion of images used in positives/detections/alerts
    # delete from archiveDir
    for entry in dbResult:
        try:
            os.remove(entry['imagepath'])
        except Exception as e:
            logging.error('Error deleting %s: %s', entry['imagepath'], str(e))

    # delete from DB
    sqlTemplate = """DELETE FROM archive
        where timestamp < %s"""
    sqlStr = sqlTemplate % (timestamp - DELETE_AFTER)
    dbResult = dbManager.execute(sqlStr)
    return
deleteOldFiles.lastRun = 0


def getTimeType():
    detectStartTime = int(dateutil.parser.parse(settings.detectStartTime).timestamp())
    detectEndTime = int(dateutil.parser.parse(settings.detectEndTime).timestamp())
    # add extra 10 minutes for things to start and shut down properly
    archiveStartTime = detectStartTime - 60 * 10
    archiveEndTime = detectEndTime + 60 * 10
    timeNow = int(time.time())
    if (timeNow > detectStartTime) and (timeNow < detectEndTime):
        type = 'detect'
    elif (timeNow > archiveStartTime) and (timeNow < archiveEndTime):
        type = 'archive'
    else:
        type = 'inactive'
    logging.warning('getTimeType: %s now: %s (%s, %s)', type, datetime.datetime.fromtimestamp(timeNow), settings.detectStartTime, settings.detectEndTime)
    return type


def checkDetectGroup(groupName, numInstances):
    if (not groupName) or (not isinstance(numInstances,int)):
        logging.error('Invalid detect group name (%s) or num (%s)', groupName, numInstances)
        return
    if not checkDetectGroup.igmClient:
        checkDetectGroup.igmClient = compute_v1.services.instance_group_managers.InstanceGroupManagersClient()
    groupData = checkDetectGroup.igmClient.get(project=settings.gcpProject, zone=settings.detectZone, instance_group_manager=groupName)
    logging.warning('DetectGroup %s: Num instances expected (%d), found (%d)', groupName, numInstances, groupData.target_size)
    if numInstances != groupData.target_size:
        checkDetectGroup.igmClient.resize_unary(project=settings.gcpProject, zone=settings.detectZone, instance_group_manager=groupName, size=numInstances)
        logging.warning('DetectGroup resize %s to %d', groupName, numInstances)
checkDetectGroup.igmClient = None


def checkDetectGroups(enableDetect):
    if (not settings.detectZone) or (not settings.detectGroups) or (not settings.detectStartTime) or (not settings.detectEndTime):
        logging.error('Missing detect management settings')
        return
    if (time.time() - checkDetectGroups.lastCheckTime > 5*60): # check at most once every 5 minutes
        # loop through all groups and verify size
        for groupInfo in settings.detectGroups:
            checkDetectGroup(groupInfo[0], groupInfo[1] if enableDetect else 0)
        checkDetectGroups.lastCheckTime = time.time()
    return
checkDetectGroups.lastCheckTime = 0


def checkDailyExit():
    nowDay = datetime.datetime.now().day
    if checkDailyExit.startDay == None:
        checkDailyExit.startDay = nowDay
    if checkDailyExit.startDay != nowDay:
        logging.warning('Nightly exit: %s, %s', checkDailyExit.startDay, nowDay)
        # XXXX TODO delete all old files on storage
        # ensure DB delete would result in 0 files
        # then delete all files not just those in DB.
        exit(1)
checkDailyExit.startDay = None


def queryCount(dbManager, sqlStr):
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) != 1:
        logging.error('Failed to get count %s:  %s', sqlStr, dbResult)
        return 0
    return dbResult[0]['ct']


def updateStats(dbManager):
    # first check if stats already has entry for today
    todayStr = datetime.datetime.now().strftime('%Y-%m-%d')
    sqlTemplate = "SELECT * FROM stats WHERE date='%s'"
    sqlStr = sqlTemplate % (todayStr)
    dbResult = dbManager.query(sqlStr)
    if len(dbResult) != 0:
        logging.error('Stats for %s already in table.  %s', todayStr, dbResult)
        return

    # get stats
    fromTimestamp = int(dateutil.parser.parse(settings.detectStartTime).timestamp())

    # images
    sqlTemplate = "SELECT count(*) as ct FROM (SELECT cameraname, timestamp FROM scores WHERE timestamp > %s group by cameraname,timestamp) as q0"
    images = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # segments
    sqlTemplate = "SELECT count(*) as ct FROM scores WHERE timestamp > %s"
    segments = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # positives
    sqlTemplate = "SELECT count(*) as ct FROM scores WHERE score >.5 and timestamp > %s"
    positiveSegments = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # probables
    sqlTemplate = "SELECT count(*) as ct FROM probables WHERE timestamp > %s and protonum = 0"
    probables = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # detections (isproto < 2 remove prototype models)
    sqlTemplate = "SELECT count(*) as ct FROM detections WHERE timestamp > %s and isproto < 2"
    detections = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # proxy total alerts (currently detections > threshold)
    sqlTemplate = "SELECT count(*) as ct FROM detections WHERE timestamp > %s and weatherscore > %s and isproto < 2"
    alerts = queryCount(dbManager, sqlTemplate % (fromTimestamp, settings.weatherThreshold))

    # prod alerts
    sqlTemplate = "SELECT count(*) as ct FROM alerts WHERE timestamp > %s"
    prodAlerts = queryCount(dbManager, sqlTemplate % fromTimestamp)

    # prod cams
    prodCams = dbManager.get_sources(activeOnly=True, restrictType=settings.prodTypes)
    prodCamsCount = len(prodCams)

    dbRow = {
        'Date': todayStr,
        'Images': images,
        'AllSegments': segments,
        'PositiveSegments': positiveSegments,
        'Probables': probables,
        'Detections': detections,
        'Alerts': alerts,
        'ProdCamsCount': prodCamsCount,
        'ProdAlerts': prodAlerts,
    }
    dbManager.add_data('stats', dbRow)
    logging.warning('Stats inserted for %s: %s', todayStr, dbRow)


def deleteOldScores(dbManager):
    logging.warning('checking old scores for deletion')
    firstTimestamp = int(time.time()-3600*24*7*3) # 3 weeks
    sqlTemplate = "SELECT count(*) as ct FROM scores WHERE timestamp < %s"
    oldEntries = queryCount(dbManager, sqlTemplate % firstTimestamp)
    if oldEntries == 0:
        logging.warning('No old entries.  All done')
        return

    logging.warning('deleting %s old scores', oldEntries)
    sqlTemplate = "DELETE FROM scores WHERE timestamp < %s"
    sqlStr = sqlTemplate % (firstTimestamp)
    dbManager.execute(sqlStr)
    # vacuum and reindex code is here (in case needed in future) but disabled for now
    # System already uses autovacuum, so manual vacuum should not be needed
    # Also scores table should not need daily reindex
    # if settings.psqlHost: # if using postgres
    #     # vacuum and reindex after deletion to improve postgres performance
    #     logging.warning('vacuuming scores')
    #     dbManager.vacuum('scores')
    #     logging.warning('reindexing scores')
    #     sqlStr = "REINDEX INDEX scores_camera_heading_model_time"
    #     dbManager.execute(sqlStr)


def deleteFilesInDir(archiveDir):
    staleFiles = os.listdir(archiveDir)
    logging.warning('deleteFilesInDir: Found %d files', len(staleFiles))
    logging.warning('deleteFilesInDir: Sample name: %s', staleFiles[int(len(staleFiles)*random.random())])
    for name in staleFiles:
        os.remove(os.path.join(archiveDir, name))


def checkDailyPostWork(dbManager, archiveDir):
    detectEndTime = int(dateutil.parser.parse(settings.detectEndTime).timestamp())
    # 80 minutes after detect ends (60 for deleteOldFiles to trigger plus 10 for grace period in getTimeType() plus another 10 for margin)
    postWorkStartTime = detectEndTime + 80*60
    postWorkActive = time.time() > postWorkStartTime
    if checkDailyPostWork.prevActive == None:
        checkDailyPostWork.prevActive = postWorkActive
    logging.warning('checkDailyPostWork %s, %s, %s', postWorkActive and not checkDailyPostWork.prevActive, postWorkActive, checkDailyPostWork.prevActive)
    if postWorkActive and not checkDailyPostWork.prevActive:
        updateStats(dbManager)
        deleteOldScores(dbManager)
        deleteOldFiles(dbManager)
        deleteFilesInDir(archiveDir)
        logging.warning('Daily postWork done')
    checkDailyPostWork.prevActive = postWorkActive
    return
checkDailyPostWork.prevActive = None


def main():
    reqArgs = [
        ["d", "archiveDir", "Archive directory"],
        ["t", "numThreads", "Number of threads", int],
    ]
    optArgs = [
        ["r", "restrictType", "Only process images from cameras of given type"],
        ["n", "noState", "(optional) no changes to state"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    stateless = True if args.noState else False
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    cameras = dbManager.get_sources(activeOnly=True, restrictType=args.restrictType)

    # cameras = cameras[0:4]
    logging.warning('cameras %s: %s', len(cameras), cameras[0:2])

    # images are fetched with multiple threads to overlap network wait time
    def threadFn(threadParams):
        logging.warning('Child thread %s with %d fetches', threading.get_ident(), len(threadParams))
        for (cameraInfo, lastFetchTime) in threadParams:
            fetchImage(dbManager, cameraInfo, lastFetchTime, args.archiveDir)
        logging.warning('Exiting child thread %s', threading.get_ident())

    MAX_INTERVAL_MINUTES = 1
    numIterations = 0
    numFetches = 0
    while True:
        timeType = getTimeType()
        if timeType == 'detect':
            checkDetectGroups(True)
        elif timeType == 'archive':
            checkDetectGroups(False)
        else:
            assert timeType == 'inactive'
            checkDetectGroups(False)
            checkDailyPostWork(dbManager, args.archiveDir)
            checkDailyExit()
            time.sleep(1*60)
            continue

        threadParams = []
        for threadNum in range(args.numThreads):
            threadParams.append([])
        nextThread = 0

        numIterations += 1
        startTime = time.time()
        for cameraInfo in cameras:
            timestamp = int(time.time())
            # logging.warning('Check camera %s, ts %s', cameraInfo['name'], timestamp)
            lastFetchTime = getFetchInfo(dbManager, cameraInfo, timestamp)
            if (lastFetchTime < timestamp - 60*MAX_INTERVAL_MINUTES):
                # fetchImage(dbManager, cameraInfo, lastFetchTime, args.archiveDir)
                # queue the fetching work to a thread and change the thread for enqueueing next fetch
                threadParams[nextThread].append([cameraInfo, lastFetchTime])
                nextThread = (nextThread + 1) % args.numThreads
                numFetches += 1

        # start all the threads to work concurrently
        threads = []
        for threadNum in range(args.numThreads):
            # logging.warning('params for thread %d: %s', threadNum, threadParams[threadNum])
            thread = threading.Thread(target=threadFn, args=(threadParams[threadNum],))
            threads.append(thread)
            thread.start()
            logging.warning('started thread %d: %s with %d fetches', threadNum, thread.ident, len(threadParams[threadNum]))

        # wait for all threads to finish
        for thread in threads:
            thread.join()
        logging.warning('All threads ended')

        endTime = time.time()
        if endTime - startTime < MIN_CYCLE_SECONDS:
            logging.warning('Sleeping for %.1f', MIN_CYCLE_SECONDS - (endTime - startTime))
            time.sleep(MIN_CYCLE_SECONDS - (endTime - startTime))
        else:
            logging.warning('Sleep overdue by %.1f', (endTime - startTime) - MIN_CYCLE_SECONDS)
        deleteOldFiles(dbManager)
        if (numIterations % 10) == 0:
            sqlStr = """SELECT count(*) FROM archive"""
            dbResult = dbManager.query(sqlStr)
            lsRes = os.listdir(args.archiveDir)
            logging.warning('Stats: iterations %s fetches %s, dbR %s, ls %s', numIterations, numFetches, dbResult[0]['count'], len(lsRes))

if __name__=="__main__":
    main()
