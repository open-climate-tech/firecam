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

add, update, delete, and list notifictaion settings

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import db_manager

import logging
import time, datetime, dateutil.parser

def getTimeRangeStr(startTime, endTime):
    """Return a string with given time range and indication whether current time is in range

    Args:
        startTime (int): timestamp of starting time of range
        endTime (int): timestamp of ending time of range

    Returns:
        string
    """
    startTimeStr = datetime.datetime.fromtimestamp(startTime).isoformat()
    endTimeStr = datetime.datetime.fromtimestamp(endTime).isoformat()
    timeNow = int(time.time())
    active = (startTime < timeNow) and (timeNow < endTime)
    return '%s - %s %s' % (startTimeStr, endTimeStr, 'active' if active else 'dormant')


def printNoficiation(notification):
    """Print the given notification object in easy to read format

    Args:
        notification (dict): notification object from notifications table
    """
    outputStr = 'User: %s' % notification['name']
    if notification['email']:
        outputStr += ' ; Email: %s (%s)' % (notification['email'], getTimeRangeStr(notification['emailstarttime'], notification['emailendtime']))
    if notification['phone']:
        outputStr += ' ; Phone: %s (%s)' % (notification['phone'], getTimeRangeStr(notification['phonestarttime'], notification['phoneendtime']))
    logging.warning(outputStr)


def parseTimeStr(timeStr):
    """Return timestamp value from given time string

    Args:
        timeStr (str): time represented in string

    Returns:
        int (timestamp)
    """
    dt = dateutil.parser.parse(timeStr)
    return int(dt.timestamp())


def main():
    reqArgs = [
        ["o", "operation", "add (includes update), delete, list"],
    ]
    optArgs = [
        ["n", "name", "name (ID) of user"],
        ["m", "email", "email address of user"],
        ["p", "phone", "phone number of user"],
        ["s", "startTime", "starting date and time in ISO format (e.g., 2019-02-22T14:34:56 in Pacific time zone)"],
        ["e", "endTime", "ending date and time in ISO format (e.g., 2019-02-22T14:34:56 in Pacific time zone)"],
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs)
    startTime = parseTimeStr(args.startTime) if args.startTime else None
    endTime = parseTimeStr(args.endTime) if args.endTime else None
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file,
                                    psqlHost=settings.psqlHost, psqlDb=settings.psqlDb,
                                    psqlUser=settings.psqlUser, psqlPasswd=settings.psqlPasswd)
    notifications = dbManager.getNotifications()
    activeEmails = dbManager.getNotifications(filterActiveEmail=True)
    activePhones = dbManager.getNotifications(filterActivePhone=True)
    logging.warning('Num all notifications: %d.  Active emails: %d.  Active phones: %d',
                     len(notifications), len(activeEmails), len(activePhones))
    if args.operation == 'list':
        for n in notifications:
            printNoficiation(n)
        return
    assert args.name
    matching = list(filter(lambda x: x['name'] == args.name, notifications))
    logging.warning('Found %d matching for name %s', len(matching), args.name)
    if matching:
        printNoficiation(matching[0])
    if args.operation == 'add':
        assert startTime and endTime
        assert endTime >= startTime
        assert args.email or args.phone
        if not matching:
            # insert new entry
            dbRow = {
                'name': args.name,
            }
            if args.email:
                dbRow['email'] = args.email
                dbRow['EmailStartTime'] = startTime
                dbRow['EmailEndTime'] = endTime
            if args.phone:
                dbRow['phone'] = args.phone
                dbRow['PhoneStartTime'] = startTime
                dbRow['PhoneEndTime'] = endTime
            dbManager.add_data('notifications', dbRow)
            logging.warning('Successfully added notification for %s', args.name)
        else:
            # update existing entry
            if args.email:
                sqlTemplate = """UPDATE notifications SET email='%s',EmailStartTime=%s,EmailEndTime=%s WHERE name = '%s' """
                sqlStr = sqlTemplate % (args.email, startTime, endTime, args.name)
                dbManager.execute(sqlStr)
            if args.phone:
                sqlTemplate = """UPDATE notifications SET phone='%s',PhoneStartTime=%s,PhoneEndTime=%s WHERE name = '%s' """
                sqlStr = sqlTemplate % (args.phone, startTime, endTime, args.name)
                dbManager.execute(sqlStr)
            logging.warning('Successfully updated notification for %s', args.name)
        notifications = dbManager.getNotifications()
        matching = list(filter(lambda x: x['name'] == args.name, notifications))
        printNoficiation(matching[0])
    elif args.operation == 'delete':
        sqlTemplate = """DELETE FROM notifications WHERE name = '%s' """
        sqlStr = sqlTemplate % (args.name)
        dbManager.execute(sqlStr)
    else:
        logging.error('Unexpected operation: %s', args.operation)


if __name__=="__main__":
    main()
