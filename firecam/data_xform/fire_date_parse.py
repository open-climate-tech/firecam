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
Parse the start date/time text string in the fires DB and fill out the
year/month/day/hour/minute/timestamp fields

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

import time
import dateutil.parser
import logging


def getUnparsedFires(dbManager):
    sqlStr = """SELECT * from fires where timestamp is null and started is not null"""
    dbResult = dbManager.query(sqlStr)
    logging.warning('dbr %d: %s', len(dbResult), dbResult[:2])
    return dbResult


def parseDates(dbManager, fires):
    sqlTemplate = """UPDATE fires SET year=%d,month=%d,day=%d,hour=%d,minute=%d,timestamp=%d WHERE started='%s' """
    for fire in fires:
        stripped = fire['started'].replace('\\xa0', '')
        dt = dateutil.parser.parse(stripped)
        tstamp = int(time.mktime(dt.timetuple()))
        logging.warning('FIRE: %s, started %s, stripped %s, dt %s', fire['name'],fire['started'], stripped, dt)
        sqlStr = sqlTemplate % (dt.year, dt.month, dt.day, dt.hour, dt.minute, tstamp, fire['started'])
        logging.warning('sql: %s', sqlStr)
        dbManager.execute(sqlStr)


def main():
    reqArgs = []
    args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
    dbManager = db_manager.DbManager(sqliteFile=settings.db_file)
    fires = getUnparsedFires(dbManager)
    parseDates(dbManager, fires)


if __name__=="__main__":
    main()
