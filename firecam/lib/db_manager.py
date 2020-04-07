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
This file is in charge of managing database pass through. The database 
contains information such as urls for the different image sources 
we may be pulling from as well as information of past detected events.

It can be used to read from the db by the image handler or to write to
the database by tools we use to populate it.

It supports both sqllite3 (for local testing) and postgres (for real work)
backends.

"""

import logging
import sqlite3
import time, datetime
import psycopg2
import psycopg2.extras

def _dict_factory(cursor, row):
    """
    This is a helper function to create a dictionary using the column names
    from the database as the keys

    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0].lower()] = row[idx] # lower() to match postgres.extras.RealDictCursor
    return d



class DbManager(object):
    def __init__(self, sqliteFile=None, psqlHost=None, psqlDb=None, psqlUser=None, psqlPasswd=None):
        """SQL DB connection class constructor

        Connects to the SQL DB (either sqlite or postgres) and creates the
        listed tables with schemas if the tables don't exist already.
        The DB connection and cursors are setup to return a query results
        in dictionory vs. list format for reliable processing.
        To avoid dangling transactions, the default mode is to immediately commit tx.

        Args:
            sqliteFile (str): file path to SQLite DB (if specified postgres parameters are ignored)
            psqlHost (str): IP address of postgreSQL server
            psqlDb (str): Database name in postgreSQL server
            psqlUser (str): Username for authentication to postgreSQL server
            psqlPasswd (str): Password for authentication to postgreSQL server
        """
        self.dbType = None
        if sqliteFile:
            logging.warning('using sqlite %s', sqliteFile)
            self.dbType = 'sqlite'
            self.conn = sqlite3.connect(sqliteFile)
            self.conn.row_factory = _dict_factory
        elif psqlHost:
            logging.warning('using postgres %s', psqlHost)
            self.dbType = 'psql'
            self.conn = psycopg2.connect(host=psqlHost, database=psqlDb, user=psqlUser, password=psqlPasswd)

        sources_schema = [
            ('name', 'TEXT'),
            ('url', 'TEXT'),
            ('last_date', 'TEXT'),
            ('randomID', 'REAL'),
            ('dormant', 'INT'),
            ('type', 'TEXT')
        ]

        counters_schema = [
            ('name', 'TEXT'),
            ('counter', 'INT')
        ]

        # no longer used?
        fires_schema = [
            ('Name', 'TEXT'),
            ('Url', 'TEXT'),
            ('Year', 'INT'),
            ('County', 'TEXT'),
            ('Location', 'TEXT'),
            ('Acres', 'TEXT'),
            ('EvacInfo', 'TEXT'),
            ('AdminUnit', 'TEXT'),
            ('Started', 'TEXT'),
            ('Updated', 'TEXT'),
            ('Latitude', 'REAL'),
            ('Longitude', 'REAL'),
            ('Month', 'INT'),
            ('Day', 'INT'),
            ('Hour', 'INT'),
            ('Minute', 'INT'),
            ('Timestamp', 'INT')
        ]

        cameras_schema = [
            ('Name', 'TEXT'),
            ('Latitude', 'REAL'),
            ('Longitude', 'REAL'),
            ('cameraIDs', 'TEXT')
        ]
        
        images_schema = [
            ('ImageID', 'TEXT'),
            ('ImageClass', 'TEXT'),
            ('FireName', 'TEXT'),
            ('CameraName', 'TEXT'),
            ('Timestamp', 'INT'),
            ('Smoke', 'TEXT'),
            ('Fog', 'TEXT'),
            ('Rain', 'TEXT'),
            ('Glare', 'TEXT'),
            ('Snow', 'TEXT'),
        ]

        cropped_schema = [
            ('CroppedID', 'TEXT'),
            ('MinX', 'INT'),
            ('MinY', 'INT'),
            ('MaxX', 'INT'),
            ('MaxY', 'INT'),
            ('EntireImageID', 'TEXT'),
        ]

        # all the detection squares ever found. anything from 0 to 1.0
        # (0,0) = top left
        scores_schema = [
            ('CameraName', 'TEXT'),
            ('Timestamp', 'INT'),
            ('MinX', 'INT'),
            ('MinY', 'INT'),
            ('MaxX', 'INT'),
            ('MaxY', 'INT'),
            ('Score', 'REAL'),
            ('SecondsInDay', 'INT'),
            ('MinusMinutes', 'INT'),
        ]

        # detections between historical half and 1.0
        detections_schema = [
            ('CameraName', 'TEXT'),
            ('Timestamp', 'INT'),
            ('MinX', 'INT'),
            ('MinY', 'INT'),
            ('MaxX', 'INT'),
            ('MaxY', 'INT'),
            ('Score', 'REAL'),
            ('HistAvg', 'REAL'),
            ('HistMax', 'REAL'),
            ('HistNumSamples', 'INT'),
            ('ImageID', 'TEXT'),
        ]

        # this is what's used to send out alert emails
        alerts_schema = [
            ('CameraName', 'TEXT'),
            ('Timestamp', 'INT'),
            ('ImageID', 'TEXT'),
        ]

        archive_schema = [
            ('CameraName', 'TEXT'),
            ('Timestamp', 'INT'),
            ('StorageID', 'TEXT'),
            ('FileID', 'TEXT'),
            ('Pan', 'REAL'),
            ('Tilt', 'REAL'),
            ('Zoom', 'REAL'),
            ('md5', 'TEXT'),
        ]

        notifications_schema = [
            ('Name', 'TEXT'),
            ('Email', 'TEXT'),
            ('EmailStartTime', 'INT'),
            ('EmailEndTime', 'INT'),
            ('Phone', 'TEXT'),
            ('PhoneStartTime', 'INT'),
            ('PhoneEndTime', 'INT'),
        ]

        self.tables = {
            'sources': sources_schema,
            'counters': counters_schema,
            'fires': fires_schema,
            'cameras': cameras_schema,
            'images': images_schema,
            'cropped': cropped_schema,
            'scores': scores_schema,
            'detections': detections_schema,
            'alerts': alerts_schema,
            'archive': archive_schema,
            'notifications': notifications_schema,
        }

        self.sources_table_name = 'sources'
        self._check_local_db()


    def __del__(self):
        self.conn.close()


    def _getCursor(self):
        """Return a cursor to operate on the DB

        Returns:
            DB cursor
        """
        if self.dbType == 'sqlite':
            return self.conn.cursor()
        elif self.dbType == 'psql':
            return self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)


    def create_db(self):
        pass


    def execute(self, sqlCmd, commit=True):
        """Execute given SQL command on DB

        Args:
            sqlCmd (str): SQL update/insert/delete statement
            commit (bool): [default true] - If true, transaction is committed

        """
        cursor = self._getCursor()
        cursor.execute(sqlCmd)
        if commit:
            self.conn.commit()
        cursor.close()


    def add_data(self, tableName, keyValues, commit=True):
        """Insert given data into given table

        Args:
            tableName (str):
            keyValues (dict or list): Dictory of key/value pairs for data to insert
                                      Or a list of dictionaries when inserting multiple rows
            commit (bool): [default true] - If true, transaction is committed
        """
        if type(keyValues) is list:
            kvList = keyValues
        else:
            kvList = [keyValues]
        valuesList = []
        firstKeys = [key for (key,_) in kvList[0].items()]
        for kvEntry in kvList:
            assert type(kvEntry) is dict
            keys = [key for (key,_) in kvEntry.items()]
            assert firstKeys == keys
            rowData = ", ".join(repr(val) for (_, val) in kvEntry.items())
            valuesList.append('(%s)' % rowData)
        valuesStr = ', '.join(valuesList)

        sql_template = 'insert into {table_name} ({fields}) values {values}'
        db_command = sql_template.format(
            table_name = tableName,
            fields = ", ".join(firstKeys),
            values = valuesStr
        )
        self.execute(db_command, commit=commit)


    def commit(self):
        self.conn.commit()


    def query(self, queryStr):
        """Query DB with given SQL query

        Args:
            queryStr (str): SQL SELECT query

        Returns:
            Array of dictionary of name->value pairs
        """
        result = []
        cursor = self._getCursor()
        cursor.execute(queryStr)
        row = cursor.fetchone()
        while row:
            result.append(row)
            row = cursor.fetchone()
        self.conn.commit() # stop idle read transacations
        cursor.close()
        return result


    def _check_local_db(self):
        """
        This ensures that the database exists and that the specified
        table exists within it.

        """
        sql_create_template = 'create table if not exists {table_name} ({fields})'
        cursor = self._getCursor()
        for tableName, tableSchema in self.tables.items():
            db_command = sql_create_template.format(
                table_name = tableName,
                fields = ", ".join(
                    variable + " " + data_type
                    for (variable, data_type) in tableSchema
                )
            )
            cursor.execute(db_command)
        self.commit()
        cursor.close()


    def get_sources(self, activeOnly=True, restrictType=None):
        constraints = []
        if activeOnly:
            constraints.append('dormant != 1')
        if restrictType:
            constraints.append("type = '%s'" % restrictType) # PSQL wants single quotes
        sqlStr = "SELECT * FROM %s" % self.sources_table_name
        if constraints:
            sqlStr += ' where ' + ' and '.join(constraints)
        sqlStr += ' order by randomID, name'
        return self.query(sqlStr)


    def add_url(self, url, urlname):
        date = datetime.datetime.utcnow().isoformat()
        self.add_data('sources', {'name': urlname, 'url': url, 'last_date': date})


    def _incrementCounterInt(self, cursor, counterName):
        """Internal function to increment the given counter in counters table

        Uses a read modify write pattern where the write only occurs if the
        value hasn't changed underneath due to other DB connections updating
        the same counter in parallel

        Args:
            cursor: DB cursor to use for the operation
            counterName (str): name of the counter

        Returns:
            Old value and the number of updated rows from the write
        """
        sqlTemplate = 'SELECT * from counters where name=%s'
        quotedCounterName = "'" + counterName + "'"
        sqlStr = sqlTemplate % (quotedCounterName)
        # print(sqlStr)
        cursor.execute(sqlStr)
        row = cursor.fetchone()
        if not row:
            print('failed to find counter')
            exit(1)
        # print(row)
        assert row['name'] == counterName
        value = row['counter']
        sqlTemplate = 'UPDATE counters set counter=%d where counter=%d and name = %s'
        sqlStr = sqlTemplate % (value+1, value, quotedCounterName)
        cursor.execute(sqlStr)
        updatedRows = cursor.rowcount
        return (value, updatedRows)


    def incrementCounter(self, counterName):
        """Increment the given counter in counters table

        To handle concurrent updates, keeps retrying until read-modify-write
        pattern successfully updates the value

        Args:
            counterName (str): name of the counter

        Returns:
            Old value of the counter
        """
        value = None
        try:
            cursor = self._getCursor()
            (value, updatedRows) = self._incrementCounterInt(cursor, counterName)
            if updatedRows != 1:
                raise Exception('Conflict')
            self.conn.commit()
            cursor.close()
            # print("Success", value, updatedRows)
        except Exception as e:
            self.conn.rollback()
            cursor.close()
            print("Error in increment.  Retrying", value, e)
            return self.incrementCounter(counterName) # tail-recursive

        return value


    def getNextSourcesCounter(self):
        return self.incrementCounter('sources')


    def getNotifications(self, filterActiveEmail = False, filterActivePhone = False):
        """Get all the notifications matching optinal active email and phone filters

        Args:
            filterActiveEmail (bool): only return notificaitons with currently active emails
            filterActivePhone (bool): only return notificaitons with currently active phones

        Returns:
            list of notifications
        """
        sqlTemplate = """SELECT * FROM notifications"""
        filters = []
        timeNow = int(time.time())
        if filterActiveEmail:
            filters.append('email is not null AND EmailStartTime < %s and EmailEndTime > %s' % (timeNow, timeNow))
        if filterActivePhone:
            filters.append('phone is not null AND PhoneStartTime < %s and PhoneEndTime > %s' % (timeNow, timeNow))
        if filters:
            sqlTemplate += ' WHERE ' + ' AND '.join(filters)
        sqlStr = sqlTemplate
        dbResult = self.query(sqlStr)
        return dbResult
