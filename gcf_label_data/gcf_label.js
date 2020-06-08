/*
* Copyright 2020 Open Climate Tech Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
* ==============================================================================
*/

'use strict';
// Cloud function to convert mp4 videos to sequence of jpg images

// Build instructions
// requires synclink to checkfire/server-src as checkfire-src

// Deployment instructions
// gcloud functions deploy oct-label --runtime nodejs10 --trigger-http --entry-point=recordLabels --set-env-vars OCT_FIRE_SETTINGS=bar

// local test instructions
// curl localhost:8080 --data '{"type":"bbox","fileName":"f1","minX":"10","minY":"11","maxX":"20","maxY": "21"}' --header "Content-Type: application/json"

const jwtDecode = require('jwt-decode');

const gcp_storage = require('./checkfire-src/gcp_storage');
const oct_utils = require('./checkfire-src/oct_utils');
const db_mgr = require('./checkfire-src/db_mgr');

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.recordLabels = async (req, res) => {
    console.log('query', req.query);
    console.log('bodyM', req.body);
    if (!req.body || !req.body.type || !req.body.fileName || !req.body.minX || !req.body.minY || !req.body.maxX || !req.body.maxY) {
        console.log('Missing parameters');
        res.status(400).send('Missing parameters');
        return;
    }
    if (req.body.type !== 'bbox') {
        console.log('Unsupported label type', req.body.type);
        res.status(400).send('Unsupported label type');
        return;
    }

    let userID = '';
    try {
        const token = jwtDecode(req.header('authorization'));
        userID = token.email;
    } catch (err) {
        console.log('ID failure %s', err.message);
    }
    console.log('userID %s', userID);

    const config = await oct_utils.getConfig(gcp_storage);
    try {
        const db = await db_mgr.initDB(config, true);

        const bboxKeys = ['ImageName', 'MinX', 'MinY', 'MaxX', 'MaxY', 'InsertionTime', 'UserID', 'Notes'];
        const bboxVals = [req.body.fileName, req.body.minX, req.body.minY, req.body.maxX, req.body.maxY,
                          Math.floor(new Date().valueOf()/1000), userID, req.body.notes || ''];
        await db.insert('bbox', bboxKeys, bboxVals);
    } catch (err) {
        console.log('Failure %s', err.message);
        res.status(400).send('Failure - check logs');
    }
    console.log('All done');
    res.status(200).send('done');
};


function testHandler() {
    exports.recordLabels({ // fake req
        query: {},
        body: {
            type: 'bbox',
            fileName: 'f1.jpg',
            minX: 1,
            minY: 2,
            maxX: 3,
            maxY: 4,
            notes: 'test',
        }
    }, { // fake res
        status: () => ({send: (m)=>{console.log('msg', m)}})
    });
}


console.log('argv: ', process.argv)
if ((process.argv.length > 1) && !process.argv[1].includes('functions-framework')) {
    // export GOOGLE_APPLICATION_CREDENTIALS='..../service-account.json'
    testHandler();
}
