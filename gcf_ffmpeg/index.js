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


// Deployment instructions
// gcloud functions deploy fuego-ffmpeg1 --runtime nodejs10 --trigger-http --entry-point=extractMp4 --memory=2048MB --timeout=540s

const os = require('os');
const fs = require('fs');
const path = require('path');
const request = require('request');
const rimraf = require('rimraf');
const {Storage} = require('@google-cloud/storage');

const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;
const FfmpegCommand = require('fluent-ffmpeg');
FfmpegCommand.setFfmpegPath(ffmpegPath);

function getHpwrenUrl(hostName, cameraID, yearDir, dateDir, qNum) {
    var hpwrenUrl = 'http://' + encodeURIComponent(hostName) + '.hpwren.ucsd.edu/archive/';
    hpwrenUrl += encodeURIComponent(cameraID) + '/large/';
    if (yearDir) {
        hpwrenUrl += encodeURIComponent(yearDir) + '/';
    }
    hpwrenUrl += encodeURIComponent(dateDir) + '/MP4/Q' + qNum + '.mp4';
    return hpwrenUrl;
}

function getTmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'fuego_ffmpeg_'));
}


function listFiles(dir) {
    const files = fs.readdirSync(dir);
    const sizes = [];
    files.forEach((file) => {
        const ss = fs.statSync(path.join(dir, file));
        sizes.push(ss.size);
    });
    console.log('ListFiles: ', sizes.length, sizes.join(', '));
}

function downloadMp4(mp4Url, mp4File, cb) {
    request(mp4Url,
        function(error, response, body) { // triggers after all the data is received (but before 'complete' event)
            console.log('cb error:', error); // Print the error if one occurred
            console.log('cb statusCode:', response && response.statusCode); // Print the response status code if a response was received
            console.log('cb body', typeof(body), body && body.length); // this length doesn't match file size
        })
        .on('response', function(response) { // triggers on initial response
            console.log('event response sc', response.statusCode) // 200
            console.log('event response ct', response.headers['content-type']) // 'image/png'
        })
        // .on('data', function(data) {  // trigers on every chunk of data received
        //     console.log('event data', typeof(data), data.length); // sum of all data.length matches file size
        // })
        .on('error', function(err) {
            console.log('event error', err);
            cb(err);
        })
        .on('complete', function(resp, body) { // triggers after everything (including cb function to request())
            console.log('event complete sc', typeof(resp), resp.statusCode);
            console.log('event complete body', typeof(body), body.length); // this length doesn't match file size
            cb(null, resp, body);
        })
        .pipe(fs.createWriteStream(mp4File));
}

function getJpegs(mp4File, outFileSpec, cb) {
    var cmd = new FfmpegCommand();
    var cmdLine = null;
    cmd.input(mp4File).output(outFileSpec);
    cmd.format('image2').videoCodec('mjpeg').outputOptions('-qscale 0');
    cmd.on('start',function(cl){console.log('started:' + cl);cmdLine = cl;});
    cmd.on('error',function(err){console.log('errorM:' + err);cb(err)});
    cmd.on('end',function(){console.log('ended!');cb(null, cmdLine)});
    cmd.run();
}

 
function getPaddedTwoDigits(value) {
    let valueStr = value.toString()
    if (value < 10) {
        valueStr = '0' + valueStr;
    }
    return valueStr;
}

const GS_URL_REGEXP = /^gs:\/\/([a-z0-9_.-]+)\/(.+)$/;

/**
 * Parse the given string path into bucket and name if it is GCS path
 * @param {string} path
 * @return {object} with bucket and name properties
 */
function parsePath(path) {
  const parsed = GS_URL_REGEXP.exec(path);
  if (parsed && Array.isArray(parsed)) {
    let name = parsed[2];
    if (name.endsWith('/')) { // strip the final / if any
      name = name.slice(0,name.length-1);
    }
    return {
      bucket: parsed[1],
      name: name,
    }
  }
}

async function uploadFiles(fromDir, uploadDir, imgCamDatePrefix, qNum, cb) {
    // const storage = new Storage({keyFilename: "key.json"});
    const storage = new Storage();
    const parsedUploadDir = parsePath(uploadDir);
    console.log('upload dir', parsedUploadDir);
    const bucket = storage.bucket(parsedUploadDir.bucket);

    const batchSize = 8; // up to 8 files in parallel gets nice speedup in google cloud environment
    let fileNames = fs.readdirSync(fromDir);
    fileNames = fileNames.filter(fn => fn.endsWith('.jpg')); // skip the mp4
    fileNames = fileNames.sort(); // sort by name to ensure correct time ordering
    let files = [];
    let batchProms = [];
    for (var i = 0; i < fileNames.length; i++) {
        const filePath = path.join(fromDir, fileNames[i]);
        const hour = getPaddedTwoDigits((qNum-1)*3 + Math.floor(i/60));
        const minute = getPaddedTwoDigits(i % 60);
        const newFileName = imgCamDatePrefix + hour + ';' + minute + ';00.jpg';
        const destFileFullName = parsedUploadDir.name + '/' + newFileName;

        try {
            batchProms.push(bucket.upload(filePath, {destination: destFileFullName}));
            if ((batchProms.length >= batchSize) || (i == (fileNames.length - 1))) {
                let batchFiles = [];
                for (var j = 0; j < batchProms.length; j++) {
                    const fileInfo = await batchProms[j].catch(function(err) {
                        console.log('upload await err', err.message, err);
                    });
                    batchFiles.push(fileInfo[0]);
                }
                files = files.concat(batchFiles);
                console.log('await files', i, batchFiles.map(fi=>fi.name.split('/').pop()));
                batchProms = [];
                batchFiles = [];
            }
        } catch (err) {
            console.log('await err', err);
            cb(err);
            return;
        }
    }
    console.log('finishing upload');
    cb(null, files);
}

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.extractMp4 = (req, res) => {
    console.log('query', req.query);
    console.log('bodyM', req.body);
    if (!req.body || !req.body.hostName || !req.body.cameraID || !req.body.yearDir || !req.body.dateDir || !req.body.qNum) {
        console.log('Missing parameters');
        res.status(400).send('Missing parameters');
        return;
    }
    var hpwrenUrl = getHpwrenUrl(req.body.hostName, req.body.cameraID, req.body.yearDir, req.body.dateDir, req.body.qNum);
    console.log('URL: ', hpwrenUrl);
    const tmpDir = getTmpDir();
    const mp4File = path.join(tmpDir, 'q.mp4');
    console.log('File: ', mp4File);
    const imgCamDatePrefix = req.body.cameraID + '__' +
                                req.body.dateDir.slice(0,4) + '-' +
                                req.body.dateDir.slice(4,6) + '-' +
                                req.body.dateDir.slice(6,8) + 'T';
    downloadMp4(hpwrenUrl, mp4File, function(err, resp, body) {
        if (err) {
            res.status(400).send('Could not download mp4');
            return;
        }
        console.log('Listing files after download');
        listFiles(tmpDir);
        const outFileSpec = path.join(tmpDir, 'img-%03d.jpg');
        getJpegs(mp4File, outFileSpec, function (err, cmdLine) {
            if (err) {
                res.status(400).send('Could not decode mp4');
                return;
            }
            console.log('Listing files after ffmpeg');
            listFiles(tmpDir);
            uploadFiles(tmpDir, req.body.uploadDir, imgCamDatePrefix, req.body.qNum, function(err, files) {
                if (err) {
                    res.status(400).send('Could not upload jpegs');
                    return;
                }
                rimraf.sync(tmpDir);
                console.log('All done');
                res.status(200).send('done');
            });
        });
    });
};


function testHandler() {
    exports.extractMp4({ // fake req
        query: {},
        body: {
            hostName: 'c1',
            cameraID: 'rm-w-mobo-c',
            yearDir: '2017',
            dateDir: '20170613',
            qNum: '3', // 'Q3.mp4'
            uploadDir: 'gs://bucket/ffmpeg/testX',
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
