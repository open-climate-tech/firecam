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

Monitor fire detection process and restart if it dies.
Also restart if the detection process is not making progress.

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper

import time, datetime
import psutil
import subprocess
import psutil
import tempfile
import logging
import pathlib

def findProcess(name):
    for proc in psutil.process_iter():
        pinfo = None
        try:
            pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
            matches = list(filter(lambda x: name in x, pinfo['cmdline']))
            if len(matches) > 0:
                # print(matches, pinfo)
                return pinfo['pid']
        except:
            pass
    return None


def startProcess(detectFire, heartbeatFileName, collectPositves, restrictType):
    pArgs = [
        sys.executable,
        os.path.join(settings.fuegoRoot, "smoke-classifier", detectFire),
        '--heartbeat',
        heartbeatFileName
    ]
    if collectPositves:
        pArgs += ['--collectPositves', '1']
    if restrictType:
        pArgs += ['--restrictType', restrictType]
    proc = subprocess.Popen(pArgs)
    logging.warning('Started PID %d %s', proc.pid, pArgs)
    heartBeat(heartbeatFileName) # reset heartbeat
    return proc


def heartBeat(filename):
    """Inform monitor process that this detection process is alive

    Informs by updating the timestamp on given file

    Args:
        filename (str): file path of file used for heartbeating
    """
    pathlib.Path(filename).touch()


def lastHeartbeat(heartbeatFileName):
    return os.stat(heartbeatFileName).st_mtime


def main():
    optArgs = [
        ["n", "numProcesses", "number of child prcesses to start (default 1)"],
        ["g", "useGpu", "(optional) specify any value to use gpu (default off)"],
        ["c", "collectPositves", "collect positive segments for training data"],
        ["r", "restrictType", "Only process images from cameras of given type"],
    ]
    args = collect_args.collectArgs([], optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])
    numProcesses = int(args.numProcesses) if args.numProcesses else 1
    useGpu = True if args.useGpu else False

    if not useGpu:
        os.environ["CUDA_VISIBLE_DEVICES"]="-1"
    scriptName = 'detect_fire.py'
    procInfos = []
    for i in range(numProcesses):
        heartbeatFile = tempfile.NamedTemporaryFile()
        heartbeatFileName = heartbeatFile.name
        proc = startProcess(scriptName, heartbeatFileName, args.collectPositves, args.restrictType)
        procInfos.append({
            'proc': proc,
            'heartbeatFile': heartbeatFile,
            'heartbeatFileName': heartbeatFileName,
        })
        time.sleep(10) # 10 seconds per process to allow startup

    while True:
        for procInfo in procInfos:
            lastTS = lastHeartbeat(procInfo['heartbeatFileName']) # check heartbeat
            timestamp = int(time.time())
            proc = procInfo['proc']
            logging.debug('DBG: Process %d: %s: %d seconds since last image scanned, %d',
                            proc.pid, procInfo['heartbeatFileName'], timestamp - lastTS, lastTS)
            if (timestamp - lastTS) > 2*60: # warn if stuck more than 2 minutes
                logging.warning('Process %d: %d seconds since last image scanned', proc.pid, timestamp - lastTS)
            if (timestamp - lastTS) > 4*60: # kill if stuck more than 4 minutes
                logging.warning('Killing %d', proc.pid)
                proc.kill()
                procInfo['proc'] = startProcess(scriptName, procInfo['heartbeatFileName'], args.collectPositves, args.restrictType)
        time.sleep(30)


if __name__=="__main__":
    main()
