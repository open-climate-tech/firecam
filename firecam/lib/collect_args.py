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

Simple wrapper around argparse that supports a data driven (array specs) vs.
argparse's code driven specs to parse arguments

"""

import argparse
import logging
import sys

# internal function for pytest that accepts cmdArgs parameter and no defaults
def collectArgsInt(cmdArgs, requiredArgs, optionalArgs, parentParsers, silence):
    parser = argparse.ArgumentParser(parents=parentParsers if parentParsers != None else [])
    for arg in requiredArgs+optionalArgs:
        parser.add_argument('-'+arg[0], '--'+arg[1], help=arg[2], type=arg[3] if len(arg)>3 else None)
    args = parser.parse_args(cmdArgs)

    vargs = vars(args)
    for arg in requiredArgs:
        if (vargs.get(arg[1]) == None):
            vargs[arg[1]] = input('Please enter the ' + arg[2] + ': ')

    if not silence:
        logging.warning('Using these parameters')
        for arg in requiredArgs+optionalArgs:
            if vargs[arg[1]] != None:
                logging.warning(arg[2]+ ': ' + str(vargs[arg[1]]))
    return args


def collectArgs(requiredArgs, optionalArgs=[], parentParsers=None, silence=False):
    """Parse the process command line parameters into arguments given the specs

    Args:
        requiredArgs (list): list of required parameters
        optionalArgs (list): list of optional parameters
        parentParsers: parsers for arguments for other libraries
        silence (bool): If true, argument values are logged
    """
    return collectArgsInt(sys.argv[1:], requiredArgs, optionalArgs, parentParsers, silence)
