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

Test collect_args

"""

import collect_args
import pytest

def testReqStr():
    requiredArgs = [
        ["n", "name", "some string"],
    ]
    args = collect_args.collectArgsInt(['-n', 'abc'], requiredArgs, [], None, False)
    assert args.name == 'abc'


def testOptStr():
    optionalArgs = [
        ["o", "name", "some string"],
    ]
    args = collect_args.collectArgsInt(['-o', 'bcd'], [], optionalArgs, None, False)
    assert args.name == 'bcd'


def testReqInt():
    requiredArgs = [
        ["v", "value", "some integer", int],
    ]
    args = collect_args.collectArgsInt(['-v', '121'], requiredArgs, [], None, False)
    assert args.value == 121


def testMissingReq():
    requiredArgs = [
        ["n", "name", "some string"],
    ]
    # expecting OSError: reading from stdin while output is captured
    with pytest.raises(OSError):
        args = collect_args.collectArgsInt([], requiredArgs, [], None, False)
