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

List of all poicies

"""

import os, sys

from . import inception_and_threshold
from . import detect_always
from . import detect_never
from . import detect_multi

def get_policies():
    return {
        'inception_and_threshold': inception_and_threshold.InceptionV3AndHistoricalThreshold,
        'multi': detect_multi.DetectMulti,
        'always': detect_always.DetectAlways,
        'never': detect_never.DetectNever,
    }

