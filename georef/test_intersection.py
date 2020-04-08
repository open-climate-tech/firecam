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

import intersection
import pytest
import numpy as np


def test_slope_intercept_90_degrees():
    coord = intersection.slope_and_intercept(45,135,5,-5,5,5)
    assert np.all(coord == [0,0])

def test_slope_intercept_45_degrees():
    coord = intersection.slope_and_intercept(45,90,5,2,5,5)
    assert np.all(coord == [2,2])

def test_slope_intercept_90_degrees_opp():
    coord = intersection.slope_and_intercept(135,45,5,-5,-5,-5)
    assert np.all(coord == [0,0])

def test_slope_intercept_45_degrees_opp():
    coord = intersection.slope_and_intercept(45,90,-5,-2,-5,-5)
    assert np.all(coord == [-2,-2])