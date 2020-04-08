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

Test get_elevation

"""

import get_elevation


def test_mapping_with_bounds_top_left():
    coord = get_elevation.mapping_with_bounds(10, 11, 0.003, 15)
    assert coord == None


def test_mapping_with_bounds_on_boundaries():
    coord = get_elevation.mapping_with_bounds(10, 10, 0.003, 15)
    assert coord == 0


def test_mapping_with_bounds_outer_bounds():
    coord = get_elevation.mapping_with_bounds(11.045, 11, 0.003, 15)
    assert coord == 14


def test_mapping_with_bounds_in_bounds():
    coord = get_elevation.mapping_with_bounds(11.042, 11, 0.003, 15)
    assert coord == 13


def test_mapping_with_bounds_out_far_bounds():
    coord = get_elevation.mapping_with_bounds(12, 11, 0.003, 15)
    assert coord == None
