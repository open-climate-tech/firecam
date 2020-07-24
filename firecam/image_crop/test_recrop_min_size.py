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

Test recrop_min_size

"""

from . import recrop_min_size
import pytest



def test_getCameraDir():
	#(service, cameraCache, fileName):
	#need to figure out where it is used
	print( "not implemented")


def test_getRangeFromCenter():
	#getRangeFromCenter(center, size, minLimit, maxLimit) ==> (val0, val1)
	assert recrop_min_size.getRangeFromCenter(10, 6, 0, 20) == (7, 13) #unlimited
	assert recrop_min_size.getRangeFromCenter(10, 20, 0, 100) == (0, 20) # left limited
	assert recrop_min_size.getRangeFromCenter(50, 20, 0, 60) == (40, 60) # right limited
	assert recrop_min_size.getRangeFromCenter(10, 40, 0, 20) == (0, 20) # both limited
	assert recrop_min_size.getRangeFromCenter(50, 9, 0, 100) == (46, 55) # odd size


def test_randomInRange():
	#randomInRange(borders, avoidCenter, size) ==> (val0, val1)
	# Check 100 randomized trials to validate range
	borders = (20,10)
	avoidCenter = (10,5)
	size = (100,100)
	for i in range(100):
		r = recrop_min_size.randomInRange(borders, avoidCenter, size)
		print('r', r)
		assert (((r[0] >= avoidCenter[0]) and (r[0] <= int(size[0]/2 - borders[0]))) or
			    ((r[1] >= avoidCenter[1]) and (r[1] <= int(size[1]/2 - borders[1]))))

	# Force avoidCenter to be too big such that values must be size/2-border
	r = recrop_min_size.randomInRange((20,10), (45,45), (100,100))
	assert r[0] == 30
	assert r[1] == 40
	print('r2', r)


def test_appendIfDifferent():
	#(array, newItem):
	a = [1,2]
	recrop_min_size.appendIfDifferent(a,2)
	assert	a == [1,2]
	recrop_min_size.appendIfDifferent(a,3)
	assert  a == [1,2,3]


def test_getCropCoords():
	smokeCoords = (1200, 1100, 1300, 1200)
	# Check 100 randomized trials to validate range
	for i in range(100):
		cropCoords = recrop_min_size.getCropCoords(smokeCoords, 299, 299, 1.2, (2000,2000))
		print('cc0', cropCoords)
		for coords in cropCoords:
			assert coords[2] == coords[0] + 299
			assert coords[3] == coords[1] + 299
			assert coords[0] <= smokeCoords[0]
			assert coords[1] <= smokeCoords[1]
			assert coords[2] >= smokeCoords[2]
			assert coords[3] >= smokeCoords[3]

	# Limited by bottom and right
	cropCoords = recrop_min_size.getCropCoords(smokeCoords, 299, 299, 1.2, (1300,1200))
	print('cc1', cropCoords)
	for coords in cropCoords:
		assert coords[2] == coords[0] + 299
		assert coords[3] == coords[1] + 299
		assert coords[0] <= smokeCoords[0]
		assert coords[1] <= smokeCoords[1]
		assert coords[2] == 1300
		assert coords[3] == 1200

	# Limited by top and left
	smokeCoords = (0, 0, 150, 100)
	cropCoords = recrop_min_size.getCropCoords(smokeCoords, 299, 299, 1.2, (2000,2000))
	print('cc2', cropCoords)
	for coords in cropCoords:
		assert coords[2] == coords[0] + 299
		assert coords[3] == coords[1] + 299
		assert coords[0] == 0
		assert coords[1] == 0
		assert coords[2] >= smokeCoords[2]
		assert coords[3] >= smokeCoords[3]

	# smokeCoords larger (400) than minDiff (299)
	size = 400
	growRatio = 1.2
	smokeCoords = (1200, 1100, 1200 + size, 1100 + size)
	cropCoords = recrop_min_size.getCropCoords(smokeCoords, 299, 299, growRatio, (2000,2000))
	print('ccl', cropCoords)
	for coords in cropCoords:
		assert coords[2] == coords[0] + size*growRatio
		assert coords[3] == coords[1] + size*growRatio
		assert coords[0] <= smokeCoords[0]
		assert coords[1] <= smokeCoords[1]
		assert coords[2] >= smokeCoords[2]
		assert coords[3] >= smokeCoords[3]


def test_getCropCoordsCenter():
	smokeCoords = (1200, 1100, 1300, 1200)
	cropCoords = recrop_min_size.getCropCoords(smokeCoords, 299, 299, 1.2, (2000,2000), centerOnly=True)
	print('ccc', cropCoords)
	assert len(cropCoords) == 1


def test_main():
	print( "not implemented")

