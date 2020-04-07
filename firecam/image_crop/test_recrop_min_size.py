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

Test recrop_min_size

"""

import recrop_min_size
import pytest



def test_imageDisplay():
	#(imgOrig, title=''):
	print( "not implemented")
def test_buttonClick():
	#(event):
	print( "not implemented")
def test_displayImageWithScores():
	#(imgOrig, segments):
	print( "not implemented")
def test_getCameraDir():
	#(service, cameraCache, fileName):
	#need to figure out where it is used
	print( "not implemented")
def test_expandMinAndMax():
	#(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
	#expandMinAndMax(val0, val1, minimumDiff, growRatio, minLimit, maxLimit) ==> (val0, val1)
	assert recrop_min_size.expandMinAndMax(3, 6, 3, 1, 3, 6) == (3, 6)
	assert recrop_min_size.expandMinAndMax(3, 6, 5, 1, 2, 7) == (2, 7)
	assert recrop_min_size.expandMinAndMax(3, 6, 2, 2, 1, 20) == (1, 7)
	assert recrop_min_size.expandMinAndMax(3, 6, 10, 2, 1, 20) == (1, 11)
def test_expandMax():
	#(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
	assert recrop_min_size.expandMax(1, 10,5 ,1 ,3,7 ) == (3,7 )
	assert recrop_min_size.expandMax(0, 49,50 ,2 ,0,100 ) == (0,98 )
	assert recrop_min_size.expandMax(1, 10,2 ,1 ,3,7 ) == (3,7 )
	assert recrop_min_size.expandMax(6, 8,4 ,2 ,0,10 ) == (6,10 )
def test_expandMax75():
	#(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):  
	assert recrop_min_size.expandMax75(49,51 ,6 ,2 ,0 ,100 ) == (49,55 )
	assert recrop_min_size.expandMax75(0,2 ,8 ,2 ,0 ,100 ) == (0,8 )
	assert recrop_min_size.expandMax75(98,100 ,8 ,2 ,0 ,100 ) == (92,100 )
	assert recrop_min_size.expandMax75(46,53 ,6 ,2 ,0 ,100 ) == (46,60)
def test_expandMin():
	#(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
	assert recrop_min_size.expandMin(0,100 ,50 ,1 ,0 ,100 ) == (0,100 )
	assert recrop_min_size.expandMin(0,10 ,12 ,1 ,0 ,100 ) == (0,12 )
	assert recrop_min_size.expandMin(2,12 ,12 ,1 ,0 ,100 ) == (0,12 )
def test_expandMin75():
	#(val0, val1, minimumDiff, growRatio, minLimit, maxLimit):
	assert recrop_min_size.expandMin75(0,50,30 ,1 ,0 ,100 ) == (0,50 )
	assert recrop_min_size.expandMin75(0,30,60 ,1 ,0 ,100 ) == (0,60 )
def test_appendIfDifferent():
	#(array, newItem):
	a = [1,2]
	recrop_min_size.appendIfDifferent(a,2)
	assert	a == [1,2]
	recrop_min_size.appendIfDifferent(a,3)
	assert  a == [1,2,3]
def test_main():
	print( "not implemented")

