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

"""

import os, sys
from firecam.lib import settings
from firecam.lib import collect_args
from firecam.lib import goog_helper

import logging
import math
import numpy as np
"""

def main():
    reqArgs = [
        ["bn", "bang", " start of angle of view of smoke"],
        ["n", "ang", "angle of view of smoke"],
        ["a", "lat", "latitude of desired point", float],
        ["o", "long", "longtitude of desired point", float],
        ["bn2", "bang2", " start of angle of view2 of smoke"],
        ["n2", "ang2", "angle of view2 of smoke"],
        ["a2", "lat2", "latitude of desired point2", float],
        ["o2", "long2", "longtitude of desired point2", float]
    ]
    args = collect_args.collectArgs(reqArgs, optionalArgs=[], parentParsers=[goog_helper.getParentParser()])
    coords1 = np.array(slope_and_intercept(args.bang, args.bang2, args.lat, args.lat2, args.long, args.long2))
    coords2 = np.array(slope_and_intercept(args.bang, args.ang2, args.lat, args.lat2, args.long, args.long2))
    coords3 = np.array(slope_and_intercept(args.ang, args.bang2, args.lat, args.lat2, args.long, args.long2))
    coords4 = np.array(slope_and_intercept(args.ang, args.ang2, args.lat, args.lat2, args.long, args.long2))
    lalo = np.array([args.lat, args.long])
    lalo2 = np.array([args.lat2, args.long2])
    conf =  determine_false_pos(coords1,coords2,coords3,coords4,lalo,lalo2,0.251)
    conf2 =  determine_false_pos(coords1,coords2,coords3,coords4,lalo,lalo2,0.31)
    if conf:
        logging.warning('there is a fire around: lat:(%f), long:(%f)', conf[0], conf[1])
    elif conf2:
        logging.warning('there might be a fire around lat:(%f), long:(%f)', conf2[0], conf2[1])

def determine_false_pos(coords1,coords2,coords3,coords4,lalo,lalo2, range):
    midpoint1 = abs((coords1 + coords4)/2)
    midpoint2 = abs((coords2 + coords3)/2)
    midpoint = abs((midpoint1 + midpoint2)/2)
    diff = abs(midpoint - lalo)
    diff2 = abs(midpoint - lalo2)
    diff = (diff * diff)
    diff2 = (diff2 * diff2)
    dist = math.sqrt(diff[0] + diff[1])
    dist2 = math.sqrt(diff2[0] + diff2[1])
    if dist < range and dist2 < range:
        return midpoint
    else:
        return None
"""




def slope_and_intercept(a1, a2, lat1, lat2, long1, long2):
#error if vertical line add a check for it
    m1 = 1/(math.tan(math.pi/(180/a1)))

    m2 = 1/(math.tan(math.pi/(180/a2)))
    """
    if m1 == m2:
        logging.warning('the edge is parallel')
        if (((lat1 - lat2)*m1) + long1) == long2:
            co
            """
    b1 = lat1 - (m1*long1)
    b2 = lat2 - (m2*long2)
    coe = (m1 - m2)
    con = (b2 - b1)
    x = con/coe
    x = round(x,6)
    y = m1*x + b1
    y = round(y,6)
    coord = np.array([y,x])
    logging.warning('the coords of intersection are: (%f, %f)', coord[0],coord[1])
    return coord


if __name__=="__main__":
    main()
