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

Parse HPWRTEN KML data to generate list of cameras, coordinates, and URLs
Expected usage: 
egrep '(name|href|coordinates)' ~/Downloads/cameras-fixed.kml | python hpwren_kml_parse.py

"""

import fileinput
import urllib.request
from html.parser import HTMLParser


class MyHTMLParser(HTMLParser):
    def __init__(self):
        self.table = []
        self.rowInfo = None
        self.inRow = True
        self.nextType = None
        super().__init__()


    def handle_starttag(self, tag, attrs):
        # if self.inRow:
        #     print("Encountered a start tag:", tag)
        if (tag == 'name'):
            self.flushRow()

            self.rowInfo = {
                'urls': []
            }
            self.inRow = True
            self.nextType = "Name"
        elif self.inRow and tag == 'a' and len(attrs) > 0:
            for attr in attrs:
                if len(attr) == 2 and attr[0]=='href':
                    self.rowInfo['urls'].append(attr[1])
        elif (tag == 'coordinates'):
            self.nextType = 'coordinates'


    def handle_endtag(self, tag):
        # if self.inRow:
        #     print("Encountered an end tag :", tag)
        return

    def handle_data(self, data):
        # if self.inRow:
        #     print("Encountered some data  :", data)
        if (self.inRow):
            if (self.nextType == 'Name'):
                self.rowInfo[self.nextType] = data
                self.nextType = None
            elif (self.nextType == 'coordinates'):
                coords = data.split(',')
                self.rowInfo['Longitude'] = float(coords[0])
                self.rowInfo['Latitude'] = float(coords[1])
                self.nextType = None


    def flushRow(self):
        if (self.rowInfo != None) and (self.rowInfo.get('Name') != None):
            self.table.append(self.rowInfo)
            self.rowInfo = None
            self.inRow = False


    def dumpTable(self):
        for row in self.table:
            print(row)



parser = MyHTMLParser()
for data in fileinput.input():
    parser.feed(data)
parser.flushRow()

parser.dumpTable()
