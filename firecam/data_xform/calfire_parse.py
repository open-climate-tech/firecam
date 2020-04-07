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

Parse HTML data fetched from CalFire website.  Optionally also fetch the data
by iterating over all the current incidents pages.

"""

import urllib.request
from html.parser import HTMLParser

url = 'http://www.fire.ca.gov/current_incidents'
# file = 'c:/Users/kinshuk/Downloads/calfire_current_top.html'
file = 'c:/Users/kinshuk/Downloads/calfire-2017.html'
year = 2017

class MyHTMLParser(HTMLParser):
    def __init__(self):
        self.table = []
        self.rowInfo = None
        self.inRow = False
        self.nextType = None
        super().__init__()


    def handle_starttag(self, tag, attrs):
        # if self.inRow:
        #     print("Encountered a start tag:", tag)
        if (tag == 'tbody'):
            self.rowInfo = {
                "Year": year,
                "Extra": []
            }
            self.inRow = True
            self.nextType = "Name"
        elif self.inRow and tag == 'a' and len(attrs) > 0:
            for attr in attrs:
                if len(attr) == 2 and attr[0]=='href':
                    self.rowInfo['href'] = attr[1]


    def handle_endtag(self, tag):
        # if self.inRow:
        #     print("Encountered an end tag :", tag)
        if (tag == 'tbody' and self.rowInfo != None):
            rowName = self.rowInfo.get('Name')
            if ((rowName != None) and (rowName != 'Search')):
                self.table.append(self.rowInfo)
            self.rowInfo = None
            self.inRow = False


    def handle_data(self, data):
        # if self.inRow:
        #     print("Encountered some data  :", data)
        if (self.inRow):
            if ((data[0] == '\n') or (data[0] == '\r')):
                return

            if (self.nextType != None):
                self.rowInfo[self.nextType] = data
                self.nextType = None
            elif (data == 'County:'):
                self.nextType = 'County'
            elif (data == 'Location:'):
                self.nextType = 'Location'
            elif (data == 'Acres Burned - Containment:') or (data == 'Status/Notes:'):
                self.nextType = 'Acres'
            elif (data == 'Evacuation Info:'):
                self.nextType = 'EvacInfo'
            elif (data == 'Administrative Unit:'):
                self.nextType = 'AdminUnit'
            elif (data == 'Date Started:'):
                self.nextType = 'Started'
            elif (data == 'Last update:'):
                self.nextType = 'Updated'
            elif (data.startswith('Updated: ')):
                self.rowInfo['Updated'] = data[9:]
            else:
                self.rowInfo['Extra'].append(data)


    def dumpTable(self):
        for row in self.table:
            print(row)


def parseData(data):
    parser = MyHTMLParser()
    parser.feed(data)
    parser.dumpTable()

def parseFile(fileName):
    with open(fileName, 'r') as myfile:
        data = myfile.read()
        parseData(data)

def parseUrl(url):
    with urllib.request.urlopen(url) as response:
        data = response.read().decode()
        parseData(data)

# for pageNum in range(1,55):
#     pageUrl = url
#     if (pageNum != 1):
#         pageUrl += '/?page=' + str(pageNum)
#     print(pageUrl)
#     parseUrl(pageUrl)

parseFile(file)
# parseUrl(url)
