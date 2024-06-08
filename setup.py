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

Package config

To produce package files:
python setup.py sdist bdist_wheel

To upload to testPyPi
twine upload --repository testpypi --skip-existing dist/*

To upload to real PyPi
twine upload --skip-existing dist/*

To download from testPyPi
pip install --extra-index-url https://test.pypi.org/simple/  -U oct_firecam

"""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="oct-firecam",
    version="0.1.3",
    author="Open Climate Tech",
    description="Detect wildfires from camera images",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/open-climate-tech/firecam",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
    install_requires=[
        'python-dateutil',
        'requests',
        'oauth2client',
        'google-api-python-client',
        'google-cloud-storage',
        'google-cloud-pubsub',
        'google-cloud-compute',
        'googlemaps',
        'pillow',
        'opencv-python',
        'shapely',
        'ffmpeg-python',
        'tensorflow',
        'numpy',
        'pandas',
        'psycopg2-binary',
        'pysqlite3',
        'twilio',
    ],
    entry_points={
        'console_scripts': [
            'get_image_hpwren = firecam.cmds.get_image_hpwren:main',
            'sort_images = firecam.cmds.sort_images:main',
        ]
    },
)
