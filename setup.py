import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="oct-firecam",
    version="0.0.1",
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
    scripts=['bin/camera_mgmt.py',
             'bin/notification_mgmt.py'],
)
