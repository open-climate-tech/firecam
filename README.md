# Wildfire detection

[Wildfire detection](https://openclimatetech.org/#projects) system leverages machine learning techniques to detect smoke in images from real time cameras from vantage points.  This repository contains all the code for collecting training data, training the model, and fetching images from cameras, checking for smoke, and notifying interested folks.  The repository also contains datasets used for training the model.

The following [paper](https://doi.org/10.3390/rs12010166) published in Remote Sensing journal provides much of the background for this project.

## Licenses

The code in this repository is released under [Apache License 2.0](LICENSE).
The datasets are licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License](https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode).
The original images were collected from the [HPWREN camera network](https://hpwren.ucsd.edu/cameras/) and the bounding boxes coordiantes are provided by Open Climate Tech.
