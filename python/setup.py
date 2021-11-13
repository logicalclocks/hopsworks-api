#
#   Copyright 2021 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import os
import imp
from setuptools import setup, find_packages


__version__ = imp.load_source(
    "hsml.version", os.path.join("hopsworks", "version.py")
).__version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hopsworks",
    version=__version__,
    install_requires=[
        "hsfs",
        "hsml"
    ],
    extras_require={
        "dev": [
            "pytest",
            "flake8",
            "black"],
        "docs": [
            "mkdocs==1.1.2",
            "mkdocs-material==6.2.2",
            "mike==0.5.5",
            "sphinx==3.5.4",
            "keras_autodoc @ git+https://git@github.com/logicalclocks/keras-autodoc@split-tags-properties",
            "markdown-include"]
    },
    author="Logical Clocks AB",
    author_email="jim@logicalclocks.com",
    description="Hopsworks: An wrapper library that uses hsfs and hsfs libraries to interact with the Hopsworks Platform",
    license="Apache License 2.0",
    keywords="Hopsworks, Feature Store, ML, Models, Machine Learning Models, Model Registry, TensorFlow, PyTorch, Machine Learning, MLOps",
    url="https://github.com/logicalclocks/hopsworks-api",
    download_url="https://github.com/logicalclocks/hopsworks-api/releases/tag/"
    + __version__,
    packages=find_packages(),
    long_description=read("../README.md"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
    ],
)
