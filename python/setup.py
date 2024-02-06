import os
from importlib.machinery import SourceFileLoader
from setuptools import setup, find_packages


__version__ = (
    SourceFileLoader("hopsworks.version", os.path.join("hopsworks", "version.py"))
    .load_module()
    .__version__
)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hopsworks",
    version=__version__,
    install_requires=[
        "hsfs[python]>=3.7.0,<3.8.0",
        "hsml>=3.7.0,<3.8.0",
        "pyhumps==1.6.1",
        "requests",
        "furl",
        "boto3",
        "pyjks",
        "mock",
        "tqdm",
    ],
    extras_require={
        "dev": ["pytest", "flake8", "black"],
        "docs": [
            "mkdocs==1.3.0",
            "mkdocs-material==8.2.8",
            "mike==1.1.2",
            "sphinx==3.5.4",
            "keras_autodoc @ git+https://git@github.com/moritzmeister/keras-autodoc@split-tags-properties",
            "markdown-include",
            "markdown==3.3.7",
            "pymdown-extensions",
        ],
    },
    author="Logical Clocks AB",
    author_email="robin@logicalclocks.com",
    description="HOPSWORKS: An environment independent client to interact with the Hopsworks API",
    license="Apache License 2.0",
    keywords="Hopsworks, Feature Store, Spark, Machine Learning, MLOps, DataOps",
    url="https://github.com/logicalclocks/hopsworks-api",
    download_url="https://github.com/logicalclocks/hopsworks-api/releases/tag/"
    + __version__,
    packages=find_packages(),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
    ],
)
