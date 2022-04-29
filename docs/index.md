# Hopsworks Client

<p align="center">
  <a href="https://community.hopsworks.ai"><img
    src="https://img.shields.io/discourse/users?label=Hopsworks%20Community&server=https%3A%2F%2Fcommunity.hopsworks.ai"
    alt="Hopsworks Community"
  /></a>
    <a href="https://docs.hopsworks.ai"><img
    src="https://img.shields.io/badge/docs-HOPSWORKS-orange"
    alt="Hopsworks Documentation"
  /></a>
  <a href="https://pypi.org/project/hopsworks/"><img
    src="https://img.shields.io/pypi/v/hopsworks?color=blue"
    alt="PyPiStatus"
  /></a>
  <a href="https://pepy.tech/project/hopsworks/month"><img
    src="https://pepy.tech/badge/hopsworks/month"
    alt="Downloads"
  /></a>
  <a href="https://github.com/psf/black"><img
    src="https://img.shields.io/badge/code%20style-black-000000.svg"
    alt="CodeStyle"
  /></a>
  <a><img
    src="https://img.shields.io/pypi/l/hopsworks?color=green"
    alt="License"
  /></a>
</p>

*hopsworks* is the python API for interacting with a Hopsworks cluster.

## Getting Started On Hopsworks

Instantiate a connection and get the project object
```python
import hopsworks

connection = hopsworks.connection()

project = connection.get_project("my_project")


```

Create a new project
```python
project = connection.create_project("my_project")
```

Upload data to a project
```python
dataset_api = project.get_dataset_api()

dataset_api.upload("data.csv", "Resources")
```





You can find more examples on how to use the library in our [hops-examples](https://github.com/logicalclocks/hops-examples) repository.

## Documentation

Documentation is available at [Hopsworks Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks and the Feature Store please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/hopsworks-api/issues).

## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).

