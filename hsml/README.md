# Hopsworks Model Management

<p align="center">
  <a href="https://community.hopsworks.ai"><img
    src="https://img.shields.io/discourse/users?label=Hopsworks%20Community&server=https%3A%2F%2Fcommunity.hopsworks.ai"
    alt="Hopsworks Community"
  /></a>
    <a href="https://docs.hopsworks.ai"><img
    src="https://img.shields.io/badge/docs-HSML-orange"
    alt="Hopsworks Model Management Documentation"
  /></a>
  <a><img
    src="https://img.shields.io/badge/python-3.8+-blue"
    alt="python"
  /></a>
  <a href="https://pypi.org/project/hsml/"><img
    src="https://img.shields.io/pypi/v/hsml?color=blue"
    alt="PyPiStatus"
  /></a>
  <a href="https://archiva.hops.works/#artifact/com.logicalclocks/hsml"><img
    src="https://img.shields.io/badge/java-HSML-green"
    alt="Scala/Java Artifacts"
  /></a>
  <a href="https://pepy.tech/project/hsml/month"><img
    src="https://pepy.tech/badge/hsml/month"
    alt="Downloads"
  /></a>
  <a href=https://github.com/astral-sh/ruff><img
    src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"
    alt="Ruff"
  /></a>
  <a><img
    src="https://img.shields.io/pypi/l/hsml?color=green"
    alt="License"
  /></a>
</p>

HSML is the library to interact with the Hopsworks Model Registry and Model Serving. The library makes it easy to export, manage and deploy models.

However, to connect from an external Python environment additional connection information, such as host and port, is required.

## Getting Started On Hopsworks

Get started easily by registering an account on [Hopsworks Serverless](https://app.hopsworks.ai/). Create your project and a [new Api key](https://docs.hopsworks.ai/latest/user_guides/projects/api_key/create_api_key/). In a new python environment with Python 3.8 or higher, install the [client library](https://docs.hopsworks.ai/latest/user_guides/client_installation/) using pip:

```bash
# Get all Hopsworks SDKs: Feature Store, Model Serving and Platform SDK
pip install hopsworks
# or just the Model Registry and Model Serving SDK
pip install hsml
```

You can start a notebook and instantiate a connection and get the project feature store handler.

```python
import hopsworks

project = hopsworks.login() # you will be prompted for your api key

mr = project.get_model_registry()
# or
ms = project.get_model_serving()
```

or using `hsml` directly:

```python
import hsml

connection = hsml.connection(
    host="c.app.hopsworks.ai", #
    project="your-project",
    api_key_value="your-api-key",
)

mr = connection.get_model_registry()
# or
ms = connection.get_model_serving()
```

Create a new model
```python
model = mr.tensorflow.create_model(name="mnist",
                                   version=1,
                                   metrics={"accuracy": 0.94},
                                   description="mnist model description")
model.save("/tmp/model_directory") # or /tmp/model_file
```

Download a model
```python
model = mr.get_model("mnist", version=1)

model_path = model.download()
```

Delete a model
```python
model.delete()
```

Get best performing model
```python
best_model = mr.get_best_model('mnist', 'accuracy', 'max')

```

Deploy a model
```python
deployment = model.deploy()
```

Start a deployment
```python
deployment.start()
```

Make predictions with a deployed model
```python
data = { "instances": [ model.input_example ] }

predictions = deployment.predict(data)
```

# Tutorials

You can find more examples on how to use the library in our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials).

## Documentation

Documentation is available at [Hopsworks Model Management Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks Machine Learning please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).
Please report any issue using [Github issue tracking](https://github.com/logicalclocks/machine-learning-api/issues).


## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).
