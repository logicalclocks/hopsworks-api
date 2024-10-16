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
  <a><img
    src="https://img.shields.io/badge/python-3.8+-blue"
    alt="python"
  /></a>
  <a href="https://pypi.org/project/hopsworks/"><img
    src="https://img.shields.io/pypi/v/hopsworks?color=blue"
    alt="PyPiStatus"
  /></a>
  <a href="https://archiva.hops.works/#artifact/com.logicalclocks/hopsworks"><img
    src="https://img.shields.io/badge/java-HOPSWORKS-green"
    alt="Scala/Java Artifacts"
  /></a>
  <a href="https://pepy.tech/project/hopsworks/month"><img
    src="https://pepy.tech/badge/hopsworks/month"
    alt="Downloads"
  /></a>
  <a href=https://github.com/astral-sh/ruff><img
    src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"
    alt="Ruff"
  /></a>
  <a><img
    src="https://img.shields.io/pypi/l/hopsworks?color=green"
    alt="License"
  /></a>
</p>

*hopsworks* is the python API for interacting with a Hopsworks cluster. Don't have a Hopsworks cluster just yet? Register an account on [Hopsworks Serverless](https://app.hopsworks.ai/) and get started for free. Once connected to your project, you can:

- Insert dataframes into the online or offline Store, create training datasets or *serve real-time* feature vectors in the Feature Store via the Feature Store API. Already have data somewhere you want to import, checkout our [Storage Connectors](https://docs.hopsworks.ai/latest/user_guides/fs/storage_connector/) documentation.
- register ML models in the model registry and *deploy* them via model serving via the Machine Learning API.
- manage environments, executions, kafka topics and more once you deploy your own Hopsworks cluster, either on-prem or in the cloud. Hopsworks is open-source and has its own [Community Edition](https://github.com/logicalclocks/hopsworks).

Our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials) cover a wide range of use cases and example of what *you* can build using Hopsworks.

## Getting Started On Hopsworks

Once you created a project on [Hopsworks Serverless](https://app.hopsworks.ai) and created a new [Api Key](https://docs.hopsworks.ai/latest/user_guides/projects/api_key/create_api_key/), just use your favourite virtualenv and package manager to install the library:

```bash
pip install "hopsworks[python]"
```

Fire up a notebook and connect to your project, you will be prompted to enter your newly created API key:

```python
import hopsworks

project = hopsworks.login()
```

### Feature Store API

Access the Feature Store of your project to use as a central repository for your feature data. Use *your* favourite data engineering library (pandas, polars, Spark, etc...) to insert data into the Feature Store, create training datasets or serve real-time feature vectors. Want to predict likelyhood of e-scooter accidents in real-time? Here's how you can do it:

```python
fs = project.get_feature_store()

# Write to Feature Groups
bike_ride_fg = fs.get_or_create_feature_group(
  name="bike_rides",
  version=1,
  primary_key=["ride_id"],
  event_time="activation_time",
  online_enabled=True,
)

fg.insert(bike_rides_df)

# Read from Feature Views
profile_fg = fs.get_feature_group("user_profile", version=1)

bike_ride_fv = fs.get_or_create_feature_view(
  name="bike_rides_view",
  version=1,
  query=bike_ride_fg.select_except(["ride_id"]).join(profile_fg.select(["age", "has_license"]), on="user_id")
)

bike_rides_Q1_2021_df = bike_ride_fv.get_batch_data(
  start_date="2021-01-01",
  end_date="2021-01-31"
)

# Create a training dataset
version, job = bike_ride_fv.create_train_test_split(
    test_size=0.2,
    description='Description of a dataset',
    # you can have different data formats such as csv, tsv, tfrecord, parquet and others
    data_format='csv'
)

# Predict the probability of accident in real-time using new data + context data
bike_ride_fv.init_serving()

while True:
    new_ride_vector = poll_ride_queue()
    feature_vector = bike_ride_fv.get_online_feature_vector(
      {"user_id": new_ride_vector["user_id"]},
      passed_features=new_ride_vector
    )
    accident_probability = model.predict(feature_vector)
```

The API enables interaction with the Hopsworks Feature Store. It makes creating new features, feature groups and training datasets easy.

The API is environment independent and can be used in two modes:

- Spark mode: For data engineering jobs that create and write features into the feature store or generate training datasets. It requires a Spark environment such as the one provided in the Hopsworks platform or Databricks. In Spark mode, HSFS provides bindings both for Python and JVM languages.

- Python mode: For data science jobs to explore the features available in the feature store, generate training datasets and feed them in a training pipeline. Python mode requires just a Python interpreter and can be used both in Hopsworks from Python Jobs/Jupyter Kernels, Amazon SageMaker or KubeFlow.

Scala API is also available, here is a short sample of it:

```scala
import com.logicalclocks.hsfs._
val connection = HopsworksConnection.builder().build()
val fs = connection.getFeatureStore();
val attendances_features_fg = fs.getFeatureGroup("games_features", 1);
attendances_features_fg.show(1)
```

### Machine Learning API

Or you can use the Machine Learning API to interact with the Hopsworks Model Registry and Model Serving. The API makes it easy to export, manage and deploy models. For example, to register models and deploy them for serving you can do:

```python
mr = project.get_model_registry()
# or
ms = connection.get_model_serving()

# Create a new model:
model = mr.tensorflow.create_model(name="mnist",
                                   version=1,
                                   metrics={"accuracy": 0.94},
                                   description="mnist model description")
model.save("/tmp/model_directory") # or /tmp/model_file

# Download a model:
model = mr.get_model("mnist", version=1)
model_path = model.download()

# Delete the model:
model.delete()

# Get the best-performing model
best_model = mr.get_best_model('mnist', 'accuracy', 'max')

# Deploy the model:
deployment = model.deploy()
deployment.start()

# Make predictions with a deployed model
data = { "instances": [ model.input_example ] }
predictions = deployment.predict(data)
```

## Usage

Usage data is collected for improving quality of the library.
It is turned on by default if the backend is [Hopsworks Serverless](https://c.app.hopsworks.ai).
To turn it off, use one of the following ways:
```python
# use environment variable
import os
os.environ["ENABLE_HOPSWORKS_USAGE"] = "false"

# use `disable_usage_logging`
import hopsworks
hopsworks.disable_usage_logging()
```

The corresponding source code is in `python/hopsworks_common/usage.py`.

## Tutorials

Need more inspiration or want to learn more about the Hopsworks platform? Check out our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials).

## Documentation

Documentation is available at [Hopsworks Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks and the Feature Store please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/hopsworks-api/issues) and attach the client environment from the output below to your issue:

```python
import hopsworks
hopsworks.login()
print(hopsworks.get_sdk_info())
```

## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).
