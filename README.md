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
  - Insert dataframes into the online or offline Store, create training datasets or *serve real-time* feature vectors in the Feature Store via the [Feature Store API](https://github.com/logicalclocks/feature-store-api). Already have data somewhere you want to import, checkout our [Storage Connectors](https://docs.hopsworks.ai/latest/user_guides/fs/storage_connector/) documentation.
  - register ML models in the model registry and *deploy* them via model serving via the [Machine Learning API](https://gitub.com/logicalclocks/machine-learning-api).
  - manage environments, executions, kafka topics and more once you deploy your own Hopsworks cluster, either on-prem or in the cloud. Hopsworks is open-source and has its own [Community Edition](https://github.com/logicalclocks/hopsworks).

Our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials) cover a wide range of use cases and example of what *you* can build using Hopsworks.

## Getting Started On Hopsworks

Once you created a project on [Hopsworks Serverless](https://app.hopsworks.ai) and created a new [Api Key](https://docs.hopsworks.ai/latest/user_guides/projects/api_key/create_api_key/), just use your favourite virtualenv and package manager to install the library:

```bash
pip install hopsworks
```

Fire up a notebook and connect to your project, you will be prompted to enter your newly created API key:
```python
import hopsworks

project = hopsworks.login()
```

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

Or you can use the Machine Learning API to register models and deploy them for serving:
```python
mr = project.get_model_registry()
# or
ms = project.get_model_serving()
```

## Tutorials

Need more inspiration or want to learn more about the Hopsworks platform? Check out our [tutorials](https://github.com/logicalclocks/hopsworks-tutorials). 

## Documentation

Documentation is available at [Hopsworks Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks and the Feature Store please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/hopsworks-api/issues).

## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).

