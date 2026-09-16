# Batch inference — extended examples

Three complete pipelines beyond the canonical Pandas example in the parent skill: a forecast-horizon score with `spine_df`, PySpark with a spine group, and scoring via model→feature-view provenance.

## Complete Example: Scoring a Forecast Horizon with `spine_df`

Score every location for each of the next seven days, where the feature view's root feature group
holds only past observations and a joined feature group holds the forecast. A time range returns
nothing here; the rows have to be supplied.

```python
import datetime

import hopsworks
import joblib
import pandas as pd
from hsfs.constructor.prediction_times import PredictionTimes

project = hopsworks.login()
mr = project.get_model_registry()

# 1. Model
model = mr.get_model("air_quality_xgboost_model", version=1)
clf = joblib.load(f"{model.download()}/model.pkl")

# 2. Feature view from the model's provenance. This calls init_batch_scoring() with the
#    training dataset version the model was trained on, so the transformation statistics
#    cannot drift from the model the way a hardcoded version can.
fv = model.get_feature_view(init=True, online=False)

# 3. The view was created with max_feature_age={"weather": timedelta(days=1)}, so a day with
#    no forecast comes back NULL rather than inheriting the previous day's weather silently.

# 4. The rows to score: every location, every day of the horizon
tomorrow = datetime.date.today() + datetime.timedelta(days=1)
locations = pd.DataFrame(
    [
        {"country": "sweden", "city": "stockholm", "street": "sveavagen"},
        {"country": "sweden", "city": "gothenburg", "street": "avenyn"},
    ]
)
schedule = PredictionTimes.every("daily", offset="00:00", start=tomorrow, count=7)
spine_df = schedule.cross(locations, event_time="date")

# 5. Features as of each row's own prediction time
batch_df = fv.get_batch_data(
    spine_df=spine_df,
    primary_key=False,
    event_time=False,
    dataframe_type="pandas",
)

# 6. Rows come back in spine_df order, so predictions zip back positionally
predictions = spine_df.copy()
predictions["predicted_pm25"] = clf.predict(batch_df)
```

A row whose weather is older than the bound comes back `NULL` rather than carrying the previous
day forward, so a gap in the forecast is visible instead of silently becoming a prediction.

---

## Complete Example: PySpark Batch Inference with Spine Group

```python
import hopsworks
from pyspark.sql import SparkSession

# Spark Connect session with Delta extensions + DeltaCatalog (mandatory for
# Hopsworks offline feature group reads/writes — see hops-spark skill).
spark = (
    SparkSession.builder.appName("batch_inference")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# 1. Connect
project = hopsworks.login()
mr = project.get_model_registry()
fs = project.get_feature_store()

# 2. Download model
model_meta = mr.get_model("fraud_detector", version=1)
model_dir = model_meta.download()

# 3. Load model and broadcast to Spark executors
import joblib
model = joblib.load(f"{model_dir}/model.pkl")
bc_model = spark.sparkContext.broadcast(model)

# 4. Create spine with entities to score
scoring_entities = spark.sql("""
    SELECT user_id, current_timestamp() as prediction_time
    FROM active_users
    WHERE last_active > date_sub(current_date(), 1)
""")

spine_group = fs.get_or_create_spine_group(
    name="daily_scoring_spine",
    version=1,
    primary_key=["user_id"],
    event_time="prediction_time",
    dataframe=scoring_entities,
)

# 5. Get feature view and retrieve batch data with spine
fv = fs.get_feature_view("fraud_features_fv", version=1)
fv.init_batch_scoring(training_dataset_version=1)

batch_df = fv.get_batch_data(
    spine=spine_group,
    dataframe_type="spark",
)

# 6. Apply model using Spark UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def predict_udf(*features: pd.Series) -> pd.Series:
    import numpy as np
    X = np.column_stack([f.values for f in features])
    return pd.Series(bc_model.value.predict_proba(X)[:, 1])

feature_columns = [c for c in batch_df.columns if c != "user_id"]
predictions_df = batch_df.withColumn(
    "fraud_probability",
    predict_udf(*[batch_df[c] for c in feature_columns])
)

predictions_df.show()
```

---

## Complete Example: Using Model-Feature View Provenance

```python
import hopsworks
import joblib

# 1. Connect
project = hopsworks.login()
mr = project.get_model_registry()

# 2. Get model (linked to feature view at training time)
model_meta = mr.get_model("fraud_detector", version=1)
model_dir = model_meta.download()
model = joblib.load(f"{model_dir}/model.pkl")

# 3. Get the feature view directly from the model
#    init_batch_scoring() is called automatically with the correct training_dataset_version
fv = model_meta.get_feature_view(init=True, online=False)

# 4. Score — drop identifier columns the FV carries (primary key + event time);
#    they are not model inputs (same as the Pandas example above).
batch_df = fv.get_batch_data(dataframe_type="pandas")
feature_cols = [c for c in batch_df.columns if c not in ("customer_id", "event_time")]
predictions = model.predict(batch_df[feature_cols])
print(predictions)
```
