# Batch inference — extended examples

Two complete pipelines beyond the canonical Pandas example in the parent skill: PySpark with a spine group, and scoring via model→feature-view provenance.

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
