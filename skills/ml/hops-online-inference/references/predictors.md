# predictor.py variants

Copy-paste `predictor.py` skeletons for the four lookup patterns. Each loads its model with the `load_model_file` helper from the parent skill's "Writing predictor.py Files" section.

### Basic Predictor

```python
# predictor.py
import os
import joblib

class Predict:
    def __init__(self):
        """Called once when the deployment starts.
        
        Load model, initialize feature view, set up resources here.
        The model files are available in the current working directory.
        """
        self.model = joblib.load(load_model_file("model.pkl"))

    def predict(self, inputs):
        """Called for each inference request.
        
        Parameters:
            inputs: List of input instances (list of lists)
            
        Returns:
            dict with "predictions" key containing the results
        """
        return {"predictions": self.model.predict(inputs).tolist()}
```

### Predictor with Feature Store Lookup

The most common pattern: look up precomputed features from the online feature store, then predict.

```python
# predictor.py
import os
import joblib
import hopsworks

class Predict:
    def __init__(self):
        # Load model
        self.model = joblib.load(load_model_file("model.pkl"))
        
        # Connect to feature store
        project = hopsworks.login()
        fs = project.get_feature_store()
        
        # Initialize feature view for online serving
        self.fv = fs.get_feature_view("fraud_features_fv", version=1)
        self.fv.init_serving(training_dataset_version=1)

    def predict(self, inputs):
        """inputs: list of dicts with primary keys, e.g. [{"user_id": 123}]"""
        # Look up precomputed features from online store
        feature_vectors = self.fv.get_feature_vectors(
            entry=inputs,
            return_type="pandas",
        )
        
        # Predict
        predictions = self.model.predict(feature_vectors).tolist()
        return {"predictions": predictions}
```

### Predictor with On-Demand Features

Combine precomputed features with on-demand features computed at request time:

```python
# predictor.py
import os
import joblib
import hopsworks

class Predict:
    def __init__(self):
        self.model = joblib.load(load_model_file("model.pkl"))
        
        project = hopsworks.login()
        fs = project.get_feature_store()
        
        self.fv = fs.get_feature_view("recommendation_fv", version=1)
        self.fv.init_serving(training_dataset_version=1)

    def predict(self, inputs):
        """inputs: list of dicts with primary keys AND request parameters.
        
        Example: [{"user_id": 123, "query_text": "running shoes", "current_location": "NYC"}]
        """
        entries = []
        request_params = []
        
        for inp in inputs:
            # Separate primary keys from request parameters
            entries.append({"user_id": inp["user_id"]})
            request_params.append({
                "query_text": inp.get("query_text", ""),
                "current_location": inp.get("current_location", ""),
            })
        
        # Get feature vectors with on-demand features computed
        feature_vectors = self.fv.get_feature_vectors(
            entry=entries,
            request_parameters=request_params,
            return_type="pandas",
        )
        
        predictions = self.model.predict(feature_vectors).tolist()
        return {"predictions": predictions}
```

### Predictor with Passed Features

Provide feature values from the application that override or supplement stored features:

```python
# predictor.py
class Predict:
    def __init__(self):
        # ... init model and feature view ...
        pass

    def predict(self, inputs):
        """inputs: [{"user_id": 123, "session_duration": 45.2, "device": "mobile"}]"""
        entries = []
        passed = []
        
        for inp in inputs:
            entries.append({"user_id": inp["user_id"]})
            passed.append({
                "session_duration": inp["session_duration"],
                "device": inp["device"],
            })
        
        vectors = self.fv.get_feature_vectors(
            entry=entries,
            passed_features=passed,
            return_type="pandas",
        )
        return {"predictions": self.model.predict(vectors).tolist()}
```
