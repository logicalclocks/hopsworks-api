# Predictor

You can get a `ModelServing` instance using [`Project.get_model_serving`][hopsworks.project.Project.get_model_serving].
Once you have it, you can create a predictor using [`ModelServing.create_predictor`][hsml.model_serving.ModelServing.create_predictor].
Predictors can also be accessed from the [`Deployment`][hsml.deployment.Deployment] metadata objects:

```python
deployment.predictor
```

::: hsml.predictor.Predictor
