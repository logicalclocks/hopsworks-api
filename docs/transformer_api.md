# Transformer

You can obtain a `ModelServing` handle using [`Project.get_model_serving`][hopsworks.project.Project.get_model_serving].
Once you have it, you can create a transformer using [`ModelServing.create_transformer`][hsml.model_serving.ModelServing.create_transformer].

Transformers can be accessed from the [`Predictor`][hsml.predictor.Predictor] metadata objects.

``` python
predictor.transformer
```

::: hsml.transformer.Transformer
