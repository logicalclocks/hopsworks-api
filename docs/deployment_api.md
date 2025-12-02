# Deployment

You can obtain a `ModelServing` using [`Project.get_model_serving`][hopsworks.project.Project.get_model_serving].
Then you can create a deployment using [`ModelServing.create_deployment`][hsml.model_serving.ModelServing.create_deployment], and retrieve existing deployments using [`ModelServing.get_deployment`][hsml.model_serving.ModelServing.get_deployment], [`ModelServing.get_deployment_by_id`][hsml.model_serving.ModelServing.get_deployment_by_id], and [`ModelServing.get_deployments`][hsml.model_serving.ModelServing.get_deployments].

You can also create a deployment by deploying a model via [`Model.deploy`][hsml.model.Model.deploy] or from a predictor via [`Predictor.deploy`][hsml.model_serving.Predictor.deploy].

::: hsml.deployment.Deployment
