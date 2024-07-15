# Provenance Links

Provenance Links are objects returned by methods such as [get_feature_view_provenance](../model_api/#get_feature_view_provenance), [get_training_dataset_provenance](../model_api/#get_training_dataset_provenance). These methods use the provenance graph to return the parent feature view/training dataset of a model. These methods will return the actual instances of the feature view/training dataset if available. If the instance was deleted, or it belongs to a featurestore that the current project doesn't have access anymore, an Artifact object is returned.

There is an additional method using the provenance graph: [get_feature_view](../model_api/#get_feature_view). This method wraps the `get_feature_view_provenance` and always returns a correct, usable Feature View object or throws an exception if the returned object is an Artifact. Thus an exception is thrown if the feature view was deleted or the featurestore it belongs to was unshared.
## Properties

{{links_properties}}

# Artifact

Artifacts objects are part of the provenance graph and contain a minimal set of information regarding the entities (feature views, training datasets) they represent.
The provenance graph contains Artifact objects when the underlying entities have been deleted or they are corrupted or they are not accessible by the current project anymore.

{{artifact_properties}}
