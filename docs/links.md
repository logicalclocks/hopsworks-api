# Provenance Links

Provenance Links are objects returned by methods such as [`StorageConnectorApi.get_feature_groups_provenance`][hsfs.core.storage_connector_api.StorageConnectorApi.get_feature_groups_provenance], [`FeatureGroupApi.get_storage_connector_provenance`][hsfs.core.feature_group_api.FeatureGroupApi.get_storage_connector_provenance], [`FeatureGroupApi.get_parent_feature_group`][hsfs.core.feature_group_api.FeatureGroupApi.get_parent_feature_groups], [`FeatureGroupApi.get_generated_feature_groups`][hsfs.core.FeatureGroupApi.get_generated_feature_groups], [`FeatureGroupApi.get_generated_feature_views`][hsfs.core.feature_group_api.FeatureGroupApi.get_generated_feature_views] [`FeatureViewApi.get_models_provenance`][hsfs.core.feature_view_api.FeatureViewApi.get_models_provenance] and represent sections of the provenance graph, depending on the method invoked.

::: hsfs.core.explicit_provenance.Links

## Artifact

Artifacts objects are part of the provenance graph and contain a minimal set of information regarding the entities (feature groups, feature views) they represent.
The provenance graph contains Artifact objects when the underlying entities have been deleted or they are corrupted or they are not accessible by the user.

::: hsfs.core.explicit_provenance.Artifact
