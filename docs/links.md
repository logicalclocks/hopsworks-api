# Provenance Links

Provenance Links are objects returned by methods such as [`StorageConnector.get_feature_groups_provenance`][hsfs.storage_connector.StorageConnector.get_feature_groups_provenance], [`FeatureGroup.get_storage_connector_provenance`][hsfs.feature_group.FeatureGroup.get_storage_connector_provenance], [`FeatureGroup.get_parent_feature_group`][hsfs.feature_group.FeatureGroup.get_parent_feature_groups], [`FeatureGroup.get_generated_feature_groups`][hsfs.feature_group.FeatureGroup.get_generated_feature_groups], [`FeatureGroup.get_generated_feature_views`][hsfs.feature_group.FeatureGroup.get_generated_feature_views], [`FeatureView.get_models_provenance`][hsfs.feature_view.FeatureView.get_models_provenance] and represent sections of the provenance graph, depending on the method invoked.

::: hsfs.core.explicit_provenance.Links

## Artifact

Artifacts objects are part of the provenance graph and contain a minimal set of information regarding the entities (feature groups, feature views) they represent.
The provenance graph contains Artifact objects when the underlying entities have been deleted or they are corrupted or they are not accessible by the user.

::: hsfs.core.explicit_provenance.Artifact
