# Feature Monitoring Configuration

Yoou can create a feature monitoring configuration from a feature group using [`FeatureGroup.create_statistics_monitoring`][hsfs.feature_group.FeatureGroup.create_statistics_monitoring] or [`FeatureGroup.create_feature_monitoring`][hsfs.feature_group.FeatureGroup.create_feature_monitoring]; and with [`FeatureView.create_statistics_monitoring`][hsfs.feature_view.FeatureView.create_statistics_monitoring] or [`FeatureView.create_feature_monitoring`][hsfs.feature_view.FeatureView.create_feature_monitoring] to create it from a feature view.
You can retrieve an existing feature monitoring configuration by calling [`FeatureGroup.get_feature_monitoring_configs`][hsfs.feature_group.FeatureGroup.get_feature_monitoring_configs] and [`FeatureView.get_feature_monitoring_configs`][hsfs.feature_view.FeatureView.get_feature_monitoring_configs].

::: hsfs.core.feature_monitoring_config.FeatureMonitoringConfig
