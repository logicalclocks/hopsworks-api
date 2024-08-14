#
#   Copyright 2022 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import os
import pathlib
import shutil

import keras_autodoc


JSON_METHODS = [
    "extract_fields_from_json",
    "from_json",
    "from_response_json",
    "json",
    "update_from_response_json",
]

PAGES = {
    "api/login.md": {
        "login": ["hopsworks.login"],
        "get_current_project": ["hopsworks.get_current_project"],
        "fs_api": ["hopsworks.project.Project.get_feature_store"],
        "mr_api": ["hopsworks.project.Project.get_model_registry"],
        "ms_api": ["hopsworks.project.Project.get_model_serving"],
    },
    "api/udf.md": {
        "udf": ["hopsworks.udf"],
    },
    "api/connection.md": {
        "connection_create": ["hopsworks.connection.Connection.connection"],
        "connection_properties": keras_autodoc.get_properties(
            "hopsworks.connection.Connection"
        ),
        "connection_methods": keras_autodoc.get_methods(
            "hopsworks.connection.Connection", exclude=["from_response_json", "json"]
        ),
    },
    "api/projects.md": {
        "project_create": ["hopsworks.create_project"],
        "project_properties": keras_autodoc.get_properties("hopsworks.project.Project"),
        "project_methods": keras_autodoc.get_methods(
            "hopsworks.project.Project", exclude=["from_response_json", "json"]
        ),
    },
    "api/jobs.md": {
        "job_api_handle": ["hopsworks.project.Project.get_job_api"],
        "job_create": ["hopsworks.core.job_api.JobApi.create_job"],
        "job_get": ["hopsworks.core.job_api.JobApi.get_job"],
        "job_get_all": ["hopsworks.core.job_api.JobApi.get_jobs"],
        "job_properties": keras_autodoc.get_properties("hopsworks.job.Job"),
        "job_config": ["hopsworks.core.job_api.JobApi.get_configuration"],
        "job_methods": keras_autodoc.get_methods(
            "hopsworks.job.Job", exclude=["from_response_json", "json"]
        ),
    },
    "api/executions.md": {
        "execution_create": ["hopsworks.job.Job.run"],
        "execution_get": ["hopsworks.job.Job.get_executions"],
        "execution_properties": keras_autodoc.get_properties(
            "hopsworks.execution.Execution"
        ),
        "execution_methods": keras_autodoc.get_methods(
            "hopsworks.execution.Execution",
            exclude=["from_response_json", "json", "update_from_response_json"],
        ),
    },
    "api/flink_cluster.md": {
        "flink_api_handle": ["hopsworks.project.Project.get_flink_cluster_api"],
        "setup_cluster": [
            "hopsworks.core.flink_cluster_api.FlinkClusterApi.setup_cluster"
        ],
        "get_cluster": ["hopsworks.core.flink_cluster_api.FlinkClusterApi.get_cluster"],
        "start_cluster": ["hopsworks.flink_cluster.FlinkCluster.start"],
        "submit_job_to_cluster": ["hopsworks.flink_cluster.FlinkCluster.submit_job"],
        "flink_cluster_properties": keras_autodoc.get_properties(
            "hopsworks.flink_cluster.FlinkCluster"
        ),
        "flink_cluster_methods": keras_autodoc.get_methods(
            "hopsworks.flink_cluster.FlinkCluster",
            exclude=["from_response_json", "json"],
        ),
    },
    "api/environment.md": {
        "env_api_handle": ["hopsworks.project.Project.get_environment_api"],
        "env_create": [
            "hopsworks.core.environment_api.EnvironmentApi.create_environment"
        ],
        "env_get": ["hopsworks.core.environment_api.EnvironmentApi.get_environment"],
        "env_methods": keras_autodoc.get_methods(
            "hopsworks.environment.Environment", exclude=["from_response_json", "json"]
        ),
    },
    "api/git_repo.md": {
        "git_api_handle": ["hopsworks.project.Project.get_git_api"],
        "git_repo_clone": ["hopsworks.core.git_api.GitApi.clone"],
        "git_repo_get": ["hopsworks.core.git_api.GitApi.get_repo"],
        "git_repo_get_all": ["hopsworks.core.git_api.GitApi.get_repos"],
        "git_repo_properties": keras_autodoc.get_properties(
            "hopsworks.git_repo.GitRepo"
        ),
        "git_repo_methods": keras_autodoc.get_methods(
            "hopsworks.git_repo.GitRepo", exclude=["from_response_json", "json"]
        ),
    },
    "api/git_provider.md": {
        "git_api_handle": ["hopsworks.project.Project.get_git_api"],
        "git_provider_create": ["hopsworks.core.git_api.GitApi.set_provider"],
        "git_provider_get": ["hopsworks.core.git_api.GitApi.get_provider"],
        "git_provider_get_all": ["hopsworks.core.git_api.GitApi.get_providers"],
        "git_provider_properties": keras_autodoc.get_properties(
            "hopsworks.git_provider.GitProvider"
        ),
        "git_provider_methods": keras_autodoc.get_methods(
            "hopsworks.git_provider.GitProvider", exclude=["from_response_json", "json"]
        ),
    },
    "api/git_remote.md": {
        "git_api_handle": ["hopsworks.project.Project.get_git_api"],
        "git_remote_create": ["hopsworks.git_repo.GitRepo.add_remote"],
        "git_remote_get": ["hopsworks.git_repo.GitRepo.get_remote"],
        "git_remote_get_all": ["hopsworks.git_repo.GitRepo.get_remotes"],
        "git_remote_properties": keras_autodoc.get_properties(
            "hopsworks.git_remote.GitRemote"
        ),
        "git_remote_methods": keras_autodoc.get_methods(
            "hopsworks.git_remote.GitRemote", exclude=["from_response_json", "json"]
        ),
    },
    "api/datasets.md": {
        "dataset_api_handle": ["hopsworks.project.Project.get_dataset_api"],
        "dataset_methods": keras_autodoc.get_methods(
            "hopsworks.core.dataset_api.DatasetApi"
        ),
    },
    "api/kafka_topic.md": {
        "kafka_api_handle": ["hopsworks.project.Project.get_kafka_api"],
        "kafka_config": ["hopsworks.core.kafka_api.KafkaApi.get_default_config"],
        "kafka_topic_create": ["hopsworks.core.kafka_api.KafkaApi.create_topic"],
        "kafka_topic_get": ["hopsworks.core.kafka_api.KafkaApi.get_topic"],
        "kafka_topic_get_all": ["hopsworks.core.kafka_api.KafkaApi.get_topics"],
        "kafka_topic_properties": keras_autodoc.get_properties(
            "hopsworks.kafka_topic.KafkaTopic"
        ),
        "kafka_topic_methods": keras_autodoc.get_methods(
            "hopsworks.kafka_topic.KafkaTopic",
            exclude=["from_response_json", "json", "update_from_response_json"],
        ),
    },
    "api/kafka_schema.md": {
        "kafka_api_handle": ["hopsworks.project.Project.get_kafka_api"],
        "kafka_schema_create": ["hopsworks.core.kafka_api.KafkaApi.create_schema"],
        "kafka_schema_get": ["hopsworks.core.kafka_api.KafkaApi.get_schema"],
        "kafka_schema_get_all": ["hopsworks.core.kafka_api.KafkaApi.get_schemas"],
        "kafka_schema_get_subjects": ["hopsworks.core.kafka_api.KafkaApi.get_subjects"],
        "kafka_schema_properties": keras_autodoc.get_properties(
            "hopsworks.kafka_schema.KafkaSchema"
        ),
        "kafka_schema_methods": keras_autodoc.get_methods(
            "hopsworks.kafka_schema.KafkaSchema",
            exclude=["from_response_json", "json", "update_from_response_json"],
        ),
    },
    "api/secrets.md": {
        "secret_api_handle": ["hopsworks.get_secrets_api"],
        "secret_create": ["hopsworks.core.secret_api.SecretsApi.create_secret"],
        "secret_get": ["hopsworks.core.secret_api.SecretsApi.get_secret"],
        "secret_get_simplified": ["hopsworks.core.secret_api.SecretsApi.get"],
        "secret_get_all": ["hopsworks.core.secret_api.SecretsApi.get_secrets"],
        "secret_properties": keras_autodoc.get_properties("hopsworks.secret.Secret"),
        "secret_methods": keras_autodoc.get_methods(
            "hopsworks.secret.Secret", exclude=["from_response_json", "json"]
        ),
    },
    "api/opensearch.md": {
        "opensearch_api_handle": ["hopsworks.project.Project.get_opensearch_api"],
        "opensearch_methods": keras_autodoc.get_methods(
            "hopsworks.core.opensearch_api.OpenSearchApi"
        ),
    },
    "api/connection_api.md": {
        "connection": ["hsfs.connection.Connection"],
        "connection_properties": keras_autodoc.get_properties(
            "hsfs.connection.Connection"
        ),
        "connection_methods": keras_autodoc.get_methods("hsfs.connection.Connection"),
    },
    "api/spine_group_api.md": {
        "fg": ["hsfs.feature_group.SpineGroup"],
        "fg_create": ["hsfs.feature_store.FeatureStore.get_or_create_spine_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_or_create_spine_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.SpineGroup",
            exclude=[
                "expectation_suite",
                "location",
                "online_enabled",
                "statistics",
                "statistics_config",
                "subject",
            ],
        ),
        "fg_methods": keras_autodoc.get_methods(
            "hsfs.feature_group.SpineGroup",
            exclude=[
                "append_features",
                "compute_statistics",
                "delete_expectation_suite",
                "from_response_json",
                "get_all_validation_reports",
                "get_expectation_suite",
                "get_latest_validation_report",
                "get_statistics",
                "get_validation_history",
                "save_expectation_suite",
                "save_validation_report",
                "update_from_response_json",
                "update_statistics_config",
                "validate",
            ],
        ),
    },
    "api/training_dataset_api.md": {
        "td": ["hsfs.training_dataset.TrainingDataset"],
        "td_create": ["hsfs.feature_store.FeatureStore.create_training_dataset"],
        "td_get": ["hsfs.feature_store.FeatureStore.get_training_dataset"],
        "td_properties": keras_autodoc.get_properties(
            "hsfs.training_dataset.TrainingDataset"
        ),
        "td_methods": keras_autodoc.get_methods(
            "hsfs.training_dataset.TrainingDataset"
        ),
    },
    "api/feature_view_api.md": {
        "fv": ["hsfs.feature_view.FeatureView"],
        "fv_create": ["hsfs.feature_store.FeatureStore.create_feature_view"],
        "fv_get": ["hsfs.feature_store.FeatureStore.get_feature_view"],
        "fvs_get": ["hsfs.feature_store.FeatureStore.get_feature_views"],
        "fv_properties": keras_autodoc.get_properties("hsfs.feature_view.FeatureView"),
        "fv_methods": keras_autodoc.get_methods("hsfs.feature_view.FeatureView"),
    },
    "api/feature_api.md": {
        "feature": ["hsfs.feature.Feature"],
        "feature_properties": keras_autodoc.get_properties("hsfs.feature.Feature"),
        "feature_methods": keras_autodoc.get_methods("hsfs.feature.Feature"),
    },
    "api/expectation_suite_api.md": {
        "expectation_suite": ["hsfs.expectation_suite.ExpectationSuite"],
        "expectation_suite_attach": [
            "hsfs.feature_group.FeatureGroup.save_expectation_suite"
        ],
        "single_expectation_api": [
            "hsfs.expectation_suite.ExpectationSuite.add_expectation",
            "hsfs.expectation_suite.ExpectationSuite.replace_expectation",
            "hsfs.expectation_suite.ExpectationSuite.remove_expectation",
        ],
        "expectation_suite_properties": keras_autodoc.get_properties(
            "hsfs.expectation_suite.ExpectationSuite"
        ),
        "expectation_suite_methods": keras_autodoc.get_methods(
            "hsfs.expectation_suite.ExpectationSuite"
        ),
    },
    "api/feature_store_api.md": {
        "fs": ["hsfs.feature_store.FeatureStore"],
        "fs_get": ["hsfs.connection.Connection.get_feature_store"],
        "fs_properties": keras_autodoc.get_properties(
            "hsfs.feature_store.FeatureStore"
        ),
        "fs_methods": keras_autodoc.get_methods("hsfs.feature_store.FeatureStore"),
    },
    "api/feature_group_api.md": {
        "fg": ["hsfs.feature_group.FeatureGroup"],
        "fg_create": [
            "hsfs.feature_store.FeatureStore.create_feature_group",
            "hsfs.feature_store.FeatureStore.get_or_create_feature_group",
        ],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.FeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods("hsfs.feature_group.FeatureGroup"),
    },
    "api/external_feature_group_api.md": {
        "fg": ["hsfs.feature_group.ExternalFeatureGroup"],
        "fg_create": ["hsfs.feature_store.FeatureStore.create_external_feature_group"],
        "fg_get": ["hsfs.feature_store.FeatureStore.get_external_feature_group"],
        "fg_properties": keras_autodoc.get_properties(
            "hsfs.feature_group.ExternalFeatureGroup"
        ),
        "fg_methods": keras_autodoc.get_methods(
            "hsfs.feature_group.ExternalFeatureGroup"
        ),
    },
    "api/storage_connector_api.md": {
        "sc_get": [
            "hsfs.feature_store.FeatureStore.get_storage_connector",
            "hsfs.feature_store.FeatureStore.get_online_storage_connector",
        ],
        "hopsfs_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.HopsFSConnector", exclude=["from_response_json"]
        ),
        "hopsfs_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.HopsFSConnector"
        ),
        "s3_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.S3Connector", exclude=["from_response_json"]
        ),
        "s3_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.S3Connector"
        ),
        "redshift_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.RedshiftConnector", exclude=["from_response_json"]
        ),
        "redshift_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.RedshiftConnector"
        ),
        "adls_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.AdlsConnector", exclude=["from_response_json"]
        ),
        "adls_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.AdlsConnector"
        ),
        "snowflake_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.SnowflakeConnector", exclude=["from_response_json"]
        ),
        "snowflake_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.SnowflakeConnector"
        ),
        "jdbc_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.JdbcConnector", exclude=["from_response_json"]
        ),
        "jdbc_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.JdbcConnector"
        ),
        "gcs_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.GcsConnector", exclude=["from_response_json"]
        ),
        "gcs_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.GcsConnector"
        ),
        "bigquery_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.BigQueryConnector", exclude=["from_response_json"]
        ),
        "bigquery_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.BigQueryConnector"
        ),
        "kafka_methods": keras_autodoc.get_methods(
            "hsfs.storage_connector.KafkaConnector", exclude=["from_response_json"]
        ),
        "kafka_properties": keras_autodoc.get_properties(
            "hsfs.storage_connector.KafkaConnector"
        ),
    },
    "api/statistics_config_api.md": {
        "statistics_config": ["hsfs.statistics_config.StatisticsConfig"],
        "statistics_config_properties": keras_autodoc.get_properties(
            "hsfs.statistics_config.StatisticsConfig"
        ),
    },
    "api/transformation_functions_api.md": {
        "transformation_function": [
            "hsfs.transformation_function.TransformationFunction"
        ],
        "transformation_function_properties": keras_autodoc.get_properties(
            "hsfs.transformation_function.TransformationFunction"
        ),
        "transformation_function_methods": keras_autodoc.get_methods(
            "hsfs.transformation_function.TransformationFunction",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        "create_transformation_function": [
            "hsfs.feature_store.FeatureStore.create_transformation_function"
        ],
        "get_transformation_function": [
            "hsfs.feature_store.FeatureStore.get_transformation_function"
        ],
        "get_transformation_functions": [
            "hsfs.feature_store.FeatureStore.get_transformation_functions"
        ],
    },
    "api/validation_report_api.md": {
        "validation_report": ["hsfs.validation_report.ValidationReport"],
        "validation_report_validate": [
            "hsfs.feature_group.FeatureGroup.validate",
            "hsfs.feature_group.FeatureGroup.insert",
        ],
        "validation_report_get": [
            "hsfs.feature_group.FeatureGroup.get_latest_validation_report",
            "hsfs.feature_group.FeatureGroup.get_all_validation_reports",
        ],
        "validation_report_properties": keras_autodoc.get_properties(
            "hsfs.validation_report.ValidationReport"
        ),
        "validation_report_methods": keras_autodoc.get_methods(
            "hsfs.validation_report.ValidationReport"
        ),
    },
    "api/job.md": {
        "job_configuration": ["hsfs.core.job_configuration.JobConfiguration"],
        "job": ["hsfs.core.job.Job"],
        "job_methods": [
            "hsfs.core.job.Job.get_state",
            "hsfs.core.job.Job.get_final_state",
        ],
    },
    "api/query_api.md": {
        "query_methods": keras_autodoc.get_methods(
            "hsfs.constructor.query.Query",
            exclude=["json", "to_dict"],
        ),
        "query_properties": keras_autodoc.get_properties(
            "hsfs.constructor.query.Query"
        ),
    },
    "api/links.md": {
        "links_properties": keras_autodoc.get_properties(
            "hsfs.core.explicit_provenance.Links"
        ),
        "artifact_properties": keras_autodoc.get_properties(
            "hsfs.core.explicit_provenance.Artifact"
        ),
    },
    "api/statistics_api.md": {
        "statistics": ["hsfs.statistics.Statistics"],
        "statistics_properties": keras_autodoc.get_properties(
            "hsfs.statistics.Statistics"
        ),
    },
    "api/split_statistics_api.md": {
        "split_statistics": ["hsfs.split_statistics.SplitStatistics"],
        "split_statistics_properties": keras_autodoc.get_properties(
            "hsfs.split_statistics.SplitStatistics"
        ),
    },
    "api/feature_descriptive_statistics_api.md": {
        "feature_descriptive_statistics": [
            "hsfs.core.feature_descriptive_statistics.FeatureDescriptiveStatistics"
        ],
        "feature_descriptive_statistics_properties": keras_autodoc.get_properties(
            "hsfs.core.feature_descriptive_statistics.FeatureDescriptiveStatistics"
        ),
    },
    "api/hopsworks_udf.md": {
        "hopsworks_udf": ["hsfs.hopsworks_udf.HopsworksUdf"],
        "hopsworks_udf_properties": keras_autodoc.get_properties(
            "hsfs.hopsworks_udf.HopsworksUdf"
        ),
        "transformation_feature": ["hsfs.hopsworks_udf.TransformationFeature"],
    },
    "api/transformation_statistics.md": {
        "transformation_statistics": [
            "hsfs.transformation_statistics.TransformationStatistics"
        ],
    },
    "api/feature_transformation_statistics.md": {
        "feature_transformation_statistics": [
            "hsfs.transformation_statistics.FeatureTransformationStatistics"
        ],
        "feature_transformation_statistics_properties": keras_autodoc.get_properties(
            "hsfs.transformation_statistics.FeatureTransformationStatistics"
        ),
    },
    "api/feature_monitoring_config_api.md": {
        "feature_monitoring_config": [
            "hsfs.core.feature_monitoring_config.FeatureMonitoringConfig"
        ],
        "feature_monitoring_config_properties": keras_autodoc.get_properties(
            "hsfs.core.feature_monitoring_config.FeatureMonitoringConfig"
        ),
        "feature_monitoring_config_methods": keras_autodoc.get_methods(
            "hsfs.core.feature_monitoring_config.FeatureMonitoringConfig",
            exclude=[
                "from_response_json",
                "update_from_response_json",
                "json",
                "to_dict",
            ],
        ),
        # from feature group
        "feature_monitoring_config_creation_fg": [
            "hsfs.feature_group.FeatureGroup.create_statistics_monitoring",
            "hsfs.feature_group.FeatureGroup.create_feature_monitoring",
        ],
        # from feature view
        "feature_monitoring_config_creation_fv": [
            "hsfs.feature_view.FeatureView.create_statistics_monitoring",
            "hsfs.feature_view.FeatureView.create_feature_monitoring",
        ],
        # retrieval
        "feature_monitoring_config_retrieval_fg": [
            "hsfs.feature_group.FeatureGroup.get_feature_monitoring_configs",
        ],
        "feature_monitoring_config_retrieval_fv": [
            "hsfs.feature_view.FeatureView.get_feature_monitoring_configs",
        ],
    },
    "api/feature_monitoring_result_api.md": {
        "feature_monitoring_result": [
            "hsfs.core.feature_monitoring_result.FeatureMonitoringResult"
        ],
        "feature_monitoring_result_retrieval": [
            "hsfs.core.feature_monitoring_config.FeatureMonitoringConfig.get_history"
        ],
        "feature_monitoring_result_properties": keras_autodoc.get_properties(
            "hsfs.core.feature_monitoring_result.FeatureMonitoringResult"
        ),
    },
    "api/feature_monitoring_window_config_api.md": {
        "feature_monitoring_window_config": [
            "hsfs.core.monitoring_window_config.MonitoringWindowConfig"
        ],
        "feature_monitoring_window_config_properties": keras_autodoc.get_properties(
            "hsfs.core.monitoring_window_config.MonitoringWindowConfig"
        ),
    },
    "api/embedding_index_api.md": {
        "embedding_index": ["hsfs.embedding.EmbeddingIndex"],
        "embedding_index_properties": keras_autodoc.get_properties(
            "hsfs.embedding.EmbeddingIndex"
        ),
        "embedding_index_methods": keras_autodoc.get_methods(
            "hsfs.embedding.EmbeddingIndex", exclude=["from_response_json"]
        ),
    },
    "api/embedding_feature_api.md": {
        "embedding_feature": ["hsfs.embedding.EmbeddingFeature"],
        "embedding_feature_properties": keras_autodoc.get_properties(
            "hsfs.embedding.EmbeddingFeature"
        ),
    },
    "api/similarity_function_type_api.md": {
        "similarity_function_type": ["hsfs.embedding.SimilarityFunctionType"],
    },
    # Model registry
    "connection_api.md": {
        "connection": ["hsml.connection.Connection"],
        "connection_properties": keras_autodoc.get_properties(
            "hsml.connection.Connection", exclude=["trust_store_path"]
        ),
        "connection_methods": keras_autodoc.get_methods("hsml.connection.Connection"),
    },
    "model-registry/model_registry_api.md": {
        "mr_get": ["hsml.connection.Connection.get_model_registry"],
        "mr_modules": keras_autodoc.get_properties(
            "hsml.model_registry.ModelRegistry",
            exclude=[
                "project_id",
                "project_name",
                "model_registry_id",
                "shared_registry_project_name",
            ],
        ),
        "mr_properties": keras_autodoc.get_properties(
            "hsml.model_registry.ModelRegistry",
            exclude=[
                "python",
                "sklearn",
                "tensorflow",
                "torch",
            ],
        ),
        "mr_methods": keras_autodoc.get_methods(
            "hsml.model_registry.ModelRegistry", exclude=["from_response_json"]
        ),
    },
    "model-registry/model_api.md": {
        "ml_create_tf": ["hsml.model_registry.ModelRegistry.tensorflow.create_model"],
        "ml_create_th": ["hsml.model_registry.ModelRegistry.torch.create_model"],
        "ml_create_sl": ["hsml.model_registry.ModelRegistry.sklearn.create_model"],
        "ml_create_py": ["hsml.model_registry.ModelRegistry.python.create_model"],
        "ml_get": ["hsml.model_registry.ModelRegistry.get_model"],
        "ml_properties": keras_autodoc.get_properties("hsml.model.Model"),
        "ml_methods": keras_autodoc.get_methods(
            "hsml.model.Model",
            exclude=[
                "from_response_json",
                "json",
                "to_dict",
                "update_from_response_json",
            ],
        ),
    },
    "model-registry/model_schema.md": {},
    "model-registry/model_schema_api.md": {
        "schema": ["hsml.schema.Schema"],
        "schema_dict": ["hsml.schema.Schema.to_dict"],
        "model_schema": ["hsml.model_schema.ModelSchema"],
        "model_schema_dict": ["hsml.model_schema.ModelSchema.to_dict"],
    },
    "model-registry/links.md": {
        "links_properties": keras_autodoc.get_properties(
            "hsml.core.explicit_provenance.Links"
        ),
        "artifact_properties": keras_autodoc.get_properties(
            "hsml.core.explicit_provenance.Artifact"
        ),
    },
    # Model Serving
    "model-serving/model_serving_api.md": {
        "ms_get": ["hsml.connection.Connection.get_model_serving"],
        "ms_properties": keras_autodoc.get_properties(
            "hsml.model_serving.ModelServing"
        ),
        "ms_methods": keras_autodoc.get_methods(
            "hsml.model_serving.ModelServing", exclude=["from_response_json"]
        ),
    },
    "model-serving/deployment_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_get_deployments": [
            "hsml.model_serving.ModelServing.get_deployment",
            "hsml.model_serving.ModelServing.get_deployment_by_id",
            "hsml.model_serving.ModelServing.get_deployments",
        ],
        "ms_create_deployment": ["hsml.model_serving.ModelServing.create_deployment"],
        "m_deploy": ["hsml.model.Model.deploy"],
        "p_deploy": ["hsml.predictor.Predictor.deploy"],
        "dep_properties": keras_autodoc.get_properties("hsml.deployment.Deployment"),
        "dep_methods": keras_autodoc.get_methods(
            "hsml.deployment.Deployment", exclude=JSON_METHODS + ["from_predictor"]
        ),
    },
    "model-serving/predictor_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_create_predictor": ["hsml.model_serving.ModelServing.create_predictor"],
        "pred_properties": keras_autodoc.get_properties("hsml.predictor.Predictor"),
        "pred_methods": keras_autodoc.get_methods(
            "hsml.predictor.Predictor",
            exclude=JSON_METHODS + ["for_model"],
        ),
    },
    "model-serving/transformer_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_create_transformer": ["hsml.model_serving.ModelServing.create_transformer"],
        "trans_properties": keras_autodoc.get_properties(
            "hsml.transformer.Transformer"
        ),
        "trans_methods": keras_autodoc.get_methods(
            "hsml.transformer.Transformer", exclude=JSON_METHODS
        ),
    },
    "model-serving/inference_logger_api.md": {
        "il": ["hsml.inference_logger.InferenceLogger"],
        "il_properties": keras_autodoc.get_properties(
            "hsml.inference_logger.InferenceLogger"
        ),
        "il_methods": keras_autodoc.get_methods(
            "hsml.inference_logger.InferenceLogger", exclude=JSON_METHODS
        ),
    },
    "model-serving/inference_batcher_api.md": {
        "ib": ["hsml.inference_batcher.InferenceBatcher"],
        "ib_properties": keras_autodoc.get_properties(
            "hsml.inference_batcher.InferenceBatcher"
        ),
        "ib_methods": keras_autodoc.get_methods(
            "hsml.inference_batcher.InferenceBatcher", exclude=JSON_METHODS
        ),
    },
    "model-serving/resources_api.md": {
        "res": ["hsml.resources.Resources"],
        "res_properties": keras_autodoc.get_properties("hsml.resources.Resources"),
        "res_methods": keras_autodoc.get_methods(
            "hsml.resources.Resources", exclude=JSON_METHODS
        ),
    },
    "model-serving/predictor_state_api.md": {
        "ps_get": ["hsml.deployment.Deployment.get_state"],
        "ps_properties": keras_autodoc.get_properties(
            "hsml.predictor_state.PredictorState"
        ),
        "ps_methods": keras_autodoc.get_methods(
            "hsml.predictor_state.PredictorState", exclude=JSON_METHODS
        ),
    },
    "model-serving/predictor_state_condition_api.md": {
        "psc_get": ["hsml.predictor_state.PredictorState.condition"],
        "psc_properties": keras_autodoc.get_properties(
            "hsml.predictor_state_condition.PredictorStateCondition"
        ),
        "psc_methods": keras_autodoc.get_methods(
            "hsml.predictor_state_condition.PredictorStateCondition",
            exclude=JSON_METHODS,
        ),
    },
}

hw_dir = pathlib.Path(__file__).resolve().parents[1]
if "GITHUB_SHA" in os.environ:
    commit_sha = os.environ["GITHUB_SHA"]
    project_url = (
        f"https://github.com/logicalclocks/hopsworks-api/tree/{commit_sha}/python"
    )
else:
    branch_name = os.environ.get("GITHUB_BASE_REF", "master")
    project_url = (
        f"https://github.com/logicalclocks/hopsworks-api/blob/{branch_name}/python"
    )


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url=project_url,
        template_dir="./docs/templates",
        titles_size="###",
        extra_aliases={
            "hsfs.core.query.Query": "hsfs.Query",
            "hsfs.storage_connector.StorageConnector": "hsfs.StorageConnector",
            "hsfs.statistics_config.StatisticsConfig": "hsfs.StatisticsConfig",
            "hsfs.training_dataset_feature.TrainingDatasetFeature": "hsfs.TrainingDatasetFeature",
            "pandas.core.frame.DataFrame": "pandas.DataFrame",
        },
        max_signature_line_length=100,
    )
    shutil.copyfile(hw_dir / "CONTRIBUTING.md", dest_dir / "CONTRIBUTING.md")
    shutil.copyfile(hw_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hw_dir / "docs")
