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

PAGES = {
    "api/login.md": {
        "login": ["hopsworks.login"],
        "get_current_project": ["hopsworks.get_current_project"],
        "fs_api": ["hopsworks.project.Project.get_feature_store"],
        "mr_api": ["hopsworks.project.Project.get_model_registry"],
        "ms_api": ["hopsworks.project.Project.get_model_serving"],
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
        "job_api_handle": ["hopsworks.project.Project.get_jobs_api"],
        "job_create": ["hopsworks.core.job_api.JobsApi.create_job"],
        "job_get": ["hopsworks.core.job_api.JobsApi.get_job"],
        "job_get_all": ["hopsworks.core.job_api.JobsApi.get_jobs"],
        "job_properties": keras_autodoc.get_properties("hopsworks.job.Job"),
        "job_config": ["hopsworks.core.job_api.JobsApi.get_configuration"],
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
}

hw_dir = pathlib.Path(__file__).resolve().parents[0]
if "GITHUB_SHA" in os.environ:
    commit_sha = os.environ["GITHUB_SHA"]
    project_url = (
        f"https://github.com/logicalclocks/feature-store-api/tree/{commit_sha}/python"
    )
else:
    branch_name = os.environ.get("GITHUB_BASE_REF", "master")
    project_url = (
        f"https://github.com/logicalclocks/feature-store-api/blob/{branch_name}/python"
    )


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url=project_url,
        template_dir="./docs/templates",
        titles_size="###",
        extra_aliases={},
        max_signature_line_length=100,
    )
    shutil.copyfile(hw_dir / "CONTRIBUTING.md", dest_dir / "CONTRIBUTING.md")
    shutil.copyfile(hw_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hw_dir / "docs")
