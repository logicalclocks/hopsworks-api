#
#   Copyright 2026 Hopsworks AB
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
"""AWS Glue Data Catalog integration shared across table formats.

The Glue connector is reused by the Iceberg, Delta and Hudi engines.
For Iceberg the catalog owns the table's current-metadata pointer, so writes
and reads are mediated by the catalog.
For Delta and Hudi the on-path log/timeline stays authoritative; the catalog is
a discoverability mirror that is registered on create and synced on write, so
external engines (Athena, EMR, ...) can find the table by name.

[`GlueCatalog`][hsfs.core.glue_catalog.GlueCatalog] owns the connector- and
data-source-derived information common to all three formats plus the shared
Spark-session orchestration (credentials and catalog configuration).
The format-specific catalog mechanics (how reads, writes, merges and schema
evolution use the catalog) live in each engine.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from hsfs import storage_connector as sc
    from hsfs.feature_group import FeatureGroup


_logger = logging.getLogger(__name__)


class GlueCatalog:
    """Glue Data Catalog binding for a single feature group.

    Holds the feature group and its Glue connector and derives the catalog name,
    database, table and warehouse from them.
    Obtain one with
    [`GlueCatalog.for_feature_group`][hsfs.core.glue_catalog.GlueCatalog.for_feature_group],
    which returns `None` for feature groups that are not Glue-backed.
    """

    def __init__(self, feature_group: FeatureGroup, connector: sc.StorageConnector):
        self._feature_group = feature_group
        self._connector = connector

    @classmethod
    def for_feature_group(cls, feature_group: FeatureGroup) -> GlueCatalog | None:
        """Return a `GlueCatalog` for the feature group, or None if it is not Glue-backed.

        Parameters:
            feature_group: The feature group whose storage connector to inspect.

        Returns:
            A `GlueCatalog` bound to the feature group, or `None` when the feature group has a different connector or none.
        """
        from hsfs import storage_connector as sc

        connector = feature_group.storage_connector
        if connector is not None and connector.type == sc.StorageConnector.GLUE:
            return cls(feature_group, connector)
        return None

    @property
    def connector(self) -> sc.StorageConnector:
        """The Glue storage connector backing the feature group."""
        return self._connector

    @property
    def catalog_name(self) -> str:
        """The Spark catalog name used for the Glue Data Catalog."""
        return self._connector.DEFAULT_CATALOG_NAME

    @property
    def database_and_table(self) -> tuple[str | None, str]:
        """The Glue `(database, table)` for the feature group.

        The data source holds the actual Glue database and table, so it takes
        precedence over the connector's `database` (which is the Hopsworks
        default database, not the Glue one).
        The table falls back to `<name>_<version>` when the data source has none.
        """
        data_source = self._feature_group.data_source
        database = (
            data_source.database if data_source and data_source.database else None
        ) or self._connector.database
        table = data_source.table if data_source and data_source.table else None
        if not table:
            table = self._feature_group.get_fg_name()
        return database, table

    @property
    def identifier(self) -> str:
        """The `<database>.<table>` Glue Data Catalog identifier.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If neither the data source nor the connector provides a database.
        """
        from hopsworks_common.client.exceptions import FeatureStoreException

        database, table = self.database_and_table
        if not database:
            raise FeatureStoreException(
                "Glue feature groups require a database on the data source or connector."
            )
        return f"{database}.{table}"

    @property
    def qualified_name(self) -> str:
        """The catalog-qualified table name, `<catalog>.<database>.<table>`."""
        return f"{self.catalog_name}.{self.identifier}"

    @property
    def warehouse(self) -> str | None:
        """The S3 warehouse root for the Glue catalog.

        A Glue warehouse is the root under which tables are laid out as
        `<warehouse>/<database>.db/<table>`, so the feature group's own location
        (which already includes the `<database>.db/<table>` suffix) is trimmed
        back to that root.
        The warehouse is only used when the catalog creates a new table; for an
        existing table the catalog uses the location recorded in its metadata.
        """
        location = self._feature_group.location
        if not location:
            return None
        data_source = self._feature_group.data_source
        database = data_source.database if data_source else None
        suffix = f"/{database}.db/" if database else None
        if suffix and suffix in location:
            return location[: location.index(suffix)]
        return location

    def catalog_properties(self) -> dict[str, str]:
        """Return the Spark Iceberg catalog properties for the connector, warehouse included.

        Returns:
            The connector's Spark catalog options with the warehouse root applied.
        """
        return self._connector.catalog_options(warehouse=self.warehouse)

    def pyiceberg_catalog_properties(self) -> dict[str, str]:
        """Return the PyIceberg catalog properties for the connector, warehouse included.

        PyIceberg uses a different property schema than the Iceberg Spark
        connector, so this is not interchangeable with
        [`catalog_properties`][hsfs.core.glue_catalog.GlueCatalog.catalog_properties].

        Returns:
            The connector's PyIceberg catalog options with the warehouse root applied.
        """
        return self._connector.pyiceberg_catalog_options(warehouse=self.warehouse)

    def set_jvm_credentials(self, spark_context) -> None:
        """Push the connector's AWS credentials into the driver JVM's SDK system properties.

        The Glue metadata client authenticates through the AWS SDK default
        credentials chain rather than format-specific credential options, and
        the metadata call runs in the driver JVM, so the credentials are set as
        system properties which the default chain's
        `SystemPropertyCredentialsProvider` resolves first.

        Warning: Process-global
            JVM system properties are shared across the process, so when several
            Glue connectors are used in one session the last one set wins for the
            Glue metadata client.

        Does nothing in Spark Connect mode (no JVM bridge) or when the connector
        has no static credentials (the default chain then falls back to the
        instance/role credentials).

        Parameters:
            spark_context: The Spark context providing the JVM bridge, or `None` under Spark Connect.
        """
        if spark_context is None:
            return
        connector = self._connector
        if not (connector.access_key and connector.secret_key):
            return
        jvm = spark_context._jvm
        jvm.java.lang.System.setProperty("aws.accessKeyId", connector.access_key)
        jvm.java.lang.System.setProperty("aws.secretAccessKey", connector.secret_key)
        if connector.session_token:
            jvm.java.lang.System.setProperty(
                "aws.sessionToken", connector.session_token
            )
        if connector.region:
            jvm.java.lang.System.setProperty("aws.region", connector.region)
        _logger.debug("Set AWS JVM system properties for the Glue catalog client")

    def configure_spark_session(
        self,
        spark_session,
        spark_context,
        default_catalog_impl: str,
        catalog_properties: dict[str, str] | None = None,
    ) -> None:
        """Prepare the Spark session to use the Glue catalog.

        Sets the Glue metadata credentials on the driver JVM and configures
        `spark.sql.catalog.<name>.*` from the catalog properties.
        The catalog implementation comes from the properties' `impl` entry when
        present, otherwise from `default_catalog_impl` (Iceberg's `SparkCatalog`
        or Delta's `DeltaCatalog`), and only when the key is not already set.

        Parameters:
            spark_session: The Spark session to configure.
            spark_context: The Spark context for setting JVM credentials, or `None` under Spark Connect.
            default_catalog_impl: The catalog implementation class to use when none is configured.
            catalog_properties: The catalog properties to apply; defaults to the connector's catalog options.
        """
        self.set_jvm_credentials(spark_context)

        properties = dict(
            self.catalog_properties()
            if catalog_properties is None
            else catalog_properties
        )
        base_key = f"spark.sql.catalog.{self.catalog_name}"
        impl = properties.pop("impl", None)
        if impl is not None:
            spark_session.conf.set(base_key, impl)
        elif spark_session.conf.get(base_key, None) is None:
            spark_session.conf.set(base_key, default_catalog_impl)
        for prop, value in properties.items():
            spark_session.conf.set(f"{base_key}.{prop}", value)

    def iceberg_write_options(
        self,
        write_options: dict[str, Any],
        catalog_option: str,
        identifier_option: str,
        property_prefix: str,
    ) -> dict[str, Any]:
        """Augment Iceberg write options to commit through the Glue Data Catalog.

        Sets the catalog name, the `<database>.<table>` identifier and the
        catalog properties (each under `property_prefix`), so that a write
        registers the table in the Glue Data Catalog while the data stays on S3.

        Parameters:
            write_options: The write options to augment (copied, not mutated).
            catalog_option: The write-option key naming the catalog (Iceberg's `iceberg.catalog`).
            identifier_option: The write-option key for the table identifier.
            property_prefix: The prefix under which catalog properties are passed.

        Returns:
            A new options dict augmented with the Glue catalog configuration.
        """
        write_options = dict(write_options)
        write_options[catalog_option] = self.catalog_name
        write_options[identifier_option] = self.identifier
        for key, value in self.catalog_properties().items():
            write_options[f"{property_prefix}{key}"] = value
        _logger.debug(
            f"Routing Glue feature group {self._feature_group.name} "
            f"v{self._feature_group.version} through the Glue Data Catalog "
            f"as {write_options[identifier_option]}"
        )
        return write_options
