#
#   Copyright 2024 Hopsworks AB
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

import unittest
from unittest import mock

from hsml.core import explicit_provenance


class TestExplicitProvenance(unittest.TestCase):
    def test_one_accessible_parent(self):
        artifact = {"id": 1}
        links = explicit_provenance.Links(accessible=[artifact])
        parent = explicit_provenance.Links.get_one_accessible_parent(links)
        self.assertEqual(artifact["id"], parent["id"])

    def test_one_accessible_parent_none(self):
        links = explicit_provenance.Links()
        with mock.patch.object(explicit_provenance._logger, "info") as mock_logger:
            parent = explicit_provenance.Links.get_one_accessible_parent(links)
            mock_logger.assert_called_once_with("There is no parent information")
            self.assertIsNone(parent)

    def test_one_accessible_parent_inaccessible(self):
        artifact = {"id": 1}
        links = explicit_provenance.Links(inaccessible=[artifact])
        with mock.patch.object(explicit_provenance._logger, "info") as mock_logger:
            parent = explicit_provenance.Links.get_one_accessible_parent(links)
            mock_logger.assert_called_once_with(
                "The parent is deleted or inaccessible. For more details get the full provenance from `_provenance` method"
            )
            self.assertIsNone(parent)

    def test_one_accessible_parent_deleted(self):
        artifact = {"id": 1}
        links = explicit_provenance.Links(deleted=[artifact])
        with mock.patch.object(explicit_provenance._logger, "info") as mock_logger:
            parent = explicit_provenance.Links.get_one_accessible_parent(links)
            mock_logger.assert_called_once_with(
                "The parent is deleted or inaccessible. For more details get the full provenance from `_provenance` method"
            )
            self.assertIsNone(parent)

    def test_one_accessible_parent_too_many(self):
        artifact1 = {"id": 1}
        artifact2 = {"id": 2}
        links = explicit_provenance.Links(accessible=[artifact1, artifact2])
        with self.assertRaises(Exception) as context:
            explicit_provenance.Links.get_one_accessible_parent(links)
            self.assertTrue(
                "Backend inconsistency - provenance returned more than one parent"
                in context.exception
            )

    def test_one_accessible_parent_should_not_be_artifact(self):
        artifact = explicit_provenance.Artifact(
            1, "test", 1, None, explicit_provenance.Artifact.MetaType.NOT_SUPPORTED
        )
        links = explicit_provenance.Links(accessible=[artifact])
        with self.assertRaises(Exception) as context:
            explicit_provenance.Links.get_one_accessible_parent(links)
            self.assertTrue(
                "The returned object is not a valid object. For more details get the full provenance from `_provenance` method"
                in context.exception
            )
