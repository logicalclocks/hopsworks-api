#
#   Copyright 2025 Hopsworks AB
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

import pytest
from hopsworks_common.pagination import Page, Paginator


class TestPage:
    def test_properties(self):
        page = Page(items=["a", "b"], total=5, offset=0, count=2)

        assert page.items == ["a", "b"]
        assert page.total == 5
        assert page.offset == 0
        assert page.count == 2

    def test_has_next_true(self):
        page = Page(items=["a", "b"], total=5, offset=0, count=2)

        assert page.has_next is True

    def test_has_next_false_when_last_page(self):
        page = Page(items=["e"], total=5, offset=4, count=1)

        assert page.has_next is False

    def test_has_next_false_when_single_page(self):
        page = Page(items=["a", "b", "c"], total=3, offset=0, count=3)

        assert page.has_next is False

    def test_json(self):
        page = Page(items=["x"], total=10, offset=5, count=1)

        result = page.json()

        assert result == {"offset": 5, "count": 1, "total": 10}

    def test_repr(self):
        page = Page(items=[], total=0, offset=0, count=0)

        assert repr(page) == "Page(offset=0, count=0, total=0)"


class TestPaginator:
    def _make_fetch(self, all_items, page_size):
        """Build a fetch function that slices `all_items`."""

        def fetch(offset, limit):
            slice_ = all_items[offset : offset + limit]
            return Page(
                items=slice_,
                total=len(all_items),
                offset=offset,
                count=len(slice_),
            )

        return fetch

    def test_collect_single_page(self):
        fetch = self._make_fetch(["a", "b", "c"], page_size=10)
        paginator = Paginator(fetch, page_size=10)

        result = paginator.collect()

        assert result == ["a", "b", "c"]

    def test_collect_multiple_pages(self):
        items = list(range(7))
        fetch = self._make_fetch(items, page_size=3)
        paginator = Paginator(fetch, page_size=3)

        result = paginator.collect()

        assert result == items

    def test_collect_empty(self):
        fetch = self._make_fetch([], page_size=10)
        paginator = Paginator(fetch, page_size=10)

        result = paginator.collect()

        assert result == []

    def test_iter_yields_pages(self):
        fetch = self._make_fetch(list(range(5)), page_size=2)
        paginator = Paginator(fetch, page_size=2)

        pages = list(paginator)

        assert len(pages) == 3
        assert pages[0].offset == 0
        assert pages[1].offset == 2
        assert pages[2].offset == 4

    def test_default_page_size(self):
        fetch = self._make_fetch([], page_size=25)
        paginator = Paginator(fetch)

        assert paginator.page_size == 25

    def test_custom_page_size(self):
        fetch = self._make_fetch([], page_size=5)
        paginator = Paginator(fetch, page_size=5)

        assert paginator.page_size == 5

    def test_invalid_page_size_raises(self):
        fetch = self._make_fetch([], page_size=1)

        with pytest.raises(ValueError, match="page_size must be a positive integer"):
            Paginator(fetch, page_size=0)

    def test_negative_page_size_raises(self):
        fetch = self._make_fetch([], page_size=1)

        with pytest.raises(ValueError, match="page_size must be a positive integer"):
            Paginator(fetch, page_size=-5)
