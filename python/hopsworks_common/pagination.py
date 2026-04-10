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

from __future__ import annotations

from typing import TYPE_CHECKING

from hopsworks_apigen import public


if TYPE_CHECKING:
    from collections.abc import Callable


@public("hopsworks_common.pagination.Page")
class Page:
    """A single page of results from a paginated API response.

    Contains a slice of the total result set along with the metadata
    needed to determine whether more pages exist.
    """

    def __init__(self, items: list, total: int, offset: int, count: int):
        self._items = items
        self._total = total
        self._offset = offset
        self._count = count

    @public
    @property
    def items(self) -> list:
        """Items contained in this page."""
        return self._items

    @public
    @property
    def total(self) -> int:
        """Total number of items across all pages."""
        return self._total

    @public
    @property
    def offset(self) -> int:
        """Zero-based index of the first item on this page within the full result set."""
        return self._offset

    @public
    @property
    def count(self) -> int:
        """Number of items on this page."""
        return self._count

    @public
    @property
    def has_next(self) -> bool:
        """Whether there are more pages after this one."""
        return self._offset + self._count < self._total

    def json(self) -> dict:
        """Serialize page metadata to a plain dictionary.

        Returns:
            Dictionary with offset, count, and total fields.
        """
        return {
            "offset": self._offset,
            "count": self._count,
            "total": self._total,
        }

    def __repr__(self) -> str:
        return f"Page(offset={self._offset}, count={self._count}, total={self._total})"


@public("hopsworks_common.pagination.Paginator")
class Paginator:
    """Iterates lazily over all pages returned by a paginated fetch function.

    Calls `fetch_fn(offset, limit)` repeatedly until all items have been
    retrieved, yielding one `Page` per call.
    """

    DEFAULT_PAGE_SIZE = 25

    def __init__(
        self,
        fetch_fn: Callable[[int, int], Page],
        page_size: int = DEFAULT_PAGE_SIZE,
    ):
        if page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        self._fetch_fn = fetch_fn
        self._page_size = page_size

    @public
    @property
    def page_size(self) -> int:
        """Number of items requested per page."""
        return self._page_size

    def __iter__(self):
        """Yield each `Page` from first to last."""
        offset = 0
        while True:
            page = self._fetch_fn(offset, self._page_size)
            yield page
            if not page.has_next:
                break
            offset += page.count

    @public
    def collect(self) -> list:
        """Fetch all pages and return a flat list of every item.

        Returns:
            All items from every page concatenated into a single list.
        """
        return [item for page in self for item in page.items]
