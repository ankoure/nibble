"""Abstract base class for agency-specific feed normalizers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from nibble.gtfs.static import StaticGTFS
from nibble.protos import gtfs_realtime_pb2


class BaseNormalizer(ABC):
    """Contract for agency-specific feed normalizers.

    A normalizer receives the raw GTFS-RT ``FeedMessage`` and the loaded
    ``StaticGTFS``, may mutate the feed to fix agency quirks, and returns the
    (possibly modified) ``FeedMessage``. It runs before ``_parse_feed`` so
    downstream code never sees the raw agency data.
    """

    @abstractmethod
    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Normalize the feed (potentially in place) and return it.

        Args:
            feed: The raw ``FeedMessage`` from the adapter.
            gtfs: Static GTFS indexes, available for cross-referencing trip IDs.

        Returns:
            The (possibly mutated) ``FeedMessage`` ready for parsing.
        """
        ...
