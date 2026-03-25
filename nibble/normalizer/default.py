"""Pass-through normalizer for well-behaved feeds."""

from __future__ import annotations

from nibble.gtfs.static import StaticGTFS
from nibble.normalizer.base import BaseNormalizer
from nibble.protos import gtfs_realtime_pb2


class DefaultNormalizer(BaseNormalizer):
    """No-op normalizer. Returns the feed unmodified."""

    def normalize(
        self, feed: gtfs_realtime_pb2.FeedMessage, gtfs: StaticGTFS
    ) -> gtfs_realtime_pb2.FeedMessage:
        """Return the feed unchanged.

        Args:
            feed: The raw ``FeedMessage`` from the adapter.
            gtfs: Static GTFS indexes (unused).

        Returns:
            The unmodified ``feed``.
        """
        return feed
