"""Feed adapter factory."""

from __future__ import annotations

from nibble.adapters.base import BaseAdapter


def get_adapter(
    adapter_name: str,
    url: str,
    agency_id: str = "",
    agency_timezone: str | None = None,
    auth_type: str = "none",
    auth_secret: str | None = None,
    passio_static_routes_file: str | None = None,
) -> BaseAdapter:
    """Return the appropriate adapter for the given adapter name.

    Args:
        adapter_name: One of ``"gtfs_rt"`` or ``"passio"``.
        url: The feed URL (GTFS-RT endpoint or Passio GO! API URL).
        agency_id: Optional agency identifier used by some JSON adapters.
        agency_timezone: IANA timezone name used to localise naive timestamps
            from adapters that report local time without a UTC offset.
        auth_type: Auth method - ``"none"``, ``"query_param"``, ``"header"``,
            or ``"path"``. Only ``"path"`` affects the URL here; the others are
            handled by the httpx client via ``build_httpx_auth``.
        auth_secret: The API key or token value used for ``"path"`` substitution.

    Returns:
        A ``BaseAdapter`` instance ready to use in the poll loop.

    Raises:
        ValueError: If ``adapter_name`` does not match a known adapter.
    """
    from nibble.auth import resolve_url

    url = resolve_url(url, auth_type, auth_secret)
    if adapter_name == "passio":
        from nibble.adapters.passio import PassioAdapter

        return PassioAdapter(url, agency_id, static_routes_file=passio_static_routes_file)
    if adapter_name == "gtfs_rt":
        from nibble.adapters.gtfs_rt import GtfsRtAdapter

        return GtfsRtAdapter(url)
    if adapter_name == "mwrta":
        from nibble.adapters.mwrta import MwrtaAdapter

        return MwrtaAdapter(url, agency_id, agency_timezone)
    if adapter_name == "trillium":
        from nibble.adapters.trillium import TrilliumAdapter

        return TrilliumAdapter(url, agency_id)
    if adapter_name == "swiv":
        from nibble.adapters.swiv import SwivAdapter

        return SwivAdapter(url, agency_id)
    if adapter_name == "routematch":
        from nibble.adapters.routematch import RouteMatchAdapter

        return RouteMatchAdapter(url, agency_id)
    if adapter_name == "vta":
        from nibble.adapters.vta import VtaAdapter

        return VtaAdapter(url, agency_id, agency_timezone)
    raise ValueError(f"Unknown adapter: {adapter_name!r}")
