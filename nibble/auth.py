"""Auth strategies for feed adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generator

import httpx

if TYPE_CHECKING:
    from nibble.config import Settings


class QueryParamAuth(httpx.Auth):
    """Appends a fixed query parameter to every request."""

    def __init__(self, param: str, value: str) -> None:
        self._param = param
        self._value = value

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.url = request.url.copy_add_param(self._param, self._value)
        yield request


class HeaderAuth(httpx.Auth):
    """Adds a fixed header to every request."""

    def __init__(self, header: str, value: str) -> None:
        self._header = header
        self._value = value

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers[self._header] = self._value
        yield request


class _PartialFormat(dict):
    """str.format_map helper that leaves unknown {placeholders} intact."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def resolve_url(url: str, auth_type: str, auth_secret: str | None) -> str:
    """Substitute ``{api_key}`` in *url* when ``auth_type`` is ``"path"``."""
    if auth_type != "path":
        return url
    if not auth_secret:
        raise ValueError("NIBBLE_AUTH_SECRET is required when NIBBLE_AUTH_TYPE='path'")
    return url.format_map(_PartialFormat(api_key=auth_secret))


def build_httpx_auth(config: Settings) -> httpx.Auth | None:
    """Return an httpx.Auth instance for the configured auth type, or None."""
    if config.auth_type == "none":
        return None
    if config.auth_type == "path":
        return None  # handled at URL resolution time in the adapter factory
    if not config.auth_secret:
        raise ValueError(
            f"NIBBLE_AUTH_SECRET is required when NIBBLE_AUTH_TYPE={config.auth_type!r}"
        )
    if config.auth_type == "query_param":
        return QueryParamAuth(config.auth_param_name, config.auth_secret)
    if config.auth_type == "header":
        return HeaderAuth(config.auth_header_name, config.auth_secret)
    raise ValueError(f"Unknown NIBBLE_AUTH_TYPE: {config.auth_type!r}")
