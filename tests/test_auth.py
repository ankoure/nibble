"""Tests for nibble.auth."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from nibble.auth import (
    HeaderAuth,
    QueryParamAuth,
    _PartialFormat,
    build_httpx_auth,
    resolve_url,
)


# ---------------------------------------------------------------------------
# resolve_url
# ---------------------------------------------------------------------------


def test_resolve_url_passthrough_for_none_auth() -> None:
    url = "https://example.com/feed"
    assert resolve_url(url, "none", None) == url


def test_resolve_url_passthrough_for_query_param() -> None:
    url = "https://example.com/feed"
    assert resolve_url(url, "query_param", "secret") == url


def test_resolve_url_passthrough_for_header() -> None:
    url = "https://example.com/feed"
    assert resolve_url(url, "header", "secret") == url


def test_resolve_url_substitutes_api_key() -> None:
    url = "https://example.com/ws/V1/{api_key}/vehicles"
    assert resolve_url(url, "path", "mykey") == "https://example.com/ws/V1/mykey/vehicles"


def test_resolve_url_path_raises_without_secret() -> None:
    with pytest.raises(ValueError, match="NIBBLE_AUTH_SECRET"):
        resolve_url("https://example.com/{api_key}/feed", "path", None)


def test_resolve_url_leaves_other_placeholders_intact() -> None:
    url = "https://example.com/{api_key}/feed?other={other}"
    assert resolve_url(url, "path", "mykey") == "https://example.com/mykey/feed?other={other}"


# ---------------------------------------------------------------------------
# _PartialFormat
# ---------------------------------------------------------------------------


def test_partial_format_substitutes_known_keys() -> None:
    assert "hello {name}".format_map(_PartialFormat(name="world")) == "hello world"


def test_partial_format_preserves_unknown_keys() -> None:
    result = "hello {name} {other}".format_map(_PartialFormat(name="world"))
    assert result == "hello world {other}"


# ---------------------------------------------------------------------------
# QueryParamAuth
# ---------------------------------------------------------------------------


@respx.mock
async def test_query_param_auth_appends_param() -> None:
    respx.get(url__startswith="https://example.com/feed").mock(return_value=httpx.Response(200))
    async with httpx.AsyncClient(auth=QueryParamAuth("apikey", "secret123")) as client:
        await client.get("https://example.com/feed")
    assert "apikey=secret123" in str(respx.calls.last.request.url)


@respx.mock
async def test_query_param_auth_preserves_existing_params() -> None:
    respx.get(url__startswith="https://example.com/feed").mock(return_value=httpx.Response(200))
    async with httpx.AsyncClient(auth=QueryParamAuth("apikey", "secret123")) as client:
        await client.get("https://example.com/feed", params={"format": "json"})
    url_str = str(respx.calls.last.request.url)
    assert "apikey=secret123" in url_str
    assert "format=json" in url_str


# ---------------------------------------------------------------------------
# HeaderAuth
# ---------------------------------------------------------------------------


@respx.mock
async def test_header_auth_adds_header() -> None:
    respx.get("https://example.com/feed").mock(return_value=httpx.Response(200))
    async with httpx.AsyncClient(auth=HeaderAuth("X-API-Key", "secret123")) as client:
        await client.get("https://example.com/feed")
    assert respx.calls.last.request.headers["x-api-key"] == "secret123"


@respx.mock
async def test_header_auth_preserves_existing_headers() -> None:
    respx.get("https://example.com/feed").mock(return_value=httpx.Response(200))
    async with httpx.AsyncClient(auth=HeaderAuth("X-API-Key", "secret123")) as client:
        await client.get("https://example.com/feed", headers={"Accept": "application/json"})
    headers = respx.calls.last.request.headers
    assert headers["x-api-key"] == "secret123"
    assert headers["accept"] == "application/json"


# ---------------------------------------------------------------------------
# build_httpx_auth
# ---------------------------------------------------------------------------


def _cfg(**kwargs: Any) -> Any:
    defaults: dict[str, Any] = {
        "auth_type": "none",
        "auth_secret": None,
        "auth_param_name": "api_key",
        "auth_header_name": "X-API-Key",
    }
    return SimpleNamespace(**{**defaults, **kwargs})


def test_build_httpx_auth_none() -> None:
    assert build_httpx_auth(_cfg()) is None


def test_build_httpx_auth_path_returns_none() -> None:
    # path auth is handled at URL resolution time, not at request time
    assert build_httpx_auth(_cfg(auth_type="path", auth_secret="key")) is None


def test_build_httpx_auth_query_param() -> None:
    auth = build_httpx_auth(
        _cfg(auth_type="query_param", auth_secret="s", auth_param_name="apikey")
    )
    assert isinstance(auth, QueryParamAuth)
    assert auth._param == "apikey"
    assert auth._value == "s"


def test_build_httpx_auth_header() -> None:
    auth = build_httpx_auth(
        _cfg(auth_type="header", auth_secret="tok", auth_header_name="Authorization")
    )
    assert isinstance(auth, HeaderAuth)
    assert auth._header == "Authorization"
    assert auth._value == "tok"


def test_build_httpx_auth_raises_without_secret_for_query_param() -> None:
    with pytest.raises(ValueError, match="NIBBLE_AUTH_SECRET"):
        build_httpx_auth(_cfg(auth_type="query_param", auth_secret=None))


def test_build_httpx_auth_raises_without_secret_for_header() -> None:
    with pytest.raises(ValueError, match="NIBBLE_AUTH_SECRET"):
        build_httpx_auth(_cfg(auth_type="header", auth_secret=None))


def test_build_httpx_auth_raises_for_unknown_type() -> None:
    with pytest.raises(ValueError, match="Unknown"):
        build_httpx_auth(_cfg(auth_type="oauth2", auth_secret="x"))
