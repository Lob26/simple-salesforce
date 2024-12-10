# pyright: reportArgumentType=false
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from datetime import date as date_, datetime
from numbers import Number
from typing import Any, Literal, Optional, TypedDict
from urllib.parse import quote
from xml.etree.ElementTree import XML as fromstring

import httpx
from httpx._utils import URLPattern

from .exceptions import exception_handler


class Proxies(TypedDict, total=False):
    http: str
    https: str
    socks4: str
    socks5: str


def to_mount(proxies: Proxies) -> dict[str, httpx.AsyncHTTPTransport]:
    return {
        protocol: httpx.AsyncHTTPTransport(proxy=proxy)  # pyright: ignore[reportArgumentType]
        for protocol, proxy in proxies.items()
    }


def to_url_mount(proxies: Proxies) -> dict[URLPattern, httpx.AsyncHTTPTransport]:
    return {
        URLPattern(protocol): httpx.AsyncHTTPTransport(proxy=proxy)
        for protocol, proxy in proxies.items()
    }


type KwargsAny = date_ | str | Iterable[KwargsAny] | Number | None
type URLMethod = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
type JsonType = None | int | str | bool | Sequence[JsonType] | Mapping[str, JsonType]
type SFOperations = Literal["delete", "hardDelete", "insert", "query", "queryAll", "update", "upsert"],


class CallableSF:
    client: httpx.AsyncClient
    parse_float: Optional[Callable[[str], Any]]
    object_pairs_hook: Callable[
        [Sequence[tuple[Hashable, Any]]], Mapping[Hashable, Any]
    ] = dict[Hashable, Any]

    async def call_salesforce(
        self,
        method: URLMethod,
        endpoint: str,
        headers: httpx.Headers | None = None,
        **kwargs: KwargsAny,
    ) -> httpx.Response:
        """Performs an HTTP request to Salesforce and raises an error for non-2xx responses.

        Parameters:
            url (str): The Salesforce API endpoint to call.
            method (Literal): The HTTP method to use (e.g., "GET", "POST").
            headers (httpx.Headers): HTTP headers to include in the request.
            **kwargs (Any): Additional arguments passed to `httpx.request`.

        Returns:
            requests.Response: The response object from the HTTP request.

        Raises:
            ValueError: If an invalid HTTP method is provided.
            CustomException: For non-2xx HTTP status codes.

        Examples:
            >>> response = await call_salesforce("GET", "https://example.com")
            >>> response.status_code
            200
        """
        headers = headers or httpx.Headers()

        headers.update(self.client.headers.copy())
        headers.update(kwargs.pop("headers", dict[str, Any]()))
        headers.update(kwargs.pop("additional_headers", dict[str, Any]()))

        response = await self.client.request(
            method, endpoint, headers=headers, **kwargs
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            exception_handler(response)

        return response


def fetch_unique_xml_element_value(
    xml_string: str | bytes, element_name: str
) -> Optional[str]:
    """
    Extracts the text content of a specified XML element from an XML string.

    Parameters:
        xml_string (str | bytes): The XML content as a string or bytes.
        element_name (str): The name of the element whose value is to be extracted.

    Returns:
        Optional[str]: The text value of the first matching element, or None if no match is found.

    Raises:
        ValueError: If the XML string cannot be parsed.

    Examples:
        >>> fetch_unique_xml_element_value(
        ...     '<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', "foo"
        ... )
        'bar'

        >>> fetch_unique_xml_element_value(
        ...     '<?xml version="1.0" encoding="UTF-8"?><root><item>123</item></root>',
        ...     "item",
        ... )
        '123'

        >>> fetch_unique_xml_element_value("<foo><bar>baz</bar></foo>", "nonexistent")
        None
    """
    try:
        # Parse the XML safely using ElementTree
        root = fromstring(xml_string)
    except Exception as e:
        raise ValueError(f"Failed to parse XML: {e}")

    # Find the first matching element
    element = root.find(element_name)
    return element.text if element is not None else None


def date_to_iso8601(date: date_ | datetime) -> str:
    """Converts a date or datetime object to an ISO8601-compliant string, optionally encoding it for use in URLs."""
    # Convert the date to an ISO8601 string
    iso_string = date.isoformat()
    # URL-encode the ISO8601 string
    return quote(iso_string, safe="")


def list_from_generator[T](generator_function: Iterable[Iterable[T]]) -> list[T]:
    """Flattens a nested iterable into a single list.

    Parameters:
        generator_function (Iterable[Iterable[T]]): A generator or iterable of iterables.

    Returns:
        list[T]: A flattened list containing all items from the nested iterables.

    Examples:
        >>> gen = ([1, 2], [3, 4], [5])
        >>> list_from_generator(gen)
        [1, 2, 3, 4, 5]
    """
    return [item for sublist in generator_function for item in sublist]
