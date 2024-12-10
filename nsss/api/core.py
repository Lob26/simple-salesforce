# pyright: reportArgumentType=false
import asyncio
import html
import json
import logging
import re
from collections.abc import Mapping
from functools import partial
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Optional,
    TypedDict,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import urlparse

from httpx import AsyncClient as Client, HTTPStatusError
from starlette.status import HTTP_401_UNAUTHORIZED

from nsss.__version__ import DEFAULT_API_VERSION
from nsss.api import Bulk2SFHandler, BulkSFHandler, CompositeSFHandler, TypeSF
from nsss.others import SalesforceLogin, SfdcMetadataApi
from nsss.utils import CallableSF, exception_handler, to_mount

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from httpx import Response

    from nsss.utils import (
        JsonType,
        KwargsAny,
        Proxies,
        URLMethod,
    )

logger = logging.getLogger(__name__)

K = TypeVar("K")
V = TypeVar("V")


class QueryResult[T: Mapping[str, Any]](TypedDict):
    totalSize: int
    done: bool
    records: list[T]
    nextRecordsUrl: NotRequired[str]


class Salesforce(CallableSF):
    """Salesforce API client.
    An instance of Salesforce is a handy way to wrap a Salesforce session
    for easy use of the Salesforce REST API."""

    # fmt:off
    @overload
    def __init__(self, *, username: str, password: str, security_token: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        Password Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * username: The Salesforce username to use for authentication
            * password: The password for the username
            * security_token: The security token for the username

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * client_id: The ID of this client
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html#json.load
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  username: str, password: str, organizationid: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        IP Filtering Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * username: The Salesforce username to use for authentication
            * password: The password for the username
            * organizationid: The organization ID for the username

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * client_id: The ID of this client
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  username: str, password: str, consumer_key: str, consumer_secret: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        OAuth 2.0 Password Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * username: The Salesforce username to use for authentication
            * password: The password for the username
            * consumer_key: The consumer key for the connected app
            * consumer_secret: The consumer secret for the connected app

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * client_id: The ID of this client
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  username: str, consumer_key: str, privatekey_file: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        OAuth 2.0 JWT-Bearer Token Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * username: The Salesforce username to use for authentication
            * consumer_key: The consumer key for the connected app
            * privatekey_file: The path to the private key file for the connected app

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * client_id: The ID of this client
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  username: str, consumer_key: str, privatekey: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        OAuth 2.0 JWT-Bearer Token Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * username: The Salesforce username to use for authentication
            * consumer_key: The consumer key for the connected app
            * privatekey: The private key for the connected app

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  consumer_key: str, consumer_secret: str, domain: str = "login",
        proxies: Optional[Proxies] = None, session: Optional[Client] = None, client_id: Optional[str] = None,
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        OAuth 2.0 Client Credentials Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * consumer_key: The consumer key for the connected app
            * consumer_secret: The consumer secret for the connected app
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  session_id: str, instance: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        Direct Session Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * session_id: The session ID to use for authentication
            * instance: The Salesforce instance to use for authentication

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    @overload
    def __init__(self, *,  session_id: str, instance_url: str,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
        version: str = DEFAULT_API_VERSION,
    ):
        """
        Direct Session Authentication
        ---
        Initialize the instance with the given parameters.

        Necessary kwargs:
            * session_id: The session ID to use for authentication
            * instance_url: The Salesforce instance URL to use for authentication

        Universal kwargs:
            * proxies: The optional map of scheme to proxy server
            * session: An existing httpx.AsyncClient instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
            * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
            * parse_float: Function to parse float values with.\
                It's passed along to https://docs.python.org/3/library/json.html
            * object_pairs_hook: Function to parse ordered list of pairs in json.
            * version: The Salesforce API version to use. Defaults to '59.0'.
        """
    # fmt:on
    def __init__(
        self, *, #NOSONAR
        username: Optional[str] = None, password: Optional[str] = None, security_token: Optional[str] = None,
        organizationid: Optional[str] = None, consumer_key: Optional[str] = None, consumer_secret: Optional[str] = None,
        privatekey_file: Optional[str] = None, privatekey: Optional[str] = None, session_id: Optional[str] = None,
        instance: Optional[str] = None, instance_url: Optional[str] = None, version: str = DEFAULT_API_VERSION,
        proxies: Optional[Proxies] = None, session: Optional[Client] = None,
        client_id: Optional[str] = None, domain: str = "login",
        parse_float: Optional[Callable[[str], Any]] = None,
        object_pairs_hook: Callable[[list[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
    ):  # fmt:skip
        """Initialize the instance with the given parameters."""

        self.sf_version = version
        self.domain = domain
        _proxies = to_mount(proxies) if proxies else None
        self.client = session or Client(
            mounts=_proxies,
            follow_redirects=True,
            base_url=f"https://{self.sf_instance}/services/",
        )
        self.proxies = _proxies

        args = dict(
            session=self.client,
            sf_version=self.sf_version,
            proxies=self.proxies,
            domain=self.domain,
        )

        extra_args = self._populate_args(
            username, password, security_token, organizationid, consumer_key,
            consumer_secret, privatekey_file, privatekey, instance_url, client_id,
            domain,
        )  # fmt:skip
        if extra_args != (None, None):
            extra_args = cast(
                tuple[
                    dict[str, str | None],
                    Literal["password", "ipfilter", "jwt-bearer", "client-credentials"],
                ],
                extra_args,
            )
            self.auth_type = extra_args[1]
            args.update(extra_args[0])
        elif session_id and (instance or instance_url):
            self.auth_type = "direct"
            # If the user provides the full url (as returned by the OAuth
            # interface for example) extract the hostname (which we rely on)
            if instance_url is not None:
                parsed_url = urlparse(instance_url)
                self.sf_instance = parsed_url.hostname
                if self.sf_instance and parsed_url.port and parsed_url.port != 443:
                    self.sf_instance += f":{parsed_url.port}"
            else:
                self.sf_instance = instance
        else:
            raise ValueError("Invalid arguments provided")

        if self.auth_type != "direct":
            self._salesforce_login_partial = partial(SalesforceLogin, **args)
        self._refresh_session()
        self._generate_headers()

    @staticmethod
    def _populate_args(
        username: Optional[str],
        password: Optional[str],
        security_token: Optional[str],
        organizationid: Optional[str],
        consumer_key: Optional[str],
        consumer_secret: Optional[str],
        privatekey_file: Optional[str],
        privatekey: Optional[str],
        instance_url: Optional[str],
        client_id: Optional[str],
        domain: str,
    ):
        return_: (
            tuple[
                dict[str, str | None],
                Literal["password", "ipfilter", "jwt-bearer", "client-credentials"],
            ]
            | tuple[None, None]
        ) = (None, None)
        if username and password and security_token:
            return_ = (
                {
                    "username": username,
                    "password": password,
                    "security_token": security_token,
                    "client_id": client_id,
                },
                "password",
            )
        elif username and password and organizationid:
            return_ = (
                {
                    "username": username,
                    "password": password,
                    "organizationId": organizationid,
                    "client_id": client_id,
                },
                "ipfilter",
            )
        elif username and password and consumer_key and consumer_secret:
            return_ = (
                {
                    "username": username,
                    "password": password,
                    "consumer_key": consumer_key,
                    "consumer_secret": consumer_secret,
                },
                "password",
            )
        elif username and consumer_key and (privatekey_file or privatekey):
            return_ = (
                {
                    "username": username,
                    "instance_url": instance_url,
                    "consumer_key": consumer_key,
                    "privatekey_file": privatekey_file,
                    "privatekey": privatekey,
                },
                "jwt-bearer",
            )
        elif consumer_key and consumer_secret and domain:
            return_ = (
                {
                    "consumer_key": consumer_key,
                    "consumer_secret": consumer_secret,
                },
                "client-credentials",
            )

        return return_

    def _generate_headers(self):
        """Utility to generate headers when refreshing the session"""
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.session_id}",
            "X-PrettyPrint": "1",
        }

    def _refresh_session(self) -> None:
        """Utility to refresh the session when expired"""
        assert (
            self._salesforce_login_partial is not None
        ), "The simple_salesforce session can not refreshed if a session id has been provided."

        self.session_id, self.sf_instance = self._salesforce_login_partial()

    @staticmethod
    def parse_api_usage(sforce_limit_info: str):
        """Parse API usage and limits out of the Sforce-Limit-Info header.

        Arguments:
            * sforce_limit_info: The value of response header 'Sforce-Limit-Info'
                Example 1: 'api-usage=18/5000'
                Example 2: 'api-usage=25/5000; per-app-api-usage=17/250(appName=sample-connected-app)'

        Example:
            >>> parse_api_usage("api-usage=18/5000")
            {'api_usage': (18, 5000), 'per_app_api_usage': None}

            >>> parse_api_usage(
            ...     "api-usage=25/5000; per-app-api-usage=17/250(appName=sample-connected-app)"
            ... )
            {'api_usage': (25, 5000), 'per_app_api_usage': (17, 250, 'sample-connected-app')}
        """
        api_usage: tuple[int, int] | None = None
        per_app_api_usage: tuple[int, int, str] | None = None

        if match := re.search(r"api-usage=(?P<used>\d+)/(?P<total>\d+)", sforce_limit_info):  # fmt:skip
            api_usage = tuple[int, int](map(int, match.group("used", "total")))

        if match := re.search(r"per-app-api-usage=(?P<used>\d+)/(?P<total>\d+)\(appName=(?P<name>.+?)\)", sforce_limit_info):  # fmt:skip
            groups = match.group("used", "used", "used")
            per_app_api_usage = (int(groups[0]), int(groups[1]), groups[2])  # fmt:skip

        return {
            "api_usage": api_usage,
            "per_app_api_usage": per_app_api_usage,
        }

    @property
    def mdapi(self) -> SfdcMetadataApi:
        """Utility to interact with metadata api functionality"""
        if not self._mdapi:
            self._mdapi = SfdcMetadataApi(
                session=self.session,
                session_id=self.session_id,
                instance=self.sf_instance,
                metadata_url=self.metadata_url,
                api_version=self.sf_version,
                headers=self.headers,
            )
        return self._mdapi

    async def _call_salesforce(
        self,
        method: URLMethod,
        endpoint: str,
        name: str,
        retries: int = 0,
        max_retries: int = 3,
        **kwargs: KwargsAny,
    ) -> Response:
        """Utility method for performing HTTP call to Salesforce."""
        try:
            response = await self.call_salesforce(
                method, endpoint, self.headers, **kwargs
            )
            if sforce_limit_info := response.headers.get("Sforce-Limit-Info"):
                self.api_usage = self.parse_api_usage(sforce_limit_info)
            return response
        except HTTPStatusError as e:
            response = e.response
            if (
                self._salesforce_login_partial
                and response.status_code == HTTP_401_UNAUTHORIZED
                and response.json().get("errorCode") == "INVALID_SESSION_ID"
            ):
                self._refresh_session()
                if retries == max_retries:
                    exception_handler(response, name)
                return await self._call_salesforce(
                    method, endpoint, name, retries=retries + 1, **kwargs
                )
            exception_handler(response, name)

    def toolingexecute(
        self,
        action: str,
        method: URLMethod = "GET",
        data: dict[str, Any] | None = None,
        **kwargs: KwargsAny,
    ) -> JsonType:
        """
        Makes an HTTP request to an TOOLING REST endpoint
        ---

        Arguments:
            * action: The action to take
            * method: The HTTP method to use (e.g., "GET", "POST")
            * data: The data to send with the request
            * kwargs: Additional arguments passed to the request
        """
        action = html.escape(action)
        json_data = json.dumps(data) if data else None
        response = asyncio.run(
            self._call_salesforce(
                endpoint=f"data/v{self.sf_version}/tooling/{action}",
                method=method,
                name="toolingexecute",
                data=json_data,
                **kwargs,
            )
        )

        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text

    def apexexecute(
        self,
        action: str,
        method: URLMethod = "GET",
        data: dict[str, Any] | None = None,
        **kwargs: KwargsAny,
    ) -> JsonType:
        """
        Makes an HTTP request to an APEX REST endpoint
        ---
        Arguments:
            * action: The action to take
            * method: The HTTP method to use (e.g., "GET", "POST")
            * data: The data to send with the request
            * kwargs: Additional arguments passed to the request
        """
        action = html.escape(action)
        json_data = json.dumps(data) if data else None
        response = asyncio.run(
            self._call_salesforce(
                endpoint=f"apexrest/{action}",
                method=method,
                name="apexexecute",
                data=json_data,
                **kwargs,
            )
        )

        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text

    def restful(
        self,
        path: str,
        method: URLMethod = "GET",
        *,
        params: dict[str, Any] | None = None,
        **kwargs: KwargsAny,
    ) -> JsonType:
        """
        Makes an HTTP request to a known REST endpoint
        ---

        Arguments:
            * path: The path to the request\
                (e.g., '/services/data/v59.0/sobjects/Account/')
            * method: The HTTP method to use (e.g., "GET", "POST")
            * params: The parameters to send with the request to the endpoint
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        response = asyncio.run(
            self._call_salesforce(
                endpoint=f"data/v{self.sf_version}/{path}",
                method=method,
                name=cast(str, kwargs.pop("name", "restful")),
                params=params,
                **kwargs,
            )
        )

        try:
            return response.json(
                object_pairs_hook=self.object_pairs_hook,
                parse_float=self._parse_float,
            )
        except json.JSONDecodeError:
            return response.text

    def oauth2(
        self,
        path: str,
        method: URLMethod = "GET",
        *,
        params: dict[str, Any] | None = None,
        **kwargs: KwargsAny,
    ):
        """
        Makes an HTTP request to a known OAuth2 endpoint
        ---

        Arguments:
            * path: The path to the request\
                (e.g., '/services/oauth2/token')
            * method: The HTTP method to use (e.g., "GET", "POST")
            * params: The parameters to send with the request to the endpoint
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        response = asyncio.run(
            self._call_salesforce(
                endpoint=f"oauth2/{path}",
                method=method,
                name=cast(str, kwargs.pop("name", "oauth2")),
                params=params,
                **kwargs,
            )
        )

        try:
            if response.headers.get("Content-Type") == "application/json":
                return response.json(
                    object_pairs_hook=self.object_pairs_hook,
                    parse_float=self._parse_float,
                )
            return response.text
        except json.JSONDecodeError:
            return response.text or response.content

    def describe(self, **kwargs: KwargsAny) -> dict[str, Any] | None:
        """
        Returns a dictionary describing all available Salesforce objects.
        ---
        Arguments:
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        return cast(
            dict[str, Any] | None,
            self.restful(
                path="sobjects",
                method="GET",
                name="describe",
                **kwargs,
            ),
        )

    def search(self, search: str) -> dict[str, Any] | None:
        """
        Returns the result of a SF search as a dictionary.
        ---
        Arguments:
            * search: The fully formatted SOSL search string\
                (e.g. `FIND {Waldo}`)
        """
        return cast(
            dict[str, Any] | None,
            self.restful(
                path="search",
                method="GET",
                params={"q": search},
                name="search",
            ),
        )

    def quick_search(self, search: str) -> dict[str, Any] | None:
        """
        Returns the result of a SF quick search as a dictionary.
        ---
        Arguments:
            * search: The non-SOSL search string.\
                (e.g. `Waldo`) This search string will be wrapped\
                    to read `FIND {Waldo}` before being sent to Salesforce
        """
        return self.search(f"FIND {{{search}}}")

    def limits(self, **kwargs: KwargsAny) -> dict[str, Any]:
        """
        Returns the current limits for the Salesforce organization.
        ---
        Arguments:
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        return cast(
            dict[str, Any],
            self.restful(
                path="limits",
                method="GET",
                name="limits",
                **kwargs,
            ),
        )

    def query(
        self, query: str, include_deleted: bool = False, **kwargs: KwargsAny
    ) -> QueryResult[dict[str, Any]]:
        """
        Returns the result of a SOQL query as a dictionary.
        ---
        Arguments:
            * query: The SOQL query to execute
            * include_deleted: Whether to include deleted records in the query
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        return cast(
            QueryResult[dict[str, Any]],
            self.restful(
                path="queryAll" if include_deleted else "query",
                method="GET",
                params={"q": query},
                name="query",
                **kwargs,
            ),
        )

    def query_more(
        self,
        next_records_identifier: str,
        identifier_is_url: bool = False,
        include_deleted: bool = False,
        **kwargs: KwargsAny,
    ) -> QueryResult[dict[str, Any]]:
        """
        Retrieves more results from a previous query that returned a `nextRecordsUrl`.
        ---
        Arguments:
            * next_records_identifier: The identifier to use to retrieve the next set of records
            * identifier_is_url: Whether the `next_records_identifier` is a URL or not
            * include_deleted: Whether to include deleted records in the query
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)

        NOTE: `nextRecordsUrl` only is returned when there are more records than the batch maximum.
        """
        if not identifier_is_url:
            next_records_identifier = next_records_identifier.lstrip(f"/services/data/v{self.sf_version}")  # fmt:skip
        endpoint = "queryAll" if include_deleted else "query"
        next_records_url = f"/{endpoint}/{next_records_identifier}"  # fmt:skip

        return cast(
            QueryResult[dict[str, Any]],
            self.restful(
                path=next_records_url,
                method="GET",
                name="query_more",
                **kwargs,
            ),
        )

    def query_all_iter(
        self, query: str, include_deleted: bool = False, **kwargs: KwargsAny
    ) -> Iterator[dict[str, Any]]:
        """
        This is a lazy alternative that returns an iterator.
        It does not construct the whole result set into one container,
        but returns objects from each page it retrieves from the API.
        ---
        Arguments:
            * query: The SOQL query to execute
            * include_deleted: Whether to include deleted records in the query
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        result = self.query(query, include_deleted, **kwargs)
        while True:
            yield from result["records"]
            # If there are more records to retrieve, get the next set
            if not result["done"] and (next_url := result.get("nextRecordsUrl")):
                result = self.query_more(
                    next_url,
                    identifier_is_url=True,
                    **kwargs,
                )

    def query_all(
        self, query: str, include_deleted: bool = False, **kwargs: KwargsAny
    ) -> QueryResult[dict[str, Any]]:
        """
        Returns the full set of results for the `query`.
        This is a convenience wrapper around `query(...)` and `query_more(...)`
        ---
        Arguments:
            * query: The SOQL query to execute
            * include_deleted: Whether to include deleted records in the query
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)

        NOTE: The `nextRecordsUrl` key is removed from the final result.
        NOTE: The `done` key is set to `True` as the full result set is returned.
        NOTE: The `totalSize` key is the total number of records returned.
        """
        all_records = list(self.query_all_iter(query, include_deleted, **kwargs))

        return {
            "done": True,
            "totalSize": len(all_records),
            "records": all_records,
        }

    def is_sandbox(self) -> Literal[True, False, None]:
        """After connection returns is the organization in a sandbox"""
        is_sandbox = None
        if self.session_id:
            is_sandbox = (
                self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
                .get("records", [{"IsSandbox": None}])[0]
                .get("IsSandbox")
            )
        return is_sandbox

    def set_password(self, user: str, password: str):
        """
        Sets the password for the given user
        See: https://www.salesforce.com/us/developer/docs/api_rest/Content/dome_sobject_user_password.htm
        """
        return self.restful(
            path=f"sobjects/User/{user}/password",
            method="POST",
            data=json.dumps(dict(NewPassword=password)),
            name="set_password",
        )

    def deploy(self, zipfile: str | IO[bytes], sandbox: bool, **kwargs: KwargsAny):
        """
        Deploy using the Metadata API
        Wrapper for `SfdcMetaDataApi.deploy(...)`
        ---
        Arguments:
            * zipfile: The zip file to deploy
            * sandbox: Whether to deploy to sandbox or not
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        async_id, state = self.mdapi.deploy(zipfile, sandbox, **kwargs)
        return {"asyncId": async_id, "state": state}

    def check_deploy_status(self, async_id: str, **kwargs: KwargsAny):
        """
        Check on the progress of a file-based deploymend via the Metadata API
        Wrapper for `SfdcMetaDataApi.check_deploy_status(...)`
        ---
        Arguments:
            * async_id: The async ID to check on
            * kwargs: Additional arguments passed to the request supported by httpx request\
                (e.g., headers, cookies, etc.)
        """
        (
            state,
            state_detail,
            deployment_detail,
            unit_test_detail,
        ) = self.mdapi.check_deploy_status(async_id, **kwargs)

        return {
            "state": state,
            "state_detail": state_detail,
            "deployment_detail": deployment_detail,
            "unit_test_detail": unit_test_detail,
        }

    @overload
    def __getattr__(self, name: Literal["bulk"]) -> BulkSFHandler:  # pyright: ignore[reportOverlappingOverload]
        """Returns an `SFBulkHandler` instance for the Salesforce Bulk API."""

    @overload
    def __getattr__(self, name: Literal["bulk2"]) -> Bulk2SFHandler:
        """Returns an `SFBulk2Handler` instance for the Salesforce Bulk API."""

    @overload
    def __getattr__(self, name: Literal["composite"]) -> CompositeSFHandler:
        """Returns an `SFCompositeHandler` instance for the Salesforce Composite API."""

    @overload
    def __getattr__(self, name: str) -> TypeSF:
        """Returns an `SFType` instance for the given Salesforce object type
        The magic part of the SalesforceAPI, this function translates
        calls such as `salesforce_api_instance.Lead.metadata()` into fully
        constituted `SFType` instances to make a nice Python API wrapper
        for the REST API.
        """

    def __getattr__(self, name: str) -> BulkSFHandler | Bulk2SFHandler | CompositeSFHandler | TypeSF:  # fmt:skip
        """
        Returns the appropriate handler for the given attribute.

        Arguments:
            * name: The name of the attribute to retrieve

        Returns:
            * Any: If the attribute starts with '__'
            * BulkSFHandler: If the attribute is 'bulk'
            * Bulk2SFHandler: If the attribute is 'bulk2'
            * CompositeSFHandler: If the attribute is 'composite'
            * TypeSF: If the attribute is a Salesforce object type
        """
        if name.startswith("__"):
            return_ = super().__getattr__(name)  # pyright: ignore[reportAttributeAccessIssue]
        elif name == "bulk":
            # Deal with bulk API functions
            return_ = BulkSFHandler()
        elif name == "bulk2":
            # Deal with bulk v2 API functions
            return_ = Bulk2SFHandler()
        elif name == "composite":
            # Deal with composite API functions
            return_ = CompositeSFHandler()
        else:
            # Deal with standard SF object API functions
            return_ = TypeSF()
        return return_
