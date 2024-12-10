"""Core classes and exceptions for Simple-Salesforce"""

# ruff: noqa: E402
from datetime import datetime

from httpx import Headers
import requests

from nsss.utils.base import Proxies, KwargsAny

# has to be defined prior to login import
DEFAULT_API_VERSION = "59.0"
import base64
import json
from collections import OrderedDict
from collections.abc import (
    Callable,
    MutableMapping,
)
from pathlib import Path
from typing import (
    Any,
    Literal,
    Optional,
)
from urllib.parse import urljoin

from requests import Response, Session


class Salesforce:
    def __init__(self):
        self.sf_version: str
        self.domain: str
        self.session: requests.Session
        self.proxies: Proxies
        self.auth_type: str
        self.sf_instance: str | None
        self._salesforce_login_partial: Callable[[], None] | None


K = Any
V = Any


class SFType:
    """An interface to a specific type of SObject"""

    _parse_float = None
    _object_pairs_hook = OrderedDict

    def __init__(
        self,
        object_name: str,
        session_id: str,
        sf_instance: str,
        sf_version: Optional[str] = DEFAULT_API_VERSION,
        proxies: Proxies | None = None,
        session: Session | None = None,
        salesforce: Salesforce | None = None,
        parse_float: Callable[[str], Any] | None = None,
        object_pairs_hook: Callable[
            [list[tuple[K, V]]], MutableMapping[K, V]
        ] = OrderedDict,
    ):
        """Initialize the instance with the given parameters.
        Arguments:
        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * session_id -- the session ID for authenticating to Salesforce
        * sf_instance -- the domain of the instance of Salesforce to use
        * sf_version -- the version of the Salesforce API to use
        * proxies -- the optional map of scheme to proxy server
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        * parse_float -- Function to parse float values with. Is passed along to
                         https://docs.python.org/3/library/json.html#json.load
        * object_pairs_hook -- Function to parse ordered list of pairs in json.
                               To use python 'dict' change it to None or dict.
        """
        # Make this backwards compatible with any tests that
        # explicitly set the session_id and any other projects that
        # might be creating this object manually?

        if salesforce is None and session_id is None:
            raise RuntimeError(
                "The argument session_id or salesforce must be specified to "
                "instanciate SFType."
            )

        self._session_id = session_id
        self.salesforce = salesforce
        self.name = object_name
        self.session = session or Session()
        self._parse_float = parse_float
        self._object_pairs_hook = object_pairs_hook  # type: ignore[assignment]

        # don't wipe out original proxies with None
        if not session and proxies is not None:
            self.session.proxies = proxies
        self.api_usage = {}

        self.base_url = (
            f"https://{sf_instance}/services/data/v{sf_version}/sobjects"
            f"/{object_name}/"
        )

    @property
    def session_id(self) -> str:
        """Helper to return the session id"""
        if self.salesforce is not None:
            return self.salesforce.session_id
        return self._session_id

    def metadata(self, headers: Optional[Headers] = None) -> dict[str, Any]:
        """Returns the result of a GET to `.../{object_name}/` as a dict
        decoded from the JSON payload returned by Salesforce.
        Arguments:
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce("GET", self.base_url, headers=headers)
        return self.parse_result_to_json(result)

    def describe(self, headers: Optional[Headers] = None) -> dict[str, Any]:
        """Returns the result of a GET to `.../{object_name}/describe` as a
        dict decoded from the JSON payload returned by Salesforce.
        Arguments:
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="GET", url=urljoin(self.base_url, "describe"), headers=headers
        )
        return self.parse_result_to_json(result)

    def describe_layout(
        self, record_id: str, headers: Optional[Headers] = None
    ) -> dict[str, Any]:
        """Returns the layout of the object
        Returns the result of a GET to
        `.../{object_name}/describe/layouts/<recordid>` as a dict decoded from
        the JSON payload returned by Salesforce.
        Arguments:
        * record_id -- the Id of the SObject to get
        * headers -- a dict with additional request headers.
        """
        custom_url_part = f"describe/layouts/{record_id}"
        result = self._call_salesforce(
            method="GET", url=urljoin(self.base_url, custom_url_part), headers=headers
        )
        return self.parse_result_to_json(result)

    def get(
        self, record_id: str, headers: Optional[Headers] = None, **kwargs: KwargsAny
    ) -> dict[str, Any]:
        """Returns the result of a GET to `.../{object_name}/{record_id}` as a
        dict decoded from the JSON payload returned by Salesforce.
        Arguments:
        * record_id -- the Id of the SObject to get
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="GET",
            url=urljoin(self.base_url, record_id),
            headers=headers,
            **kwargs,
        )
        return self.parse_result_to_json(result)

    def get_by_custom_id(
        self,
        custom_id_field: str,
        custom_id: str,
        headers: Optional[Headers] = None,
        **kwargs: KwargsAny,
    ) -> dict[str, Any]:
        """Return an ``SFType`` by custom ID
        Returns the result of a GET to
        `.../{object_name}/{custom_id_field}/{custom_id}` as a dict decoded
        from the JSON payload returned by Salesforce.
        Arguments:
        * custom_id_field -- the API name of a custom field that was defined
                             as an External ID
        * custom_id - the External ID value of the SObject to get
        * headers -- a dict with additional request headers.
        """
        custom_url = urljoin(self.base_url, f"{custom_id_field}/{custom_id}")
        result = self._call_salesforce(
            method="GET", url=custom_url, headers=headers, **kwargs
        )
        return self.parse_result_to_json(result)

    def create(
        self, data: dict[str, Any], headers: Optional[Headers] = None
    ) -> dict[str, Any]:
        """Creates a new SObject using a POST to `.../{object_name}/`.
        Returns a dict decoded from the JSON payload returned by Salesforce.
        Arguments:
        * data -- a dict of the data to create the SObject from. It will be
                  JSON-encoded before being transmitted.
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="POST", url=self.base_url, data=json.dumps(data), headers=headers
        )
        return self.parse_result_to_json(result)

    def upsert(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ) -> int | Response:
        """Creates or updates an SObject using a PATCH to
        `.../{object_name}/{record_id}`.
        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.
        Arguments:
        * record_id -- an identifier for the SObject as described in the
                       Salesforce documentation
        * data -- a dict of the data to create or update the SObject from. It
                  will be JSON-encoded before being transmitted.
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="PATCH",
            url=urljoin(self.base_url, record_id),
            data=json.dumps(data),
            headers=headers,
        )
        return self._raw_response(result, raw_response)

    def update(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ):
        """Updates an SObject using a PATCH to
        `.../{object_name}/{record_id}`.
        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.
        Arguments:
        * record_id -- the Id of the SObject to update
        * data -- a dict of the data to update the SObject from. It will be
                  JSON-encoded before being transmitted.
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="PATCH",
            url=urljoin(self.base_url, record_id),
            data=json.dumps(data),
            headers=headers,
        )
        return self._raw_response(result, raw_response)

    def delete(
        self,
        record_id: str,
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ) -> int | Response:
        """Deletes an SObject using a DELETE to
        `.../{object_name}/{record_id}`.
        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.
        Arguments:
        * record_id -- the Id of the SObject to delete
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = self._call_salesforce(
            method="DELETE", url=urljoin(self.base_url, record_id), headers=headers
        )
        return self._raw_response(result, raw_response)

    def deleted(
        self,
        start: datetime,
        end: datetime,
        headers: Optional[Headers] = None,
    ) -> Any:
        """Gets a list of deleted records
        Use the SObject Get Deleted resource to get a list of deleted records
        for the specified object.
        .../deleted/?start=2013-05-05T00:00:00+00:00&end=2013-05-10T00:00:00
        +00:00
        * start -- start datetime object
        * end -- end datetime object
        * headers -- a dict with additional request headers.
        """
        url = urljoin(
            self.base_url,
            f"deleted/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}",
        )
        result = self._call_salesforce(method="GET", url=url, headers=headers)
        return self.parse_result_to_json(result)

    def updated(
        self,
        start: datetime,
        end: datetime,
        headers: Optional[Headers] = None,
    ) -> Any:
        """Gets a list of updated records
        Use the SObject Get Updated resource to get a list of updated
        (modified or added) records for the specified object.
         .../updated/?start=2014-03-20T00:00:00+00:00&end=2014-03-22T00:00:00
         +00:00
        * start -- start datetime object
        * end -- end datetime object
        * headers -- a dict with additional request headers.
        """
        url = urljoin(
            self.base_url,
            f"updated/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}",
        )
        result = self._call_salesforce(method="GET", url=url, headers=headers)
        return self.parse_result_to_json(result)

    def _call_salesforce(
        self,
        method: URLMethod,
        url: str,
        retries: int = 0,
        max_retries: int = 3,
        **kwargs: KwargsAny,
    ) -> Response:
        """Utility method for performing HTTP call to Salesforce."""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.session_id}",
            "X-PrettyPrint": "1",
        }
        additional_headers = kwargs.pop("headers", {})
        headers.update(additional_headers or {})
        result = self.session.request(method, url, headers=headers, **kwargs)
        # pylint: disable=W0212
        if (
            self.salesforce
            and self.salesforce._salesforce_login_partial is not None
            and result.status_code == 401
        ):
            error_details = result.json()[0]
            if error_details["errorCode"] == "INVALID_SESSION_ID":
                self.salesforce._refresh_session()
                retries += 1
                if retries > max_retries:
                    exception_handler(result, name=self.name)

                return self._call_salesforce(method, url, **kwargs)

        if result.status_code >= 300:
            exception_handler(result, self.name)

        sforce_limit_info = result.headers.get("Sforce-Limit-Info")
        if sforce_limit_info:
            self.api_usage = Salesforce.parse_api_usage(sforce_limit_info)

        return result

    @staticmethod
    def _raw_response(response: Response, body_flag: bool) -> int | Response:
        """Utility method for processing the response and returning either the
        status code or the response object.

        Returns either an `int` or a `Response` object.
        """
        return response if body_flag else response.status_code

    def parse_result_to_json(self, result: Response) -> Any:
        """Parse json from a Response object"""
        return result.json(
            object_pairs_hook=self._object_pairs_hook, parse_float=self._parse_float
        )

    def upload_base64(
        self,
        file_path: str,
        base64_field: str = "Body",
        headers: Optional[Headers] = None,
        **kwargs: KwargsAny,
    ) -> Response:
        data = {}
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        data[base64_field] = body
        result = self._call_salesforce(
            method="POST", url=self.base_url, headers=headers, json=data, **kwargs
        )

        return result

    def update_base64(
        self,
        record_id: str,
        file_path: str,
        base64_field: str = "Body",
        headers: Optional[Headers] = None,
        raw_response: bool = False,
        **kwargs: KwargsAny,
    ) -> int | Response:
        """Updated base64 image from file to Salesforce"""
        data = {}
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        data[base64_field] = body
        result = self._call_salesforce(
            method="PATCH",
            url=urljoin(self.base_url, record_id),
            json=data,
            headers=headers,
            **kwargs,
        )

        return self._raw_response(result, raw_response)

    def get_base64(
        self,
        record_id: str,
        base64_field: str = "Body",
        data: Optional[Any] = None,
        headers: Optional[Headers] = None,
        **kwargs: KwargsAny,
    ) -> bytes:
        """Returns binary stream of base64 object at specific path.

        Arguments:

        * path: The path of the request
            Example: sobjects/Attachment/ABC123/Body
                     sobjects/ContentVersion/ABC123/VersionData
        """
        result = self._call_salesforce(
            method="GET",
            url=urljoin(self.base_url, f"{record_id}/{base64_field}"),
            data=data,
            headers=headers,
            **kwargs,
        )

        return result.content
