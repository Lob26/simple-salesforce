import json
from collections.abc import (
    Callable,
    Hashable,
    Iterator,
    Mapping,
    Sequence,
)

import re
from typing import (
    Any,
    Literal,
    TypeVar,
    cast,
    overload,
)

from httpx import Client, Headers

from nsss.utils import (
    CallableSF,
    JsonType,
    Proxies,
    SFOperation,
    to_url_mount,
    LineEnding,
)
from nsss.utils.base import ColumnDelimiter
from nsss.utils.exceptions import (
    SalesforceBulkV2ExtractError,
    SalesforceBulkV2LoadError,
)

K = TypeVar("K", bound=Hashable)
V = TypeVar("V", bound=Any)

MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024
MAX_INGEST_JOB_PARALLELISM = 10  # TODO: ? Salesforce limits
DEFAULT_QUERY_PAGE_SIZE = 50000
JSON_CONTENT_TYPE = "application/json"
CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"

_delimiter_char = {
    ColumnDelimiter.BACKQUOTE: "`",
    ColumnDelimiter.CARET: "^",
    ColumnDelimiter.COMMA: ",",
    ColumnDelimiter.PIPE: "|",
    ColumnDelimiter.SEMICOLON: ";",
    ColumnDelimiter.TAB: "\t",
}
_line_ending_char = {
    "LF": "\n",
    "CRLF": "\r\n",
}


class Bulk2SFHandler:
    """Bulk 2.0 API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk2.Contacts.insert(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(
        self,
        session_id: str,
        bulk2_url: str,
        proxies: Proxies | None = None,
        session: Client | None = None,
    ):
        """Initialize the instance with the given parameters.

        Arguments:
            * session_id: The session ID for authenticating to Salesforce
            * bulk2_url: The URL to the Salesforce Bulk 2.0 API
            * proxies: The optional map of scheme to proxy server
            * session: Custom httpx.Client instance to use for requests.
                This enables the use of httpx features not otherwise exposed by the library.
        """
        self.client = session or Client(follow_redirects=True)
        self.client.headers.update(
            {
                "Content-Type": JSON_CONTENT_TYPE,
                "X-SFDC-Session": session_id,
                "X-PrettyPrint": "1",
            }
        )
        if proxies:
            _proxies = to_url_mount(proxies)
            self.client._mounts.update(  # pyright: ignore[reportPrivateUsage]
                {
                    key: _proxies[key]
                    for key in (_proxies.keys() - self.client._mounts.keys())  # pyright: ignore[reportPrivateUsage]
                }
            )

        self.client.base_url = self.client.base_url or bulk2_url

    def __getattr__(self, name: str) -> "Bulk2SFType":
        return Bulk2SFType(self.client, object_name=name)


class Bulk2SFType(CallableSF):
    """Interface to Bulk 2.0 API functions"""

    def __init__(
        self,
        client: Client,
        object_name: str,
        parse_float: Callable[[str], Any] | None = None,
        object_pairs_hook: Callable[[Sequence[tuple[K, V]]], Mapping[K, V]] = dict[K, V],
    ) -> None:  # fmt:skip
        """
        Builds the instance with the client data
        ---

        Initialize the instance with the given parameters

        Arguments:
            * client: The httpx.Client instance to use for requests
            * object_name: The name of the object to interact with
        """
        self.object_name = object_name
        self.client = client

        self.parse_float = parse_float
        self.object_pairs_hook = object_pairs_hook  # type: ignore

    @overload
    @staticmethod
    def _count_csv(
        *,
        filename: str | None = None,
        line_ending: LineEnding = "LF",
        skip_header: bool = False,
    ) -> int: ...

    @overload
    @staticmethod
    def _count_csv(
        *,
        data: str | None = None,
        line_ending: LineEnding = "LF",
        skip_header: bool = False,
    ) -> int: ...

    @staticmethod
    def _count_csv(
        *,
        filename: str | None = None,
        data: str | None = None,
        line_ending: LineEnding = "LF",
        skip_header: bool = False,
    ) -> int:
        """Count the number of records in a CSV file."""
        if filename:
            with open(filename, encoding="utf-8", mode="r") as bis:
                count = sum(1 for _ in bis)
        elif data:
            pat = _line_ending_char[line_ending]
            count = data.count(pat)
        else:
            raise ValueError("Either filename or data must be provided")

        if skip_header:
            count -= 1
        return count

    @overload
    @staticmethod
    def _split_csv(
        *,
        filename: str | None = None,
        max_records: int | None = None,
    ) -> Iterator[tuple[int, str]]: ...

    @overload
    @staticmethod
    def _split_csv(
        *,
        records: str | None = None,
        max_records: int | None = None,
    ) -> Iterator[tuple[int, str]]: ...

    @staticmethod
    def _split_csv(
        *,
        filename: str | None = None,
        records: str | None = None,
        max_records: int | None = None,
    ) -> Iterator[tuple[int, str]]:
        """Split a CSV file into chunks to avoid exceeding the Salesforce bulk 2.0 API limits"""

        max_records = cast(int, max_records or float("-inf"))
        if filename:
            import os

            with open(filename, encoding="utf-8", mode="r") as bis:
                header = bis.readline()
                lines = bis.readlines()
            _max_records = max(max_records, len(lines))
            _max_bytes = min(
                os.path.getsize(filename), MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
            )
            yield from Bulk2SFType.__yield_chunks(
                header, lines, _max_records, _max_bytes
            )
        elif records:
            import sys

            header, *lines = records.splitlines(True)
            _max_records = max(max_records, len(lines))
            _max_bytes = min(
                sys.getsizeof(records), MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
            )
            yield from Bulk2SFType.__yield_chunks(
                header, lines, _max_records, _max_bytes
            )
        else:
            raise ValueError("Either filename or data must be provided")

    @staticmethod
    def __yield_chunks(
        header: str, lines: list[str], max_records: int, max_bytes: int
    ) -> Iterator[tuple[int, str]]:
        records_size = 0
        bytes_size = 0
        buff: list[str] = []
        for line in lines:
            records_size += 1
            bytes_size += len(line.encode("utf-8"))
            if records_size > max_records or bytes_size > max_bytes:
                if buff:
                    yield records_size - 1, header + "".join(buff)
                buff = [line]
                records_size = 1
                bytes_size = len(line.encode("utf-8"))
            else:
                buff.append(line)
        if buff:
            yield records_size, header + "".join(buff)

    @staticmethod
    def _get_endpoint(job_id: str | None, is_query: bool) -> str:
        """Construct bulk 2.0 API request URL"""
        url = "query" if is_query else "ingest"
        return f"{url}/{job_id}" if job_id else url

    def create_job(
        self,
        operation: SFOperation,
        *,
        query: str | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        external_id_field: str | None = None,
    ):
        """
        Create a job

        Arguments:
            * operation: Bulk opertation to be performed by job
            * query: The SOQL query to be performed by the job
            * column_delimiter: The column delimiter used for CSV job data
            * line_ending: The line ending used for CSV job data
            * external_id_field: The external ID field used for upsert operations
        """
        payload: dict[str, Any] = {
            "operation": operation,
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
        }
        if external_id_field:
            payload["externalIdFieldName"] = external_id_field

        is_query = operation in ("query", "queryAll")
        endpoint = Bulk2SFType._get_endpoint(None, is_query)
        if is_query:
            if not query:
                raise SalesforceBulkV2ExtractError("Query is required for query jobs")
            headers = cast(
                Headers,
                {"Content-Type": JSON_CONTENT_TYPE, "Accept": CSV_CONTENT_TYPE},
            )
            payload["query"] = query
        else:
            headers = cast(
                Headers,
                {"Content-Type": CSV_CONTENT_TYPE, "Accept": JSON_CONTENT_TYPE},
            )
            payload["object"] = self.object_name
            payload["contentType"] = "CSV"

        result = self.call_salesforce(
            "POST",
            endpoint,
            headers=headers,
            json=payload,
        )
        return result.json(object_pairs_hook=self.object_pairs_hook)

    def get_job(
        self,
        job_id: str,
        is_query: bool,
        wait: bool = True,
    ):
        """Get job info"""
        endpoint = Bulk2SFType._get_endpoint(job_id, is_query)
        return self.call_salesforce("GET", endpoint).json(
            object_pairs_hook=self.object_pairs_hook
        )

    @staticmethod
    def upload_job_data(
        job_id: str,
        data: str,
    ): ...

    @staticmethod
    def close_job(
        job_id: str,
    ): ...

    @staticmethod
    def abort_job(
        job_id: str,
        wait: bool = True,
    ): ...

    @staticmethod
    def wait_for_job(
        job_id: str,
        wait: bool = True,
        seconds: int = 5,
    ): ...

    def _upload_data(
        self,
        operation: SFOperation,
        data: str | tuple[int, str],
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        external_id_field: str | None = None,
        wait: int = 5,
    ) -> dict[str, int]:
        """Upload data to Salesforce"""
        if isinstance(data, tuple):
            total, unpacked_data = data
        else:
            total = self._count_csv(
                data=data,
                line_ending=line_ending,
                skip_header=True,
            )
            unpacked_data = data

        res = self.create_job(
            operation,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
        )
        job_id = res["id"]
        try:
            if res["state"] == "Open":
                self.upload_job_data(job_id, unpacked_data)
                self.close_job(job_id)
                self.wait_for_job(job_id, False, wait)
                res = self.get_job(job_id, False)
                return {
                    "numberRecordsFailed": int(res["numberRecordsFailed"]),
                    "numberRecordsProcessed": int(res["numberRecordsProcessed"]),
                    "numberRecordsTotal": int(total),
                    "job_id": job_id,
                }
            raise SalesforceBulkV2LoadError(
                f"Failed to upload job data. Response content: {res}"
            )
        except Exception:
            res = self.get_job(job_id, False)
            if res["state"] in ("UploadComplete", "InProgress", "Open"):
                self.abort_job(job_id, False)
            raise
