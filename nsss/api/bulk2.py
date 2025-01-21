from concurrent.futures import ThreadPoolExecutor
from functools import partial
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
    AnyStr,
    Literal,
    NotRequired,
    TypeVar,
    TypedDict,
    cast,
    overload,
)

from httpx import Client, Headers
from more_itertools import chunked

from nsss.utils import (
    CallableSF,
    JsonType,
    Proxies,
    SFOperation,
    to_url_mount,
    LineEnding,
)
from nsss.utils.base import ColumnDelimiter, JobState
from nsss.utils.exceptions import (
    SalesforceBulkV2ExtractError,
    SalesforceBulkV2LoadError,
    SalesforceOperationError,
)

K = TypeVar("K", bound=Hashable)
V = TypeVar("V", bound=Any)

DEFAULT_WAIT_TIMEOUT_SECONDS = 24 * 60 * 60  # 24 hours
MAX_CHECK_INTERVAL_SECONDS = 2  # 2 seconds
JSON_CONTENT_TYPE = "application/json"
CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"

# https://developer.salesforce.com/docs/atlas.en-us.242.0
# .salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet
# /salesforce_app_limits_platform_bulkapi.htm
# https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta
# /api_asynch/datafiles_prepare_csv.htm
MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024  # 100 MiB
MAX_INGEST_JOB_PARALLELISM = 10  # TODO: ? Salesforce limits
DEFAULT_QUERY_PAGE_SIZE = 50000

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


class BulkQueryResult(TypedDict):
    locator: str
    number_of_records: int
    records: str
    file: NotRequired[str]


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

    def insert(
        self,
        *,
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        batch_size: int | None = None,
        concurrency: int = 1,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ):
        """Insert records"""
        return self._upload_file(
            "insert",
            csv_file=csv_file,
            records=self._convert_dict_to_csv(
                records,  # type: ignore
                column_delimiter=column_delimiter,
                line_ending=line_ending,
            ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            concurrency=concurrency,
            wait=wait,
        )

    create = insert

    def query(
        self,
        *,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ) -> Iterator[str]:
        """
        Bulk 2.0 query
        ---

        Args:
            * query: The SOQL query to be performed
            * max_records: The maximum number of records to retrieve per batch
        """
        res = self.create_job(
            "query",
            query=query,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
        )
        job_id = res["id"]
        self.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["records"]

    read = query

    def query_all(
        self,
        *,
        query: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ) -> Iterator[str]:
        """
        Bulk 2.0 queryAll
        ---

        Args:
            * query: The SOQL query to be performed
            * max_records: The maximum number of records to retrieve per batch
        """
        res = self.create_job(
            "queryAll",
            query=query,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
        )
        job_id = res["id"]
        self.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self.get_query_results(job_id, locator, max_records)
            locator = result["locator"]
            yield result["records"]

    queryAll = query_all
    read_all = query_all

    def update(
        self,
        *,
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ) -> list[dict[str, int]]:
        """Update records"""
        return self._upload_file(
            "update",
            csv_file=csv_file,
            records=self._convert_dict_to_csv(
                records,  # type: ignore
                column_delimiter=column_delimiter,
                line_ending=line_ending,
            ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            wait=wait,
        )

    def upsert(
        self,
        *,
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        external_id_field: str = "Id",
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ):
        """Upsert records based on a unique identifier"""
        return self._upload_file(
            "upsert",
            csv_file=csv_file,
            records=self._convert_dict_to_csv(
                records,  # type: ignore
                column_delimiter=column_delimiter,
                line_ending=line_ending,
            ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
            wait=wait,
        )

    def soft_delete(
        self,
        *,
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        external_id_field: str | None = None,
        wait: int = 5,
    ):
        """Soft delete records"""
        return self._upload_file(
            "delete",
            csv_file=csv_file,
            records=self._convert_dict_to_csv(
                records,  # type: ignore
                column_delimiter=column_delimiter,
                line_ending=line_ending,
            ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
            wait=wait,
        )

    delete = soft_delete

    def hard_delete(
        self,
        *,
        csv_file: str | None = None,
        records: list[dict[str, str]] | None = None,
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ):
        """Hard delete records"""
        return self._upload_file(
            "hardDelete",
            csv_file=csv_file,
            records=self._convert_dict_to_csv(
                records,  # type: ignore
                column_delimiter=column_delimiter,
                line_ending=line_ending,
            ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            wait=wait,
        )

    def download(
        self,
        *,
        query: str,
        path: str,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        wait: int = 5,
    ):
        """
        Bulk 2.0 query stream to file, avoiding high memory usage

        Args:
            * query: The SOQL query to be performed
            * path: The path to save the file
        """
        import os

        if not os.path.exists(path):
            raise SalesforceBulkV2LoadError(f"Path not found: {path}")

        res = self.create_job(
            "query",
            query=query,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
        )
        job_id = res["id"]
        self.wait_for_job(job_id, True, wait)

        results = []
        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""

            endpoint = f"{self._get_endpoint(job_id, True)}/results"
            params = {
                "maxRecords": max_records,
                "locator": locator,
            }

            locator = result["locator"]
            results.apend(result)

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
                header, *lines = bis.readlines()
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
    def _convert_dict_to_csv(
        data: list[dict[str, str]],
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
    ) -> str | None:
        """Convert a list of dictionaries to a CSV like object"""
        if not data:
            return None

        from csv import DictWriter
        from io import StringIO

        keys = {key for d in data for key in d.keys()}
        file = StringIO()
        writer = DictWriter(
            file,
            fieldnames=keys,
            delimiter=_delimiter_char[column_delimiter],
            lineterminator=_line_ending_char[line_ending],
        )
        writer.writeheader()
        writer.writerows(data)
        return file.getvalue()

    @staticmethod
    def _get_endpoint(job_id: str | None, is_query: bool) -> str:
        """Construct bulk 2.0 API request URL"""
        url = "query" if is_query else "ingest"
        return f"{url}/{job_id}" if job_id else url

    @staticmethod
    def _get_headers(request_ct: str | None, response_ct: str | None):
        """Utility function to replicate a common set of headers"""
        return cast(
            Headers,
            {
                "Content-Type": request_ct or JSON_CONTENT_TYPE,
                "Accept": response_ct or JSON_CONTENT_TYPE,
            },
        )

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
            headers = self._get_headers(JSON_CONTENT_TYPE, CSV_CONTENT_TYPE)
            payload["query"] = query
        else:
            headers = self._get_headers(CSV_CONTENT_TYPE, JSON_CONTENT_TYPE)
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

    def wait_for_job(
        self,
        job_id: str,
        is_query: bool,
        wait: float = 0.5,
    ) -> Literal["JobComplete"]:
        """Wait for job completion or timeout"""
        from datetime import datetime, timedelta
        from time import sleep
        from math import exp

        expiration_time = datetime.now() + timedelta(seconds=DEFAULT_WAIT_TIMEOUT_SECONDS)  # fmt:skip
        job_status: JobState = "InProgress" if is_query else "Open"
        delay_timeout = 0.0
        delay_cnt = 0
        sleep(wait)
        while datetime.now() < expiration_time:
            job_info = self.get_job(job_id, is_query)
            job_status: JobState = job_info["state"]
            if job_status == "JobComplete":
                return job_status
            elif job_status in ("Failed", "Aborted"):
                error_msg = job_info.get("errorMessage", job_info)
                raise SalesforceOperationError(
                    f"Job {job_id} failed with status {job_status}: {error_msg}"
                )

            if delay_timeout < MAX_CHECK_INTERVAL_SECONDS:
                delay_timeout = wait + exp(delay_cnt) / 1000
                delay_cnt += 1
            sleep(delay_timeout)
        raise SalesforceOperationError(
            f"Job {job_id} did not complete within the timeout period. Current status: {job_status}"
        )

    def get_query_results(
        self, job_id: str, locator: str = "", max_records: int = DEFAULT_QUERY_PAGE_SIZE
    ) -> BulkQueryResult:
        """Get results for a query job"""
        endpoint = f"{Bulk2SFType._get_endpoint(job_id, True)}/results"

        params: dict[str, str | int] = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator

        headers = Bulk2SFType._get_headers(JSON_CONTENT_TYPE, CSV_CONTENT_TYPE)
        result = self.call_salesforce(
            "GET",
            endpoint,
            params=params,
            headers=headers,
        )
        locator = result.headers.get("Sforce-Locator", "null")

        if locator == "null":
            locator = ""

        record_number = int(result.headers["Sforce-NumberOfRecords"])
        return {
            "locator": locator,
            "number_of_records": record_number,
            "records": self._filter_null_bytes(result.text),
        }

    @staticmethod
    def _filter_null_bytes(b: AnyStr) -> AnyStr:
        """
        Filter out null bytes from a byte string
        https://github.com/airbytehq/airbyte/issues/8300
        """
        if isinstance(b, str):
            return b.replace("\x00", "")
        if isinstance(b, bytes):
            return b.replace(b"\x00", b"")
        raise TypeError("Expected str or bytes")

    def _upload_file(
        self,
        operation: SFOperation,
        *,
        csv_file: str | None = None,
        records: str | None = None,
        batch_size: int | None = None,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = "LF",
        external_id_field: str | None = None,
        wait: int = 5,
        concurrency: int = 1,
    ):
        """Upload CSV file to Salesforce"""
        if csv_file and records:
            raise SalesforceBulkV2LoadError("Cannot include both file and records")
        if not records and csv_file:
            import os

            if not os.path.exists(csv_file):
                raise SalesforceBulkV2LoadError(f"File not found: {csv_file}")

        if operation in ("delete", "hardDelete"):
            assert csv_file, "File is required for delete operations"

            with open(csv_file, encoding="utf-8", mode="r") as bis:
                header = (
                    bis.readline().rstrip().split(_delimiter_char[column_delimiter])
                )
                if len(header) != 1:
                    raise SalesforceBulkV2LoadError(
                        f"InvalidBatch: The {operation!r} batch must contain only ids, {header}"
                    )

        workers = min(concurrency, MAX_INGEST_JOB_PARALLELISM)
        split_data = (
            self._split_csv(filename=csv_file, max_records=batch_size)
            if csv_file
            else self._split_csv(records=records, max_records=batch_size)
        )

        results: list[dict[str, int]] = []
        if workers == 1:
            results.extend(
                [
                    self._upload_data(
                        operation,
                        data,
                        column_delimiter,
                        line_ending,
                        external_id_field,
                        wait,
                    )
                    for data in split_data
                ]
            )
        else:
            # OOM is possible if the file is too large
            for chunks in chunked(split_data, n=workers):
                workers = min(workers, len(chunks))

                with ThreadPoolExecutor(max_workers=workers) as pool:
                    multi_thread_worker = partial(
                        self._upload_data,
                        operation,
                        column_delimiter=column_delimiter,
                        line_ending=line_ending,
                        external_id_field=external_id_field,
                        wait=wait,
                    )
                    results.extend(pool.map(multi_thread_worker, chunks))

        return results

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
