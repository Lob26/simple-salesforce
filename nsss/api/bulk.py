import asyncio
import json
from typing import TYPE_CHECKING, Literal, TypeVar, cast, overload

from httpx import AsyncClient as Client

from nsss.utils import CallableSF, Proxies, to_url_mount

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterable,
        Callable,
        Hashable,
        Iterable,
        Mapping,
        Sequence,
    )
    from typing import Any

    from nsss.utils import JsonType, SFOperations

    K = TypeVar("K", bound=Hashable)
    V = TypeVar("V", bound=Any)
else:
    K = TypeVar("K")
    V = TypeVar("V")


class BulkSFHandler:
    """Bulk API handler for Salesforce
    Intermediate class which allows to use commands, such as `sf.bulk.Contacts.create(...)`
    This is really just a middle layer, whose sole purpose is to allow the above syntax
    """

    def __init__(
        self,
        session_id: str,
        bulk_url: str,
        proxies: Proxies | None = None,
        session: Client | None = None,
    ):
        """
        Initialize the instance with the given parameters

        Arguments:
            * session_id: The session ID for authenticating to Salesforce
            * bulk_url: The URL to the Salesforce Bulk API
            * proxies: The optional map of scheme to proxy server
            * session: Custom httpx.AsyncClient instance to use for requests.
                This enables the use of httpx features not otherwise exposed by the library.
        """
        self.client = session or Client(follow_redirects=True)
        self.client.headers.update(
            {
                "Content-Type": "application/json",
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

        self.client.base_url = self.client.base_url or bulk_url

    def __getattr__(self, name: str) -> "BulkSFType":
        return BulkSFType(self.client, object_name=name)


class BulkSFType(CallableSF):
    """Interface to Bulk/Async API functions for Salesforce"""

    def __init__(
        self,
        client: Client,
        object_name: str,
        parse_float: Callable[[str], Any] | None = None,
        object_pairs_hook: Callable[[Sequence[tuple[K, V]]], Mapping[K, V]] = dict[
            K, V
        ],
    ) -> None:
        """
        Builds the instance with the client data
        ---

        Initialize the instance with the given parameters

        Arguments:
            * client: The httpx.AsyncClient instance to use for requests
            * object_name: The name of the object to interact with
        """
        self.object_name = object_name
        self.client = client

        self.parse_float = parse_float
        self.object_pairs_hook = object_pairs_hook  # type: ignore

    @overload
    def _create_job(
        self,
        operation: Literal["upsert"],
        use_serial: bool,
        external_id_field: str,
    ) -> JsonType:
        """
        Create a new job to upsert records
        ---

        Arguments:
            * operation: Bulk operation to be performed by job (upsert)
            * use_serial: Whether to process batches in order
            * external_id_field: Unique identifier field for upsert operation (required)
        """

    @overload
    def _create_job(
        self,
        operation: Literal["delete", "hardDelete", "insert", "query", "queryAll", "update"],
        use_serial: bool,
        external_id_field: None = None,
    ) -> JsonType:  # fmt:skip
        """
        Create a new job to perform the given operation
        ---

        Arguments:
            * operation: Bulk operation to be performed by job (delete, hardDelete, insert, query, queryAll, update)
            * use_serial: Whether to process batches in order
            * external_id_field: Unique identifier field for upsert operation (None)
        """

    def _create_job(
        self,
        operation: SFOperations,
        use_serial: bool,
        external_id_field: str | None = None,
    ) -> JsonType:
        """
        Create a new job to perform the given operation
        ---

        Arguments:
            * operation: Bulk operation to be performed by job
            * use_serial: Whether to process batches in order
            * external_id_field: Unique identifier field for upsert operation
        """

        payload: dict[str, str | bool | None] = {
            "operation": operation,
            "object": self.object_name,
            "concurrencyMode": bool(use_serial),
            "contentType": "JSON",
        }

        if operation == "upsert":
            payload["externalIdFieldName"] = external_id_field

        return asyncio.run(
            self.call_salesforce(
                method="POST", endpoint="job", data=json.dumps(payload, allow_nan=False)
            )
        ).json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

    def _close_job(self, job_id: str) -> JsonType:
        """
        Close the bulk job with the given ID
        ---

        Arguments:
            * job_id: The ID of the job to close
        """

        return asyncio.run(
            self.call_salesforce(
                method="POST",
                endpoint=f"job/{job_id}",
                data=json.dumps({"state": "Closed"}),
            )
        ).json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

    def _get_job(self, job_id: str) -> JsonType:
        """
        Get the bulk job with the given ID
        ---

        Arguments:
            * job_id: The ID of the job to get
        """

        return asyncio.run(
            self.call_salesforce(
                method="GET",
                endpoint=f"job/{job_id}",
            )
        ).json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

    def _add_batch(
        self, job_id: str, data: list[Mapping[str, Any]], operation: SFOperations
    ):
        """
        Add a set of data as a batch to an existing job.
        ---

        Arguments:
            * job_id: The ID of the job to add the batch to (required)
            * data: The data to add as a batch
            * operation: The operation to perform on the batch

        NOTE: Separating this out in case of later implementations involving multiple batches
        """

        data_ = (
            json.dumps(data, allow_nan=False)
            if operation not in ("query", "queryAll")
            else data
        )

        return asyncio.run(
            self.call_salesforce(
                method="POST",
                endpoint=f"job/{job_id}/batch",
                data=data_,
            )
        ).json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

    def _get_batch(self, job_id: str, batch_id: str) -> JsonType:
        """
        Get the batch with the given ID from the job with the given ID
        ---

        Arguments:
            * job_id: The ID of the job to get the batch from
            * batch_id: The ID of the batch to get
        """

        return asyncio.run(
            self.call_salesforce(
                method="GET",
                endpoint=f"job/{job_id}/batch/{batch_id}",
            )
        ).json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

    def _get_batch_results(
        self, job_id: str, batch_id: str, operation: SFOperations
    ) -> Iterable[JsonType]:
        """
        Retrieve a set of results from a completed job
        Wrapper for the async method `__get_batch_results(...)`
        """
        return asyncio.run(self.__get_batch_results(job_id, batch_id, operation))  # type: ignore

    async def __get_batch_results(
        self, job_id: str, batch_id: str, operation: SFOperations
    ) -> AsyncIterable[JsonType]:
        """
        Retrieve a set of results from a completed job
        ---
        Arguments:
            * job_id: The ID of the job to get the results from
            * batch_id: The ID of the batch to get the results from
            * operation: The operation to perform on the batch
        """

        endpoint = f"job/{job_id}/batch/{batch_id}/result"

        response = await self.call_salesforce(
            method="GET",
            endpoint=endpoint,
        )
        result = response.json(
            object_pairs_hook=self.object_pairs_hook,
            parse_float=self.parse_float,
        )

        if operation not in ("query", "queryAll"):
            yield cast(JsonType, result)
        else:
            for batch in cast(Iterable[str], result):
                batch_query_response = await self.call_salesforce(
                    method="GET",
                    endpoint=f"{endpoint}/{batch}",
                )
                batch_query_result = batch_query_response.json(
                    object_pairs_hook=self.object_pairs_hook,
                    parse_float=self.parse_float,
                )
                yield batch_query_result
