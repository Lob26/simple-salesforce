import asyncio
from base64 import b64decode, b64encode
from collections.abc import Mapping
from pathlib import Path
from typing import IO, Any, Optional, cast
from xml.etree.ElementTree import XML as fromstring, Element, tostring

import httpx
from zeep import Client, Settings
from zeep.proxy import ServiceProxy
from zeep.xsd import AnySimpleType, ComplexType, CompoundValue

from nsss.others.messages import (
    CHECK_DEPLOY_STATUS_MSG,
    CHECK_RETRIEVE_STATUS_MSG,
    DEPLOY_MSG,
    RETRIEVE_MSG,
)
from nsss.utils import KwargsAny
from nsss.utils.base import CallableSF

TEXTXML = "text/xml"
MTSTATUS = "mt:status"
MTFILENAME = "mt:fileName"
MTPROBLEM = "mt:problem"


class MetadataType:
    def __init__(
        self,
        name: str,
        service: ServiceProxy,
        zeep_type: ComplexType | AnySimpleType,
        session_header: CompoundValue,
    ):
        self._name = name
        self._service = service
        self._zeep_type = zeep_type
        self._session_header = session_header

    @staticmethod
    def _handle_api_response(response: list[Any]) -> None:
        errors = [
            f"\n{result.fullName}: ({error.statusCode}, {error.message}), "
            for result in response
            if not result.success
            for error in result.errors
        ]
        assert not errors, "".join(errors)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._zeep_type(*args, **kwargs)

    def create(self, metadata: list[Any]) -> None:
        response = self._service.createMetadata(
            metadata, _soapheaders=[self._session_header]
        )
        self._handle_api_response(response)

    def read(self, full_names: list[str]) -> list[Any] | Any:
        response = self._service.readMetadata(
            self._name, full_names, _soapheaders=[self._session_header]
        )
        return response[0] if len(response) == 1 else response

    def update(self, metadata: list[Any]) -> None:
        response = self._service.updateMetadata(
            metadata, _soapheaders=[self._session_header]
        )
        self._handle_api_response(response)

    def upsert(self, metadata: list[Any]) -> None:
        response = self._service.upsertMetadata(
            metadata, _soapheaders=[self._session_header]
        )
        self._handle_api_response(response)

    def delete(self, full_names: list[dict[str, Any]]) -> None:
        response = self._service.deleteMetadata(
            self._name, full_names, _soapheaders=[self._session_header]
        )
        self._handle_api_response(response)

    def rename(self, old_full_name: str, new_full_name: str) -> None:
        result = self._service.renameMetadata(
            self._name,
            old_full_name,
            new_full_name,
            _soapheaders=[self._session_header],
        )
        self._handle_api_response([result])

    def describe(self) -> Any:
        return self._service.describeValueType(
            f"{{http://soap.sforce.com/2006/04/metadata}}{self._name}",
            _soapheaders=[self._session_header],
        )


class SfdcMetadataApi(CallableSF):
    _METADATA_API_BASE_URI = "/services/Soap/m/{version}"
    _XML_NAMESPACES = {
        "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
        "mt": "http://soap.sforce.com/2006/04/metadata",
    }

    def __init__(
        self,
        session: httpx.AsyncClient,
        session_id: str,
        instance: str,
        metadata_url: str,
        headers: httpx.Headers,
        api_version: str,
    ):
        self.session = session
        self.session.headers.update(headers)
        self.session.base_url = metadata_url
        self._session_id = session_id
        self._instance = instance
        self.metadata_url = metadata_url
        self.headers = headers
        self._api_version = api_version
        self._deploy_zip = None
        wsdl_path = Path(__file__).parent / "metadata.wsdl"
        self._client = Client(
            wsdl_path.absolute().as_uri(),
            settings=Settings(strict=False, xsd_ignore_sequence_order=True),  # pyright: ignore[reportCallIssue]
        )
        self._service = self._client.create_service(
            "{http://soap.sforce.com/2006/04/metadata}MetadataBinding",
            self.metadata_url,
        )
        self._session_header = self._client.get_element("ns0:SessionHeader")(
            sessionId=self._session_id
        )

    def __getattr__(self, item: str) -> MetadataType:
        return MetadataType(
            item,
            self._service,
            self._client.get_type(f"ns0:{item}"),  # type: ignore
            self._session_header,
        )

    def describe_metadata(self) -> Any:
        return self._service.describeMetadata(
            self._api_version, _soapheaders=[self._session_header]
        )

    def list_metadata(self, queries: list[Any]) -> list[Any]:
        return self._service.listMetadata(
            queries, self._api_version, _soapheaders=[self._session_header]
        )

    def deploy(
        self, zipfile: str | IO[bytes], sandbox: bool, **kwargs: Any
    ) -> tuple[str | None, str | None]:
        attributes = {
            "client": kwargs.get("client", "simple_salesforce_metahelper"),
            "checkOnly": kwargs.get("checkOnly", False),
            "sessionId": self._session_id,
            "ZipFile": self._read_deploy_zip(zipfile),
            "testLevel": kwargs.get("testLevel"),
            "tests": kwargs.get("tests"),
            "ignoreWarnings": kwargs.get("ignoreWarnings", False),
            "allowMissingFiles": kwargs.get("allowMissingFiles", False),
            "autoUpdatePackage": kwargs.get("autoUpdatePackage", False),
            "performRetrieve": kwargs.get("performRetrieve", False),
            "purgeOnDelete": kwargs.get("purgeOnDelete", False),
            "rollbackOnError": kwargs.get("rollbackOnError", False),
            "singlePackage": True,
        }

        if not sandbox:
            attributes["allowMissingFiles"] = False
            attributes["rollbackOnError"] = True

        if attributes["testLevel"]:
            attributes["testLevel"] = f"<met:testLevel>{attributes['testLevel']}</met:testLevel>"  # fmt:skip
        if (
            attributes["tests"]
            and str(attributes["testLevel"]).lower() == "runspecifiedtests"
        ):
            attributes["tests"] = "".join(
                f"<met:runTests>{test}</met:runTests>\n"
                for test in attributes["tests"]
            )  # fmt:skip

        request = DEPLOY_MSG.format(**attributes)
        headers = {"Content-Type": TEXTXML, "SOAPAction": "deploy"}
        result = asyncio.run(
            self.call_salesforce(
                method="POST",
                endpoint="deployRequest",
                headers=self.headers,
                additional_headers=headers,
                data=request,
            )
        )

        async_process_id = fromstring(result.text).findtext(
            path="soapenv:Body/mt:deployResponse/mt:result/mt:id",
            namespaces=self._XML_NAMESPACES,
            default=None,
        )
        state = fromstring(result.text).findtext(
            path="soapenv:Body/mt:deployResponse/mt:result/mt:state",
            namespaces=self._XML_NAMESPACES,
            default=None,
        )

        return async_process_id, state

    @staticmethod
    def _read_deploy_zip(zipfile: str | IO[bytes]) -> str:
        if hasattr(zipfile, "read") and hasattr(zipfile, "seek"):
            zipfile = cast(IO[bytes], zipfile)
            zipfile.seek(0)
            raw = zipfile.read()
        else:
            zipfile = cast(str, zipfile)
            raw = Path(zipfile).read_bytes()
        return b64encode(raw).decode()

    def _retrieve_deploy_result(self, async_process_id: str, **kwargs: Any) -> Element:
        attributes = {
            "client": kwargs.get("client", "simple_salesforce_metahelper"),
            "sessionId": self._session_id,
            "asyncProcessId": async_process_id,
            "includeDetails": "true",
        }
        request = CHECK_DEPLOY_STATUS_MSG.format(**attributes)
        headers = {"Content-type": TEXTXML, "SOAPAction": "checkDeployStatus"}

        res = asyncio.run(
            self.call_salesforce(
                endpoint=f"deployRequest/{async_process_id}",
                method="POST",
                headers=self.headers,
                additional_headers=headers,
                data=request,
            )
        )
        result = fromstring(res.text).find(
            "soapenv:Body/mt:checkDeployStatusResponse/mt:result", self._XML_NAMESPACES
        )
        assert result is not None, f"Result node could not be found: {res.text}"

        return result

    @staticmethod
    def get_component_error_count(value: str) -> int:
        try:
            return int(value)
        except ValueError:
            return 0

    def check_deploy_status(
        self, async_process_id: str, **kwargs: Any
    ) -> tuple[
        Optional[str],
        Optional[str],
        Optional[Mapping[str, Any]],
        Optional[Mapping[str, Any]],
    ]:
        result = self._retrieve_deploy_result(async_process_id, **kwargs)

        state = result.findtext(MTSTATUS, None, self._XML_NAMESPACES)
        state_detail = result.findtext("mt:stateDetail", None, self._XML_NAMESPACES)

        deployment_errors = [
            {
                "type": failure.findtext(
                    "mt:componentType", None, self._XML_NAMESPACES
                ),
                "file": failure.findtext(MTFILENAME, None, self._XML_NAMESPACES),
                "status": failure.findtext(
                    "mt:problemType", None, self._XML_NAMESPACES
                ),
                "message": failure.findtext(MTPROBLEM, None, self._XML_NAMESPACES),
            }
            for failure in result.findall(
                "mt:details/mt:componentFailures", self._XML_NAMESPACES
            )
        ]

        unit_test_errors = [
            {
                "class": failure.findtext("mt:name", None, self._XML_NAMESPACES),
                "method": failure.findtext("mt:methodName", None, self._XML_NAMESPACES),
                "message": failure.findtext("mt:message", None, self._XML_NAMESPACES),
                "stack_trace": failure.findtext(
                    "mt:stackTrace", None, self._XML_NAMESPACES
                ),
            }
            for failure in result.findall(
                "mt:details/mt:runTestResult/mt:failures", self._XML_NAMESPACES
            )
        ]

        deployment_detail = {
            "total_count": result.findtext(
                "mt:numberComponentsTotal", None, self._XML_NAMESPACES
            ),
            "failed_count": result.findtext(
                "mt:numberComponentErrors", None, self._XML_NAMESPACES
            ),
            "deployed_count": result.findtext(
                "mt:numberComponentsDeployed", None, self._XML_NAMESPACES
            ),
            "errors": deployment_errors,
        }
        unit_test_detail = {
            "total_count": result.findtext(
                "mt:numberTestsTotal", None, self._XML_NAMESPACES
            ),
            "failed_count": result.findtext(
                "mt:numberTestErrors", None, self._XML_NAMESPACES
            ),
            "completed_count": result.findtext(
                "mt:numberTestsCompleted", None, self._XML_NAMESPACES
            ),
            "errors": unit_test_errors,
        }

        return state, state_detail, deployment_detail, unit_test_detail

    def download_unit_test_logs(self, async_process_id: str) -> None:
        result = self._retrieve_deploy_result(async_process_id)
        print("response:", tostring(result, encoding="us-ascii", method="xml"))

    def retrieve(
        self, async_process_id: str, **kwargs: Any
    ) -> tuple[Optional[str], Optional[str]]:
        client = kwargs.get("client", "simple_salesforce_metahelper")
        single_package = kwargs.get("single_package", True)

        assert isinstance(single_package, bool), "single_package must be bool"

        unpackaged = "".join(
            f"<types>{''.join(f'<members>{member}</members>' for member in members)}<name>{metadata_type}</name></types>"
            for metadata_type, members in kwargs.get("unpackaged", {}).items()
        )

        attributes = {
            "client": client,
            "sessionId": self._session_id,
            "apiVersion": self._api_version,
            "singlePackage": single_package,
            "unpackaged": unpackaged,
        }
        request = RETRIEVE_MSG.format(**attributes)
        headers = {"Content-type": TEXTXML, "SOAPAction": "retrieve"}

        res = asyncio.run(
            self.call_salesforce(
                endpoint=f"deployRequest/{async_process_id}",
                method="POST",
                headers=self.headers,
                additional_headers=headers,
                data=request,
            )
        )

        async_process_id_ = fromstring(res.text).findtext(
            "soapenv:Body/mt:retrieveResponse/mt:result/mt:id",
            None,
            self._XML_NAMESPACES,
        )
        state = fromstring(res.text).findtext(
            "soapenv:Body/mt:retrieveResponse/mt:result/mt:state",
            None,
            self._XML_NAMESPACES,
        )

        return async_process_id_, state

    def retrieve_retrieve_result(
        self, async_process_id: str, include_zip: str, **kwargs: Any
    ) -> Element:
        attributes = {
            "client": kwargs.get("client", "simple_salesforce_metahelper"),
            "sessionId": self._session_id,
            "asyncProcessId": async_process_id,
            "includeZip": include_zip,
        }
        request = CHECK_RETRIEVE_STATUS_MSG.format(**attributes)
        headers = {"Content-type": TEXTXML, "SOAPAction": "checkRetrieveStatus"}
        res = asyncio.run(
            self.call_salesforce(
                endpoint=f"deployRequest/{async_process_id}",
                method="POST",
                headers=self.headers,
                additional_headers=headers,
                data=request,
            )
        )

        result = fromstring(res.text).find(
            "soapenv:Body/mt:checkRetrieveStatusResponse/mt:result",
            self._XML_NAMESPACES,
        )
        assert result is not None, f"Result node could not be found: {res.text}"

        return result

    def retrieve_zip(
        self, async_process_id: str, **kwargs: Any
    ) -> tuple[Optional[str], Optional[str], list[dict[str, Any]], bytes]:
        result = self.retrieve_retrieve_result(async_process_id, "true", **kwargs)
        state = result.findtext(MTSTATUS, None, self._XML_NAMESPACES)
        error_message = result.findtext("mt:errorMessage", None, self._XML_NAMESPACES)

        messages = [
            {
                "file": message.findtext(MTFILENAME, None, self._XML_NAMESPACES),
                "message": message.findtext(MTPROBLEM, None, self._XML_NAMESPACES),
            }
            for message in result.findall(
                "mt:details/mt:messages", self._XML_NAMESPACES
            )
        ]

        zipfile_base64 = result.findtext("mt:zipFile", None, self._XML_NAMESPACES)
        zipfile = b64decode(zipfile_base64) if zipfile_base64 else b""

        return state, error_message, messages, zipfile

    def check_retrieve_status(
        self, async_process_id: str, **kwargs: KwargsAny
    ) -> tuple[Optional[str], Optional[str], list[dict[str, Optional[str]]]]:
        result = self.retrieve_retrieve_result(async_process_id, "false", **kwargs)
        state = result.findtext(MTSTATUS, None, self._XML_NAMESPACES)
        error_message = result.findtext("mt:errorMessage", None, self._XML_NAMESPACES)

        messages = [
            {
                "file": message.findtext(MTFILENAME, None, self._XML_NAMESPACES),
                "message": message.findtext(MTPROBLEM, None, self._XML_NAMESPACES),
            }
            for message in result.findall(
                "mt:details/mt:messages", self._XML_NAMESPACES
            )
        ]

        return state, error_message, messages
