import base64
import html
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Optional, cast

import httpx
import jwt

from nsss.__version__ import DEFAULT_CLIENT_ID_PREFIX
from nsss.utils import (
    Proxies,
    SalesforceAuthenticationFailed,
    fetch_unique_xml_element_value,
    to_mount,
)

logger = logging.getLogger(__name__)


def SalesforceLogin(**kwargs: Any) -> tuple[str, str]:  # NOSONAR
    """
        Return a tuple of `(session_id, sf_instance)` where `session_id` is the\
    session ID to use for authentication to Salesforce and `sf_instance` is\
    the domain of the instance of Salesforce to use for the session.\

    Arguments:
        * username: The Salesforce username to use for authentication
        * password: The password for the username
        * security_token: The security token for the username
        * organizationid: The organization ID for the username
        * sf_version: The Salesforce API version to use. For example "27.0"
        * proxies: The optional map of scheme to proxy server
        * session: An existing httpx.Client instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
        * client_id: The ID of this client
        * domain: The domain to using for connecting to Salesforce.\
                Use common domains, such as 'login' or 'test', or Salesforce My domain.\
                If not used, will default to 'login'.
        * instance_url: Non-standard instance url (instance.my) used\
                for connecting to Salesforce with JWT tokens.
        * consumer_key: The consumer key for the connected app
        * consumer_secret: The consumer secret for the connected app
        * privatekey_file: The path to the private key file for the connected app
        * privatekey: The private key for the connected app

    NOTE: security_token an organizationId are mutually exclusive
    NOTE: privatekey_file and privatekey are mutually exclusive
    """
    username: Optional[str] = kwargs.get("username")
    password: Optional[str] = kwargs.get("password")
    security_token: Optional[str] = kwargs.get("security_token")
    organizationid: Optional[str] = kwargs.get("organizationid")
    sf_version: str = kwargs["sf_version"]
    proxies: Optional[Proxies] = kwargs.get("proxies")
    session: Optional[httpx.Client] = kwargs.get("session")
    client_id: Optional[str] = kwargs.get("client_id")
    domain: Literal["login"] | str = kwargs["domain"]
    instance_url: Optional[str] = kwargs.get("instance_url")
    consumer_key: Optional[str] = kwargs.get("consumer_key")
    consumer_secret: Optional[str] = kwargs.get("consumer_secret")
    privatekey_file: Optional[str] = kwargs.get("privatekey_file")
    privatekey: Optional[str] = kwargs.get("privatekey")

    sf_version = sf_version.lstrip("v")
    assert sf_version.replace(".", "", 1).isdecimal(), "Invalid Salesforce API version"

    client_id = (
        f"{DEFAULT_CLIENT_ID_PREFIX}/{client_id}"
        if client_id
        else DEFAULT_CLIENT_ID_PREFIX
    )

    username = html.escape(username) if username else None
    password = html.escape(password) if password else None

    if security_token:
        # Security Token Soap request body
        login_soap_request_body = f"""<?xml version="1.0" encoding="utf-8" ?>
<env:Envelope
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:urn="urn:partner.soap.sforce.com">
    <env:Header>
        <urn:CallOptions>
            <urn:client>{client_id}</urn:client>
            <urn:defaultNamespace>sf</urn:defaultNamespace>
        </urn:CallOptions>
    </env:Header>
    <env:Body>
        <n1:login xmlns:n1="urn:partner.soap.sforce.com">
            <n1:username>{username}</n1:username>
            <n1:password>{password}{security_token}</n1:password>
        </n1:login>
    </env:Body>
</env:Envelope>"""
    elif organizationid:
        # IP Filtering Login Soap request body
        login_soap_request_body = f"""<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:urn="urn:partner.soap.sforce.com">
    <soapenv:Header>
        <urn:CallOptions>
            <urn:client>{client_id}</urn:client>
            <urn:defaultNamespace>sf</urn:defaultNamespace>
        </urn:CallOptions>
        <urn:LoginScopeHeader>
            <urn:organizationId>{organizationid}</urn:organizationId>
        </urn:LoginScopeHeader>
    </soapenv:Header>
    <soapenv:Body>
        <urn:login>
            <urn:username>{username}</urn:username>
            <urn:password>{password}</urn:password>
        </urn:login>
    </soapenv:Body>
</soapenv:Envelope>"""
    elif username and password and consumer_key and consumer_secret:
        return token_login(
            f"https://{domain}.salesforce.com/services/oauth2/token",
            {
                "grant_type": "client_credentials",
                "client_id": consumer_key,
                "client_secret": consumer_secret,
                "username": html.unescape(username),
                "password": html.unescape(password) if password else None,
            },
            domain,
            consumer_key,
            None,
            proxies,
            session,
        )
    elif username and password:
        # IP Filtering for non self-service users
        login_soap_request_body = f"""<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:urn="urn:partner.soap.sforce.com">
    <soapenv:Header>
        <urn:CallOptions>
            <urn:client>{client_id}</urn:client>
            <urn:defaultNamespace>sf</urn:defaultNamespace>
        </urn:CallOptions>
    </soapenv:Header>
    <soapenv:Body>
        <urn:login>
            <urn:username>{username}</urn:username>
            <urn:password>{password}</urn:password>
        </urn:login>
    </soapenv:Body>
</soapenv:Envelope>"""
    elif username and consumer_key and (privatekey_file or privatekey):
        token_domain = instance_url if instance_url is not None else domain
        expiration = datetime.now(UTC) + timedelta(minutes=3)
        payload = {
            "iss": consumer_key,
            "sub": html.unescape(username),
            "aud": f"https://{domain}.salesforce.com",
            "exp": f"{expiration.timestamp():.0f}",
        }
        key = (
            Path(privatekey_file).read_bytes()
            if privatekey_file
            else cast(str, privatekey).encode("utf-8")
        )

        return token_login(
            f"https://{token_domain}.salesforce.com/services/oauth2/token",
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt.encode(payload, key, algorithm="RS256"),
            },
            domain,
            consumer_key,
            None,
            proxies,
            session,
        )
    elif consumer_key and consumer_secret and domain not in ("login", "test", None):
        authorization = base64.b64encode(
            f"{consumer_key}:{consumer_secret}".encode()
        ).decode()
        headers = {"Authorization": f"Basic {authorization}"}
        return token_login(
            f"https://{domain}.salesforce.com/services/oauth2/token",
            {"grant_type": "client_credentials"},
            domain,
            consumer_key,
            httpx.Headers(headers),
            proxies,
            session,
        )
    else:
        raise SalesforceAuthenticationFailed(
            code="INVALID AUTH",
            message="You must submit either a security token or organizationId for authentication",
        )

    soap_url = f"https://{domain}.salesforce.com/services/Soap/u/{sf_version}"
    login_soap_request_headers = {
        "content-type": "text/xml",
        "charset": "UTF-8",
        "SOAPAction": "login",
    }

    return soap_login(
        soap_url,
        login_soap_request_body,
        login_soap_request_headers,
        proxies,
        session,
    )


def token_login(
    token_url: str,
    token_data: dict[str, Any],
    domain: str,
    consumer_key: str,
    headers: Optional[httpx.Headers],
    proxies: Optional[Proxies],
    session: Optional[httpx.Client] = None,
) -> tuple[str, str]:
    """Process OAuth 2.0 JWT Bearer Token Flow."""
    with session or httpx.Client(
        headers=headers, mounts=to_mount(proxies) if proxies else None
    ) as client:
        response = client.post(token_url, data=token_data)

    json_response: dict[str, str]
    try:
        response.raise_for_status()
        json_response = response.json()
        return (
            json_response["access_token"],
            json_response["instance_url"]
            .replace("http://", "")
            .replace("https://", ""),
        )
    except json.JSONDecodeError as exc:
        raise SalesforceAuthenticationFailed(
            response.status_code, response.text
        ) from exc
    except httpx.HTTPStatusError as exc:
        json_response = exc.response.json()
        except_msg = json_response["error_description"]
        if except_msg == "user hasn't approved this consumer":
            auth_url = f"https://{domain}.salesforce.com/services/oauth2/authorize?response_type=code&client_id={consumer_key}&redirect_uri=<approved URI>"
            logger.warning(
                f"If your connected app policy is set to 'All users may self-authorize', you may need to authorize this application first. Browse to {auth_url} in order to Allow Access. Check first to ensure you have a valid <approved URI>."
            )
        raise SalesforceAuthenticationFailed(
            json_response.get("error"), except_msg
        ) from exc


def soap_login(
    soap_url: str,
    request_body: str,
    login_soap_request_headers: dict[str, str],
    proxies: Optional[Proxies],
    session: Optional[httpx.Client],
) -> tuple[str, str]:
    """
    Return a tuple of `(session_id, sf_instance)` where `session_id` is the\
    session ID to use for authentication to Salesforce and `sf_instance` is\
    the domain of the instance of Salesforce to use for the session.\

    Arguments:
        * soap_url: The URL to use for the SOAP request
        * request_body: The body of the SOAP request
        * login_soap_request_headers: The headers for the SOAP request
        * proxies: The optional map of scheme to proxy server
        * session: An existing httpx.Client instance to use for requests.\
                This enables the use of httpx features not otherwise exposed by the library.
    """
    with session or httpx.Client(
        mounts=to_mount(proxies) if proxies else None
    ) as client:
        response = client.post(
            soap_url,
            data=json.loads(request_body),
            headers=login_soap_request_headers,
        )

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        except_code = (
            fetch_unique_xml_element_value(response.content, "sf:exceptionCode")
            or response.status_code
        )
        except_message = (
            fetch_unique_xml_element_value(response.content, "sf:exceptionMessage")
            or response.text
        )
        raise SalesforceAuthenticationFailed(except_code, except_message)

    if (
        (session_id := fetch_unique_xml_element_value(response.content, "sessionId"))
    and (server_url := fetch_unique_xml_element_value(response.content, "serverUrl"))
    ):  # fmt:skip
        return session_id, server_url.split("/")[2].replace("-api", "")

    except_code = (
        fetch_unique_xml_element_value(response.content, "sf:exceptionCode")
        or "UNKNOWN_EXCEPTION_CODE"
    )
    except_message = (
        fetch_unique_xml_element_value(response.content, "sf:exceptionMessage")
        or "UNKNOWN_EXCEPTION_MESSAGE"
    )
    raise SalesforceAuthenticationFailed(except_code, except_message)
