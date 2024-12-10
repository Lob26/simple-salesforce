from typing import Optional

import httpx

from nsss.__version__ import DEFAULT_API_VERSION
from nsss.utils.base import Proxies


def SalesforceLogin(  # NOSONAR: python:S1542
    *,
    username: Optional[str],
    password: Optional[str],
    security_token: Optional[str],
    organizationid: Optional[str],
    sf_version: str,
    proxies: Optional[Proxies],
    session: Optional[httpx.AsyncClient],
    client_id: Optional[str],
    domain: str,
    instance_url: Optional[str],
    consumer_key: Optional[str],
    consumer_secret: Optional[str],
    privatekey_file: Optional[str],
    privatekey: Optional[str],
) -> tuple[str, str]:
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
        * session: An existing httpx.AsyncClient instance to use for requests.\
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
