import json

from globus_sdk import AccessTokenAuthorizer
from globus_sdk import NativeAppAuthClient
from globus_sdk import RefreshTokenAuthorizer

from globus_sdk.scopes import SearchScopes

from utils import _CLIENT_ID
from utils import _REDIRECT_URI
from utils import TOKEN_DIR
from utils import TOKEN_FILE


def create_context():
    client = NativeAppAuthClient(client_id=_CLIENT_ID)
    client.oauth2_start_flow(requested_scopes=[SearchScopes.all])
    return client


def create_authorizer():
    client = create_context()
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE) as f:
            tokens = json.load(f)
        search_refresh_token = tokens["refresh_token"]
        authorizer = RefreshTokenAuthorizer(search_refresh_token, client)
    else:
        authorizer = create_token_file(client, SearchScopes.all)
    return authorizer


def create_token_file(
    client: NativeAppAuthClient,
    scope: str | None = None,
) -> AccessTokenAuthorizer:
    # authenticate first
    client.oauth2_start_flow(
        redirect_uri=_REDIRECT_URI, refresh_tokens=True, requested_scopes=scope
    )

    url = client.oauth2_get_authorize_url()
    print("Please visit the following url to authenticate:")
    print(url)

    auth_code = input("Enter the auth code:")
    auth_code = auth_code.strip()
    tokens = client.oauth2_exchange_code_for_tokens(auth_code)

    TOKEN_DIR.mkdir(exist_ok=True, parents=True)
    _SEARCH_SERVER = "search.api.globus.org"
    search_access_token = tokens.by_resource_server[_SEARCH_SERVER]["access_token"]
    authorizer = AccessTokenAuthorizer(access_token=search_access_token)

    with open(TOKEN_FILE, "w+") as f:
        json.dump(tokens.by_resource_server[_SEARCH_SERVER], f)

    return authorizer
