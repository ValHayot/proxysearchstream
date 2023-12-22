import socket

from pathlib import Path
from typing import TypeAlias

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

STORE_DIR = Path(Path.cwd(), ".proxies")

_CLIENT_ID = "ed914d02-4bdb-4c69-a26d-cf0ec79f4822"
_REDIRECT_URI = "https://auth.globus.org/v2/web/auth-code"

TOKEN_DIR = Path(Path.home(), ".local/share/proxystream")
TOKEN_FILE = TOKEN_DIR / "tokens.json"


def get_ip():
    hostname = socket.getfqdn()
    try:
        ip = socket.gethostbyname_ex(hostname)[2][1]
    except socket.gaierror as e:
        ip = socket.gethostbyname_ex("localhost")[2][0]

    return ip
