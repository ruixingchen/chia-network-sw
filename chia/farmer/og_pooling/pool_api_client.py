from aiohttp import ClientSession, ClientTimeout

from chia.farmer.og_pooling.pool_protocol import SubmitPartialOG
from chia.server.server import ssl_context_for_root
from chia.ssl.create_ssl import get_mozilla_ca_crt

timeout = ClientTimeout(total=30)


class PoolApiClient:
    base_url: str

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    async def get_pool_info(self):
        async with ClientSession(timeout=timeout) as client:
            async with client.get(f"{self.base_url}/og/pool_info",
                                  ssl=ssl_context_for_root(get_mozilla_ca_crt()),
                                  ) as res:
                return await res.json()

    async def submit_partial(self, submit_partial: SubmitPartialOG):
        async with ClientSession(timeout=timeout) as client:
            headers = {"content-type": "application/json;", }
            body = submit_partial.to_json_dict()
            async with client.post(f"{self.base_url}/og/partial",
                                   json=body,
                                   headers=headers,
                                   ssl=ssl_context_for_root(get_mozilla_ca_crt()),
                                   ) as res:
                return await res.json()
