"""REST client handling, including GptStream base class."""

from __future__ import annotations

import decimal
import json
import time
import typing as t

import requests
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, SinglePagePaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

_DEBUG_LOG = "/Users/peter/work/.cursor/debug-d9b7ce.log"


class OpenAIAdsPaginator(BaseAPIPaginator[str | None]):
    """Cursor paginator for OpenAI Ads list responses (after / has_more)."""

    def get_next(self, response: requests.Response) -> str | None:
        payload = response.json()
        if isinstance(payload, dict) and payload.get("has_more"):
            return payload.get("last_id")
        return None


class GptStream(RESTStream):
    """gpt stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.ads.openai.com/v1"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator(
            stream=self,
            token=self.config.get("api_token", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance."""
        # #region agent log
        paginator = (
            OpenAIAdsPaginator(None)
            if self.records_jsonpath == "$.data[*]"
            else SinglePagePaginator()
        )
        with open(_DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "sessionId": "d9b7ce",
                        "runId": "post-fix",
                        "hypothesisId": "H1",
                        "location": "client.py:get_new_paginator",
                        "message": "paginator created",
                        "data": {
                            "stream": self.name,
                            "paginator_type": type(paginator).__name__,
                            "records_jsonpath": self.records_jsonpath,
                        },
                        "timestamp": int(time.time() * 1000),
                    },
                )
                + "\n",
            )
        # #endregion
        return paginator

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["after"] = next_page_token
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Append or transform raw data to match expected structure."""
        return row
