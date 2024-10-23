"""REST client handling, including SproutSocialStream base class."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any, Iterable

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
import singer_sdk.typing as th

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

class PagePaginator(BaseAPIPaginator):
    """Process the sorted response data and extract 
    the last 'guid' value from the response.

    This 'guid' will be used as the cursor for the next request.
    """
    def __init__(self, start_value=None):
        # Set a default start_value for the first request
        super().__init__(start_value=start_value)

    def get_next(self, response) -> str:
        """Get the 'guid' from the last record in the list."""
        records = response.json()
        if not records:
            return None

        cursor = records['data'][-1]['guid']
        return cursor if cursor else None


class SproutSocialStream(RESTStream):
    """SproutSocial stream class."""

    records_jsonpath = "$.data.[*]"

    # Update this value if necessary or override `get_new_paginator`.
    # next_page_token_jsonpath = "$.next_page"  # noqa: S105

    fields = None  # Post Analytics Stream will use this

    @property
    def url_base(self) -> str:
        """Return the API URL root. Version is set to v1 as default.
        """
        version=self.config.get("version", "v1")
        customer_id=self.config.get("customer_id", None)
        return f"https://api.sproutsocial.com/{version}/{customer_id}"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("token_name", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/json",
        }
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        if self.name == "post_analytics":
            return PagePaginator()
        else:
           return super().get_new_paginator()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any] = None
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        return params

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.
        By default, no payload will be sent (return None).
        Args:
            context: The stream context.
            next_page_token: The next page index or value.
        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
