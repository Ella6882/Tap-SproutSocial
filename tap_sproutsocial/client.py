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
    def get_next(self, response):
        # Extract current page and total pages from the response
        current_page = response.json()["paging"]["current_page"]
        total_pages = response.json()["paging"]["total_pages"]

        # Check if there is a next page
        if current_page < total_pages:
            # Increment the page number to move to the next page
            return {"page": current_page + 1}
        else:
            # No more pages
            return None

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

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        # Initialize the paginator with the first page as the start value
        if self.name == "post_analytics":
            return PagePaginator(start_value={"page": 1})
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
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
