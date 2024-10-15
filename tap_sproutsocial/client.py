"""REST client handling, including SproutSocialStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Iterable

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
from datetime import datetime

import json

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

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
        return PagePaginator(start_value={"page": 1})

    def extract_fields_and_metrics(self) -> tuple[list[str], list[str]]:
        """Extract fields from properties and metrics from post_analytics.json."""
        config_file = SCHEMAS_DIR / "post_analytics.json"
        with config_file.open() as f:
            config_data = json.load(f)

            data_properties = config_data.get("properties", {}).get("data", {}).get("items", {}).get("properties", {})
            fields = [key for key in data_properties.keys() if key != "metrics"]
            metrics = list(data_properties.get("metrics", {}).get("properties", {}).keys())

        return fields, metrics

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
        customer_profile_id = self.config.get("customer_profile_id", None)
        start_date_str = self.config.get("start_date", "")
        start_date = f"{start_date_str}T00:00:00"
        end_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        payload: dict = {}
        payload["limit"] = 100 # Default: 50, Max: 100
        payload["page"] = 1  # Default page number
        if self.name == "post_analytics":
            fields, metrics = self.extract_fields_and_metrics()
            payload["fields"] = fields
            payload["metrics"] = metrics

            filters = [
                f"customer_profile_id.eq({customer_profile_id})", 
                f"created_time.in({start_date}..{end_date})"
            ]

            payload["sort"] = ["created_time:asc"]
            payload["filters"] = filters

            if next_page_token:
                payload.update(next_page_token)
        return payload


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        company_name=self.config.get("company_name", None)
        text = row.get('text')

        if text is not None:
            # Replace words starting with '@' with '[Obfuscated]' unless it's '@company_name'
            obfuscated_text = ' '.join(
                '[Obfuscated]' if word.startswith('@') and word != f"@{company_name}" else word
                for word in text.split()
            )
            row['text'] = obfuscated_text
        else:
            self.logger.info(f"Key 'text' not found in row:", row)
        return row