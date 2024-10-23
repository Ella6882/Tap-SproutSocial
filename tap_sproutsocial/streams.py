"""Stream type classes for tap-sproutsocial."""

from __future__ import annotations

import sys
import re
import typing as t
import json
import os
from datetime import datetime

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sproutsocial.client import SproutSocialStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

class CustomerProfilesStream(SproutSocialStream):
    """Define Conversations List stream

    This stream is used to call the /metadata/customer endpoint which returns a list
    of customer profile ids.

    This stream is used as a parent stream for PostAnalyticsStream.
    """
    name = "customer_profiles"
    path = "/metadata/customer"
    primary_keys = str(["customer_profile_id"])
    schema_filepath = SCHEMAS_DIR / "customer_profiles.json"

    def get_child_context(self, record: dict, context: dict | None) -> str:
        """Returns the customer profile IDs for the children streams.

        The metadata/customer endpoint contains the customer profile id associated 
        with the account and is required for other endpoints including the PostAnalyticsStream.
    
        The returned string is the customer profile ID.
        """
        return {"customer_profile_id_list": str(record["customer_profile_id"])}

class CustomerTagsStream(SproutSocialStream):
    """Define Customer Tags stream."""
    name = "customer_tags"
    path = "/metadata/customer/tags"
    primary_keys = ["tag_id"]
    rest_method = "GET"
    schema_filepath = SCHEMAS_DIR / "customer_tags.json"

class PostAnalyticsStream(SproutSocialStream):
    """Define Post Analytics stream."""
    name = "post_analytics"
    path = "/analytics/posts"
    primary_keys = ["guid"]
    rest_method = "POST"
    schema_filepath = SCHEMAS_DIR / "post_analytics_response.json"
    ignore_parent_replication_keys = True
    parent_stream_type = CustomerProfilesStream

    def extract_fields_and_metrics(self) -> tuple[list[str], list[str]]:
        """Extract fields from properties and metrics from post_analytics.json."""
        SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
        config_file = SCHEMAS_DIR / "post_analytics_request.json"

        with config_file.open() as f:
            config_data = json.load(f)

            data_properties = config_data.get("properties", {}).get("data", {}).get("items", {}).get("properties", {})
            fields = [key for key in data_properties.keys() if key != "metrics"]
            metrics = list(data_properties.get("metrics", {}).get("properties", {}).keys())

        return fields, metrics

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

            customer_profile_id_list = context.pop('customer_profile_id_list')
            
            filters = [
                f"customer_profile_id.eq({customer_profile_id_list})", 
                f"created_time.in({start_date}..{end_date})",
            ]

            payload["sort"] = ["guid:asc"]
            payload["filters"] = filters

            if next_page_token is not None:
                self.logger.info(f"Next token: {next_page_token}")
                filters.append(f"guid.gt({next_page_token})")

        return payload

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Modifies an individual record from a data stream 
            by obfuscating certain parts of the text, as required.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        company_name=self.config.get("company_name", None)
        text = row.get('text')

        if text is not None and company_name is not None:
            pattern = r'@(?!{})[^\s]+'.format(re.escape(company_name))
            obfuscated_text = re.sub(pattern, '[Obfuscated]', text)
            row['text'] = obfuscated_text

            start_date_str = self.config.get("start_date", "")
            row['start_time'] = f"{start_date_str}T00:00:00"

        return row

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        try:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
        except Exception as e:
            self.logger.warning(f"An error occurred: {e}")