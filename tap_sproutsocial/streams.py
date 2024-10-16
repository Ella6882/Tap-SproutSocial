"""Stream type classes for tap-sproutsocial."""

from __future__ import annotations

import sys
import re
import typing as t
import json
import os

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_sproutsocial.client import SproutSocialStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

class PostAnalyticsStream(SproutSocialStream):
    """Define custom stream."""
    name = "post_analytics"
    path = "/analytics/posts"
    primary_keys = ["guid"]
    # replication_key = None
    rest_method = "POST"
    schema_filepath = SCHEMAS_DIR / "post_analytics_response.json"

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
            pattern = r'@(?!{})[^\s]+'.format(re.escape(company_name))
            obfuscated_text = re.sub(pattern, '[Obfuscated]', text)
            row['text'] = obfuscated_text

            start_date_str = self.config.get("start_date", "")
            row['start_time'] = f"{start_date_str}T00:00:00"
            
        else:
            self.logger.info(f"Key 'text' not found in row:", row)
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