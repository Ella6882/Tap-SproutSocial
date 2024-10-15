"""Stream type classes for tap-sproutsocial."""

from __future__ import annotations

import sys
import typing as t

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
    schema_filepath = SCHEMAS_DIR / "post_analytics.json"  # noqa: ERA001
    rest_method = "POST"