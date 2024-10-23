"""SproutSocial tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_sproutsocial import streams

class TapSproutSocial(Tap):
    """SproutSocial tap class."""

    name = "tap-sproutsocial"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "token_name",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "version",
            th.StringType,
            description="Version ID",
        ),
        th.Property(
            "customer_id",
            th.StringType,
            required=True,
            secret=True,
            description="Customer ID",
        ),
        th.Property(
            "customer_profile_id",
            th.StringType,
            required=True,
            secret=True,
            description="List of customer profile IDs you have access to.",
        ),
        th.Property(
            "company_name",
            th.StringType,
            secret=True,
            description="Company Name",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.SproutSocialStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.PostAnalyticsStream(self),
            streams.CustomerTagsStream(self),
        ]


if __name__ == "__main__":
    TapSproutSocial.cli()
