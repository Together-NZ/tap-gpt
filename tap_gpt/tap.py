"""gpt tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_gpt import streams


class TapGpt(Tap):
    """gpt tap class."""

    name = "tap-gpt"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType,
            required=True,
            secret=True,
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="the latest date"
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.GptStream]:
        """Return a list of discovered streams."""
        return [
            streams.AdAccountStream(self),
            streams.CampaignStream(self),
            streams.AdGroupStream(self),
            streams.AdsStream(self),
            streams.AdInsightsStream(self),
            streams.ConversionAdsStream(self)
        ]


if __name__ == "__main__":
    TapGpt.cli()
