"""Stream type classes for tap-gpt."""

from __future__ import annotations

import typing as t
from datetime import date

from singer_sdk import typing as th

from tap_gpt.client import GptStream


class AdAccountStream(GptStream):
    """Define custom stream."""

    name = "account"
    path = "/ad_account"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Ad account ID"),
        th.Property("name", th.StringType, description="Ad account name"),
        th.Property("status", th.StringType, description="Account status"),
        th.Property("currency_code", th.StringType, description="Account currency"),
        th.Property("timezone", th.StringType, description="Account timezone"),
        th.Property("url", th.StringType, description="Account URL"),
        th.Property("preview_url", th.StringType, description="Preview image URL"),
        th.Property("account_integrity_review", th.ObjectType()),
        th.Property("review", th.ObjectType()),
    ).to_dict()
    
class CampaignStream(GptStream):
    name = "campaigns"
    path = "/campaigns"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Campaign ID"),
        th.Property("name", th.StringType, description="Campaign name"),
        th.Property("status", th.StringType, description="Campaign status"),
        th.Property("objective", th.StringType, description="Campaign objective"),
        th.Property("bidding_type", th.StringType, description="Bidding type"),
        th.Property("billing_event_type", th.StringType, description="Billing event type"),
        th.Property("created_at", th.IntegerType, description="Created at (unix timestamp)"),
        th.Property("updated_at", th.IntegerType, description="Updated at (unix timestamp)"),
        th.Property("start_time", th.IntegerType, description="Start time (unix timestamp)"),
        th.Property("end_time", th.IntegerType, description="End time (unix timestamp)"),
        th.Property(
            "budget",
            th.ObjectType(
                th.Property(
                    "daily_spend_limit_micros",
                    th.IntegerType,
                    description="Daily spend limit in micros",
                ),
            ),
        ),
        th.Property(
            "targeting",
            th.ObjectType(
                th.Property(
                    "locations",
                    th.ObjectType(
                        th.Property(
                            "include",
                            th.ArrayType(
                                th.ObjectType(
                                    th.Property("id", th.StringType),
                                    th.Property("type", th.StringType),
                                    th.Property("country_code", th.StringType),
                                    th.Property("name", th.StringType),
                                    th.Property("region_code", th.StringType, nullable=True),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        th.Property(
            "conversion_event_setting_ids",
            th.ArrayType(th.StringType),
            description="Conversion event setting IDs",
        ),
        th.Property("business_agent_id", th.StringType, nullable=True),
        th.Property("description", th.StringType, nullable=True),
        th.Property("landing_page_configuration", th.ObjectType(), nullable=True),
        th.Property("mode", th.StringType, nullable=True),
        th.Property("product_feed_id", th.StringType, nullable=True),
    ).to_dict()

    def get_url_params(
        self,
        context: t.Any | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        params["limit"] = 500
        params["order"] = "desc"
        return params

    def get_child_context(self, record: dict, context: t.Any) -> dict:
        return {"campaign_id": record["id"]}


class AdGroupStream(GptStream):
    """Ad groups for each campaign."""

    name = "ad_groups"
    path = "/ad_groups"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.data[*]"
    parent_stream_type = CampaignStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Ad group ID"),
        th.Property("campaign_id", th.StringType, description="Parent campaign ID"),
        th.Property("name", th.StringType, description="Ad group name"),
        th.Property("status", th.StringType, description="Ad group status"),
        th.Property("description", th.StringType, nullable=True),
        th.Property("user_external_id", th.StringType, nullable=True),
        th.Property("created_at", th.IntegerType, description="Created at (unix timestamp)"),
        th.Property("updated_at", th.IntegerType, description="Updated at (unix timestamp)"),
        th.Property(
            "context_hints",
            th.ArrayType(th.StringType),
            description="Audience or placement hints",
        ),
        th.Property(
            "bidding_config",
            th.ObjectType(
                th.Property("billing_event_type", th.StringType),
                th.Property("max_bid_micros", th.IntegerType, nullable=True),
                th.Property("strategy", th.StringType, nullable=True),
                th.Property(
                    "custom_audience_bid_multipliers",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("custom_audience_id", th.StringType),
                            th.Property("bid_multiplier_micros", th.IntegerType),
                        ),
                    ),
                    nullable=True,
                ),
            ),
        ),
        th.Property("landing_page_configuration", th.ObjectType(), nullable=True),
        th.Property(
            "product_set",
            th.ObjectType(
                th.Property("product_feed_id", th.StringType),
                th.Property(
                    "filters",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("field", th.StringType),
                            th.Property("operator", th.StringType),
                            th.Property("values", th.ArrayType(th.StringType)),
                        ),
                    ),
                    nullable=True,
                ),
            ),
            nullable=True,
        ),
    ).to_dict()

    def get_url_params(
        self,
        context: t.Any | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        params["campaign_id"] = context["campaign_id"]
        params["limit"] = 500
        return params

    def post_process(self, row: dict, context: t.Any | None = None) -> dict | None:
        row["campaign_id"] = context["campaign_id"]
        return row

    def get_child_context(self, record: dict, context: t.Any) -> dict:
        return {
            "campaign_id": context["campaign_id"],
            "ad_group_id": record["id"],
        }


class AdsStream(GptStream):
    """Ads for each ad group."""

    name = "ads"
    path = "/ads"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.data[*]"
    parent_stream_type = AdGroupStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Ad ID"),
        th.Property("campaign_id", th.StringType, description="Parent campaign ID"),
        th.Property("ad_group_id", th.StringType, description="Parent ad group ID"),
        th.Property("name", th.StringType, description="Ad name"),
        th.Property("status", th.StringType, description="Ad status"),
        th.Property("review_status", th.StringType, description="Review status"),
        th.Property("created_at", th.IntegerType, description="Created at (unix timestamp)"),
        th.Property("updated_at", th.IntegerType, description="Updated at (unix timestamp)"),
        th.Property(
            "creative",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("title", th.StringType),
                th.Property("body", th.StringType),
                th.Property("target_url", th.StringType),
                th.Property("file_id", th.StringType),
                th.Property(
                    "image_crop",
                    th.ObjectType(
                        th.Property("x", th.NumberType),
                        th.Property("y", th.NumberType),
                        th.Property("width", th.NumberType),
                        th.Property("height", th.NumberType),
                    ),
                ),
            ),
        ),
        th.Property(
            "review",
            th.ObjectType(
                th.Property("status", th.StringType),
                th.Property("reason", th.StringType, nullable=True),
            ),
        ),
        th.Property("landing_page_configuration", th.ObjectType(), nullable=True),
    ).to_dict()

    def get_url_params(
        self,
        context: t.Any | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        params["ad_group_id"] = context["ad_group_id"]
        params["limit"] = 500
        return params

    def post_process(self, row: dict, context: t.Any | None = None) -> dict | None:
        row["campaign_id"] = context["campaign_id"]
        row["ad_group_id"] = context["ad_group_id"]
        return row

    def get_child_context(self, record: dict, context: t.Any) -> dict:
        return {
            "campaign_id": context["campaign_id"],
            "ad_group_id": context["ad_group_id"],
            "ad_id": record["id"],
        }


class AdInsightsStream(GptStream):
    """Daily ad-level performance metrics."""

    name = "ad_insights"
    path = "/ads/{ad_id}/insights"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "start_time"
    records_jsonpath = "$.data[*]"
    parent_stream_type = AdsStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Insight row ID"),
        th.Property("campaign_id", th.StringType, description="Campaign ID"),
        th.Property("ad_group_id", th.StringType, description="Ad group ID"),
        th.Property("ad_id", th.StringType, description="Ad ID"),
        th.Property("ad_name", th.StringType, description="Ad name"),
        th.Property("start_time", th.IntegerType, description="Period start (unix timestamp)"),
        th.Property("end_time", th.IntegerType, description="Period end (unix timestamp)"),
        th.Property("impressions", th.IntegerType, description="Impressions"),
        th.Property("clicks", th.IntegerType, description="Clicks"),
        th.Property("spend", th.NumberType, description="Spend in account currency"),
    ).to_dict()

    def get_url_params(
        self,
        context: t.Any | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        start_date = str(self.config["start_date"])[:10]
        configured_end = str(self.config.get("end_date") or date.today())[:10]
        end_date = min(configured_end, date.today().isoformat())
        params.update(
            {
                "time_granularity": "daily",
                "fields[]": [
                    "ad.impressions",
                    "ad.clicks",
                    "ad.spend",
                    "ad.id",
                    "ad.name",
                    "campaign.id",
                    "ad_group.id",
                ],
                "time_ranges[]": [
                    f'{{"type":"date_range","since":"{start_date}","until":"{end_date}"}}',
                ],
            },
        )
        return params
