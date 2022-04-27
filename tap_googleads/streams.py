"""Stream type classes for tap-googleads."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_googleads.client import GoogleAdsStream
from tap_googleads.auth import GoogleAdsAuthenticator

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class CustomerStream(GoogleAdsStream):
    """Define custom stream."""

    @property
    def path(self):
        return "/customers/" + self.config["customer_id"]

    name = "stream_customers"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "customer.json"


class AccessibleCustomers(GoogleAdsStream):
    """Accessible Customers"""

    path = "/customers:listAccessibleCustomers"
    name = "stream_accessible_customers"
    primary_keys = ["resource_names"]
    replication_key = None
    # TODO add an assert for one record
    #    schema_filepath = SCHEMAS_DIR / "customer.json"
    schema = th.PropertiesList(
        th.Property("resourceNames", th.ArrayType(th.StringType))
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"resourceNames": ["customers/" + self.config.get("customer_id")]}


class CustomerHierarchyStream(GoogleAdsStream):
    """
    Customer Hierarchy, inspiration from Google here
    https://developers.google.com/google-ads/api/docs/account-management/get-account-hierarchy.

    This stream is stictly to be the Parent Stream, to let all Child Streams
    know when to query the down stream apps.

    """

    # TODO add a seperate stream to get the Customer information and return i
    rest_method = "POST"

    @property
    def path(self):
        # Paramas
        path = "/customers/{client_id}"
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path

    @property
    def gaql(self):
        return """
	SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
	"""

    records_jsonpath = "$.results[*]"
    name = "stream_customer_hierarchy"
    primary_keys = ["customer_client__id"]
    replication_key = None
    parent_stream_type = AccessibleCustomers
    schema = th.PropertiesList(
        th.Property(
            "customerClient",
            th.ObjectType(
                th.Property("resourceName", th.StringType),
                th.Property("clientCustomer", th.StringType),
                th.Property("level", th.StringType),
                th.Property("timeZone", th.StringType),
                th.Property("manager", th.BooleanType),
                th.Property("descriptiveName", th.StringType),
                th.Property("currencyCode", th.StringType),
                th.Property("id", th.StringType),
            ),
        )
    ).to_dict()

    # Goal of this stream is to send to children stream a dict of
    # login-customer-id:customer-id to query for all queries downstream
    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """

        context["client_id"] = self.config.get("customer_id")
        for row in self.request_records(context):
            row = self.post_process(row, context)
            # Don't search Manager accounts as we can't query them for everything
            if row["customerClient"]["manager"] == True:
                continue
            yield row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"client_id": self.config.get("customer_id")}


class ReportsStream(GoogleAdsStream):
    rest_method = "POST"
    parent_stream_type = CustomerHierarchyStream

    @property
    def gaql(self):
        raise NotImplementedError

    @property
    def path(self):
        # Paramas
        path = "/customers/{client_id}"
        path = path + "/googleAds:search"
        path = path + "?pageSize=10000"
        path = path + f"&query={self.gaql}"
        return path


class CampaignsStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
        SELECT 
        campaign.advertising_channel_sub_type, 
        campaign.advertising_channel_type, 
        campaign.bidding_strategy, 
        campaign.bidding_strategy_type, 
        campaign.campaign_budget, 
        campaign.end_date, 
        campaign.geo_target_type_setting.positive_geo_target_type, 
        campaign.id, 
        campaign.labels, 
        campaign.name, 
        campaign.optimization_goal_setting.optimization_goal_types, 
        campaign.resource_name, 
        campaign.serving_status, 
        campaign.start_date, 
        campaign.status 
        FROM campaign 
        """

    records_jsonpath = "$.results[*]"
    name = "stream_campaign"
    primary_keys = ["campaign__id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign.json"



class AdGroupAssetStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT 
        ad_group_asset.ad_group, 
        ad_group_asset.asset, 
        ad_group_asset.field_type, 
        ad_group_asset.resource_name, 
        ad_group_asset.status
        FROM ad_group_asset 
       """

    records_jsonpath = "$.results[*]"
    name = "stream_adgroups"
    primary_keys = ["ad_group_asset__ad_group"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_group.json"


class AdStream(ReportsStream):
    """Define custom stream."""

    @property
    def gaql(self):
        return """
       SELECT 
        ad_group_ad_asset_view.ad_group_ad, 
        ad_group_ad_asset_view.asset, 
        ad_group_ad_asset_view.enabled, 
        ad_group_ad_asset_view.field_type, 
        ad_group_ad_asset_view.performance_label, 
        ad_group_ad_asset_view.policy_summary, 
        ad_group_ad_asset_view.resource_name
        FROM ad_group_ad_asset_view 
       """

    records_jsonpath = "$.results[*]"
    name = "stream_ads"
    primary_keys = None
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad.json"



class PerformanceStream(ReportsStream):
    """PerformanceStream"""

    @property
    def gaql(self):
        return f"""
    SELECT 
    segments.date, 
    ad_group_criterion.criterion_id, 
    campaign.id, 
    ad_group.id, 
    metrics.absolute_top_impression_percentage, 
    metrics.active_view_impressions, 
    metrics.all_conversions, 
    metrics.all_conversions_value, 
    metrics.clicks, 
    metrics.conversions, 
    metrics.conversions_value, 
    metrics.cost_micros, 
    metrics.gmail_forwards, 
    metrics.gmail_saves, 
    metrics.impressions, 
    metrics.search_absolute_top_impression_share, 
    metrics.search_impression_share, 
    metrics.search_click_share, 
    metrics.search_top_impression_share, 
    metrics.video_views, 
    metrics.video_quartile_p100_rate, 
    metrics.video_quartile_p25_rate, 
    metrics.video_quartile_p50_rate, 
    metrics.video_quartile_p75_rate 
    FROM keyword_view 
    WHERE segments.date >= {self.start_date} and segments.date <= {self.end_date}
    """

    records_jsonpath = "$.results[*]"
    name = "stream_performance"
    primary_keys = None
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "performance.json"


