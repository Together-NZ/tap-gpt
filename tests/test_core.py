"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_gpt.tap import TapGpt

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "auth_token": "test-auth-token",
}


TestTapGpt = get_tap_test_class(
    tap_class=TapGpt,
    config=SAMPLE_CONFIG,
)
