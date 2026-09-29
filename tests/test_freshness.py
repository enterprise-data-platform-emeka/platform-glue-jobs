import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

from lib import freshness


def test_transport_age_uses_current_utc(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 29, 12, tzinfo=UTC)

    client = Mock()
    monkeypatch.setattr(freshness, "datetime", Clock)
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda service: client))
    freshness.publish_freshness_metric("fact_orders", "2026-09-29 10:00:00", "edp-dev-fact_orders")
    metric = client.put_metric_data.call_args.kwargs["MetricData"][0]
    assert metric["Value"] == 2.0
    assert metric["Dimensions"][1]["Value"] == "dev"


def test_empty_input_does_not_publish(monkeypatch):
    client = Mock()
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda service: client))
    freshness.publish_freshness_metric("fact_orders", None, "edp-dev-fact_orders")
    client.put_metric_data.assert_not_called()
