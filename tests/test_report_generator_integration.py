"""Integration tests for ReportGenerator wiring in kerygma_pipeline."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from kerygma_pipeline import KerygmaPipeline
from kerygma_strategy.analytics import EngagementMetric


TEMPLATES_DIR = Path(__file__).parent.parent.parent / "announcement-templates" / "templates"


@pytest.fixture
def registry(tmp_path):
    reg = {
        "organs": {
            "ORGAN-V": {
                "repos": [
                    {
                        "name": "public-process",
                        "description": "Public accountability ledger",
                        "tier": "flagship",
                        "github_url": "https://github.com/organvm-v-logos/public-process",
                        "implementation_status": "PRODUCTION",
                    }
                ]
            }
        }
    }
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(reg))
    return path


@pytest.fixture
def social_config(tmp_path):
    cfg = tmp_path / "social.yaml"
    cfg.write_text(
        "mastodon:\n"
        "  instance_url: ''\n"
        "  access_token: ''\n"
        "discord:\n"
        "  webhook_url: ''\n"
        "bluesky:\n"
        "  handle: ''\n"
        "  app_password: ''\n"
        "delivery_log_path: ''\n"
        "rss_feed_url: ''\n"
        "live_mode: false\n"
        "channels: []\n"
    )
    return cfg


@pytest.fixture
def pipeline(registry, social_config, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=registry,
        social_config_path=social_config,
        analytics_store_path=tmp_path / "analytics.json",
    )


class TestReportGeneratorIntegration:
    def test_report_uses_report_generator_format(self, pipeline):
        """Report output matches ReportGenerator's markdown format."""
        report = pipeline.generate_report(period_days=7)
        assert "# Distribution Report (weekly)" in report
        assert "Schedule Summary" in report
        assert "Pending entries:" in report

    def test_weekly_vs_monthly(self, pipeline):
        """Different period_days produce different report labels."""
        weekly = pipeline.generate_report(period_days=7)
        monthly = pipeline.generate_report(period_days=30)
        assert "(weekly)" in weekly
        assert "(monthly)" in monthly

    def test_report_includes_analytics_data(self, pipeline):
        """Report reflects recorded analytics."""
        pipeline._analytics.record(EngagementMetric(
            channel_id="mastodon",
            content_id="test-content",
            timestamp=datetime.now(),
            impressions=100,
            clicks=10,
        ))
        pipeline._analytics.flush()

        report = pipeline.generate_report(period_days=7)
        assert "mastodon" in report
        assert "Total impressions:" in report
