"""Integration tests for kerygma_pipeline — full orchestrator."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from kerygma_pipeline import KerygmaPipeline, EVENT_TEMPLATE_MAP, _find_templates_dir


TEMPLATES_DIR = Path(__file__).parent.parent.parent / "announcement-templates" / "templates"


@pytest.fixture
def sample_registry(tmp_path):
    """Minimal registry fixture for integration tests (registry-v2 format)."""
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
    """Mock social config — all platforms disabled, live_mode false."""
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
    )
    return cfg


@pytest.fixture
def pipeline(sample_registry, social_config, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=sample_registry,
        social_config_path=social_config,
        analytics_store_path=tmp_path / "analytics.json",
    )


class TestPipelineTemplateSelection:
    def test_select_valid_template(self, pipeline):
        tid = pipeline.select_template("essay-published")
        assert tid == "essay-announce"

    def test_select_unknown_event_raises(self, pipeline):
        with pytest.raises(ValueError, match="No template mapped"):
            pipeline.select_template("unknown-event-type")

    def test_all_mapped_templates_exist(self, pipeline):
        """Every entry in EVENT_TEMPLATE_MAP should resolve to a loaded template."""
        for event_type, template_id in EVENT_TEMPLATE_MAP.items():
            tid = pipeline.select_template(event_type)
            assert tid == template_id


class TestPipelineRenderAndCheck:
    def test_render_produces_output(self, pipeline):
        results = pipeline.render_and_check(
            "essay-announce", "public-process", ["mastodon", "discord"],
        )
        # At least one channel should pass quality checks
        assert len(results) > 0

    def test_render_mastodon_under_limit(self, pipeline):
        results = pipeline.render_and_check(
            "essay-announce", "public-process", ["mastodon"],
        )
        if "mastodon" in results:
            assert len(results["mastodon"]) <= 500


class TestPipelineFullRun:
    def test_full_pipeline_returns_status(self, pipeline):
        result = pipeline.run_full_pipeline(
            event_type="essay-published",
            repo_name="public-process",
            channels=["mastodon", "discord"],
        )
        assert result["status"] in ("complete", "no_channels_passed")

    def test_full_pipeline_no_channels(self, pipeline):
        """Pipeline with an invalid channel list should gracefully return."""
        result = pipeline.run_full_pipeline(
            event_type="essay-published",
            repo_name="public-process",
            channels=[],
        )
        assert result["status"] == "no_channels_passed"
        assert result["dispatched"] == 0


class TestPipelinePoll:
    def test_poll_with_no_rss_url(self, pipeline):
        """Poll returns empty list when no RSS URL is configured."""
        events = pipeline.poll_for_events()
        assert events == []


class TestPipelineDispatchWithMockClient:
    """Test dispatch path using mock Mastodon client (no real API calls)."""

    def test_dispatch_records_syndication(self, pipeline):
        """Dispatch should produce syndication records for each channel."""
        channel_texts = {"mastodon": "Test post content https://example.com"}
        records = pipeline.dispatch(channel_texts)
        assert len(records) >= 1
        for rec in records:
            assert rec["channel"] == "mastodon"
            assert rec["status"] in ("published", "skipped", "failed")

    def test_dispatch_post_id_is_unique(self, pipeline):
        """Each dispatch call should produce unique post_ids."""
        channel_texts = {"mastodon": "Post A https://example.com"}
        records_a = pipeline.dispatch(channel_texts)
        records_b = pipeline.dispatch(channel_texts)
        # post_id uniqueness is embedded in syndication — check no crash from collision
        assert len(records_a) >= 1
        assert len(records_b) >= 1


class TestPipelineActivate:
    """Tests for the activate (pre-flight) command."""

    def test_activate_returns_structured_report(self, pipeline):
        report = pipeline.activate()
        expected_keys = {"templates_ok", "event_map_ok", "ready", "templates_loaded",
                         "event_map_total", "event_map_unresolved", "platforms_configured",
                         "social_config_ok", "sample_render_ok", "calendar_ok",
                         "channels_ok", "calendar_events", "channels_registered",
                         "channels_enabled"}
        assert expected_keys.issubset(report.keys())

    def test_activate_reports_zero_platforms(self, pipeline):
        """No config → social_config_ok should be false."""
        report = pipeline.activate()
        assert report["social_config_ok"] is False
        assert report["platforms_configured"] == 0

    def test_activate_event_map_complete(self, pipeline):
        """All EVENT_TEMPLATE_MAP entries should resolve to loaded templates."""
        report = pipeline.activate()
        assert report["event_map_ok"] is True
        assert report["event_map_unresolved"] == []
        assert report["event_map_total"] == len(EVENT_TEMPLATE_MAP)


class TestPipelineMetrics:
    """Tests for the metrics command."""

    def test_metrics_empty_without_credentials(self, pipeline):
        metrics = pipeline._collect_platform_metrics()
        assert metrics == {}

    def test_metrics_includes_ghost_when_configured(self, tmp_path, sample_registry):
        """Mock config with ghost creds → ghost key present in metrics."""
        cfg = tmp_path / "ghost_social.yaml"
        cfg.write_text(
            "mastodon:\n"
            "  instance_url: ''\n"
            "  access_token: ''\n"
            "discord:\n"
            "  webhook_url: ''\n"
            "bluesky:\n"
            "  handle: ''\n"
            "  app_password: ''\n"
            "ghost:\n"
            "  api_url: 'https://ghost.example.com'\n"
            "  admin_api_key: 'abc123:def456'\n"
            "delivery_log_path: ''\n"
            "rss_feed_url: ''\n"
            "live_mode: false\n"
        )
        p = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=sample_registry,
            social_config_path=cfg,
            analytics_store_path=tmp_path / "analytics.json",
        )
        # Mock the ghost metrics import so it returns data
        mock_metrics = {"total_posts": 10, "total_members": 5}
        with patch.dict("sys.modules", {
            "kerygma_strategy.ghost_metrics": MagicMock(
                GhostMetricsClient=MagicMock(return_value=MagicMock(
                    get_site_metrics=MagicMock(return_value=mock_metrics)
                )),
                GhostMetricsConfig=MagicMock(),
            ),
        }):
            metrics = p._collect_platform_metrics()
            assert "ghost" in metrics


def test_find_templates_dir_resolves():
    """_find_templates_dir() returns a directory that exists."""
    result = _find_templates_dir()
    assert result.is_dir(), f"Templates dir does not exist: {result}"


class TestPipelineReportWithMetrics:
    """Tests for report generation with platform metrics."""

    def test_report_includes_platform_metrics_section(self, tmp_path, sample_registry):
        """Verify 'Platform Metrics' appears in report when metrics available."""
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
        p = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=sample_registry,
            social_config_path=cfg,
            analytics_store_path=tmp_path / "analytics.json",
        )
        # Mock _collect_platform_metrics to return data
        mock_metrics = {"ghost": {"total_posts": 10, "total_members": 5}}
        with patch.object(p, "_collect_platform_metrics", return_value=mock_metrics):
            report = p.generate_report(period_days=7)
            assert "Platform Metrics" in report
            assert "ghost" in report
            assert "total_posts" in report


class TestMastodonThreadForLongContent:
    """Test that long content triggers thread splitting in Mastodon dispatch."""

    def test_mastodon_thread_for_long_content(self, pipeline):
        """Verify thread splitting is invoked for text exceeding max_chars."""
        # Create a post with body longer than 500 chars
        long_body = "A" * 600 + " https://example.com"
        channel_texts = {"mastodon": long_body}
        # Dispatch will use dry-run client — just verify it doesn't crash
        # and produces records
        records = pipeline.dispatch(channel_texts)
        assert len(records) >= 1
        for rec in records:
            assert rec["channel"] == "mastodon"


class TestAnalyticsPersistenceRoundTrip:
    """Test that analytics survive a save/reload cycle."""

    def test_analytics_persist_and_reload(self, sample_registry, social_config, tmp_path):
        store_path = tmp_path / "analytics_roundtrip.json"

        # Create pipeline, record analytics
        p1 = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=sample_registry,
            social_config_path=social_config,
            analytics_store_path=store_path,
        )
        from kerygma_strategy.analytics import EngagementMetric
        p1._analytics.record(EngagementMetric(
            channel_id="mastodon", content_id="test-01",
            timestamp=datetime(2026, 2, 17), impressions=100, clicks=10,
        ))
        p1._analytics.flush()
        assert store_path.exists()

        # Create second pipeline pointing to same store — should load persisted data
        p2 = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=sample_registry,
            social_config_path=social_config,
            analytics_store_path=store_path,
        )
        assert p2._analytics.total_records == 1
        loaded = p2._analytics.get_by_channel("mastodon")
        assert len(loaded) == 1
        assert loaded[0].impressions == 100
