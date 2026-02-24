"""End-to-end dry-run integration test for the full kerygma pipeline.

Validates the complete flow: select → render → check → dispatch → analytics
without touching real APIs (all clients in dry-run mode).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kerygma_pipeline import KerygmaPipeline, EVENT_TEMPLATE_MAP


TEMPLATES_DIR = Path(__file__).parent.parent.parent / "announcement-templates" / "templates"


@pytest.fixture
def registry(tmp_path):
    """Minimal registry with ORGAN-V repo."""
    reg = {
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
        },
        "ORGAN-III": {
            "repos": [
                {
                    "name": "tab-bookmark-manager",
                    "description": "Browser extension for bookmark management",
                    "tier": "standard",
                    "github_url": "https://github.com/labores-profani-crux/tab-bookmark-manager",
                    "implementation_status": "PRODUCTION",
                }
            ]
        },
    }
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(reg))
    return path


@pytest.fixture
def config(tmp_path):
    """Dry-run config with all platforms empty (no live calls)."""
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
def pipeline(registry, config, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=registry,
        social_config_path=config,
        analytics_store_path=tmp_path / "analytics.json",
    )


class TestEndToEndDryRun:
    """Full pipeline run in dry-run mode — no real API calls."""

    def test_essay_published_full_pipeline(self, pipeline):
        """Run the complete flow for essay-published event."""
        result = pipeline.run_full_pipeline(
            event_type="essay-published",
            repo_name="public-process",
            channels=["mastodon", "discord"],
        )
        # Pipeline completes (dispatch may skip due to no clients)
        assert result["status"] in ("complete", "no_channels_passed")
        assert "dispatched" in result

    def test_template_selection_resolves(self, pipeline):
        """Template selection returns a valid template ID."""
        tid = pipeline.select_template("essay-published")
        assert tid == "essay-announce"

    def test_render_produces_channel_texts(self, pipeline):
        """Rendering produces output for at least one channel."""
        results = pipeline.render_and_check(
            "essay-announce", "public-process", ["mastodon", "discord"],
        )
        assert len(results) > 0
        for channel, text in results.items():
            assert isinstance(text, str)
            assert len(text) > 0

    def test_quality_checks_pass(self, pipeline):
        """Quality checker passes for well-formed templates."""
        results = pipeline.render_and_check(
            "essay-announce", "public-process", ["mastodon"],
        )
        # If mastodon passed, quality checks succeeded
        assert "mastodon" in results

    def test_dispatch_returns_records(self, pipeline):
        """Dispatch produces syndication records (skipped without clients)."""
        channel_texts = {"mastodon": "Test essay announcement https://example.com"}
        records = pipeline.dispatch(channel_texts)
        assert isinstance(records, list)
        assert len(records) >= 1
        for rec in records:
            assert "channel" in rec
            assert "status" in rec

    def test_analytics_recorded(self, pipeline, tmp_path):
        """Analytics recording persists metrics to store."""
        records = [
            {"channel": "mastodon", "status": "published", "url": "https://example.com", "error": ""},
        ]
        pipeline.record_analytics(records)
        # Analytics store should have data
        store_path = tmp_path / "analytics.json"
        assert store_path.exists()

    def test_full_flow_multiple_events(self, pipeline):
        """Multiple event types each resolve and render without error."""
        test_events = [
            ("essay-published", "public-process"),
            ("feature-released", "tab-bookmark-manager"),
            ("repo-launched", "tab-bookmark-manager"),
        ]
        for event_type, repo in test_events:
            result = pipeline.run_full_pipeline(
                event_type=event_type,
                repo_name=repo,
                channels=["mastodon", "discord"],
            )
            assert result["status"] in ("complete", "no_channels_passed"), (
                f"Failed for {event_type}/{repo}: {result}"
            )

    def test_status_report_after_dispatches(self, pipeline):
        """Status report reflects pipeline state after activity."""
        status = pipeline.status()
        assert status["templates_loaded"] > 0
        assert status["event_map_entries"] == len(EVENT_TEMPLATE_MAP)
        assert "social_config" in status

    def test_report_generation(self, pipeline):
        """Report generates valid markdown."""
        report = pipeline.generate_report(period_days=7)
        assert "# Kerygma Distribution Report" in report
        assert "Channel Summary" in report
        assert "Available Templates" in report
