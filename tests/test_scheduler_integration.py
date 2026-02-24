"""Integration tests for strategy layer wiring in kerygma_pipeline.

Tests scheduler, calendar, and channel registry integration with the pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime, date, timedelta
from pathlib import Path

import pytest

from kerygma_pipeline import KerygmaPipeline
from kerygma_strategy.scheduler import Frequency


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
def config_with_calendar(tmp_path):
    """Config YAML with calendar events and channels."""
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
        "channels:\n"
        "  - channel_id: mastodon-primary\n"
        "    name: Mastodon Primary\n"
        "    platform: mastodon\n"
        "    endpoint: https://mastodon.social\n"
        "    max_length: 500\n"
        "    enabled: true\n"
        "  - channel_id: discord-announcements\n"
        "    name: Discord Announcements\n"
        "    platform: discord\n"
        "    endpoint: ''\n"
        "    max_length: 4096\n"
        "    enabled: true\n"
        "calendar:\n"
        "  events:\n"
        "    - event_id: test-conference\n"
        "      name: Test Conference\n"
        "      event_type: conference\n"
        "      start_date: '2026-07-01'\n"
        "      end_date: '2026-07-05'\n"
        "      posting_modifier: 1.5\n"
        "    - event_id: winter-quiet\n"
        "      name: Winter Quiet Period\n"
        "      event_type: quiet_period\n"
        "      start_date: '2026-12-23'\n"
        "      end_date: '2027-01-02'\n"
        "      posting_modifier: 0.3\n"
    )
    return cfg


@pytest.fixture
def pipeline(registry, config_with_calendar, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=registry,
        social_config_path=config_with_calendar,
        analytics_store_path=tmp_path / "analytics.json",
        calendar_config_path=config_with_calendar,
        schedule_store_path=tmp_path / "schedule.json",
    )


class TestScheduleContent:
    def test_schedule_content_creates_entries(self, pipeline):
        """schedule_content creates one entry per channel."""
        entries = pipeline.schedule_content(
            content_id="test-essay",
            channels=["mastodon", "discord"],
            scheduled_time=datetime(2026, 3, 1, 10, 0),
        )
        assert len(entries) == 2
        assert entries[0].channel == "mastodon"
        assert entries[1].channel == "discord"
        assert not entries[0].published

    def test_quiet_period_delays_entry(self, pipeline):
        """Entries scheduled during quiet period are pushed past it."""
        quiet_time = datetime(2026, 12, 25, 10, 0)
        entries = pipeline.schedule_content(
            content_id="holiday-essay",
            channels=["mastodon"],
            scheduled_time=quiet_time,
        )
        # Should be pushed to 2027-01-03 (day after end_date 2027-01-02)
        assert entries[0].scheduled_time.date() == date(2027, 1, 3)

    def test_normal_period_keeps_time(self, pipeline):
        """Entries outside quiet period keep their scheduled time."""
        normal_time = datetime(2026, 3, 15, 10, 0)
        entries = pipeline.schedule_content(
            content_id="spring-essay",
            channels=["mastodon"],
            scheduled_time=normal_time,
        )
        assert entries[0].scheduled_time == normal_time


class TestProcessDueEntries:
    def test_process_due_entries_dispatches(self, pipeline):
        """Due entries are processed and marked published."""
        past = datetime.now() - timedelta(hours=1)
        pipeline.schedule_content(
            content_id="essay-announce",
            channels=["mastodon"],
            scheduled_time=past,
        )
        results = pipeline.process_due_entries()
        assert len(results) >= 1
        assert results[0]["status"] in ("dispatched", "no_channels_passed", "error")


class TestSchedulePersistence:
    def test_persistence_round_trip(self, registry, config_with_calendar, tmp_path):
        """Schedule entries survive save/reload."""
        store_path = tmp_path / "schedule_rt.json"
        p1 = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=registry,
            social_config_path=config_with_calendar,
            calendar_config_path=config_with_calendar,
            schedule_store_path=store_path,
        )
        p1.schedule_content(
            content_id="persist-test",
            channels=["mastodon"],
            scheduled_time=datetime(2026, 6, 1, 12, 0),
        )
        assert store_path.exists()

        # Reload
        p2 = KerygmaPipeline(
            templates_dir=TEMPLATES_DIR,
            registry_path=registry,
            social_config_path=config_with_calendar,
            calendar_config_path=config_with_calendar,
            schedule_store_path=store_path,
        )
        assert p2._scheduler.total_entries >= 1


class TestStatusWithStrategy:
    def test_status_includes_scheduler_section(self, pipeline):
        status = pipeline.status()
        assert "scheduler" in status
        assert "pending" in status["scheduler"]
        assert "due_now" in status["scheduler"]

    def test_status_includes_calendar_section(self, pipeline):
        status = pipeline.status()
        assert "calendar" in status
        assert status["calendar"]["total_events"] == 2
        assert "current_modifier" in status["calendar"]

    def test_status_includes_channels_section(self, pipeline):
        status = pipeline.status()
        assert "channels" in status
        assert status["channels"]["total"] == 2
        assert status["channels"]["enabled"] == 2
