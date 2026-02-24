"""Tests for backfill_from_posts and Jekyll frontmatter parsing."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from kerygma_pipeline import KerygmaPipeline
from kerygma_social.delivery_log import DeliveryRecord


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
        schedule_store_path=tmp_path / "schedule.json",
    )


@pytest.fixture
def posts_dir(tmp_path):
    """Create a fake Jekyll _posts directory with sample posts."""
    d = tmp_path / "_posts"
    d.mkdir()

    (d / "2026-01-15-first-essay.md").write_text(
        "---\ntitle: First Essay\nslug: first-essay\ndate: 2026-01-15\n---\n\nContent here.\n"
    )
    (d / "2026-01-20-second-essay.md").write_text(
        "---\ntitle: Second Essay\nslug: second-essay\ndate: 2026-01-20\n---\n\nMore content.\n"
    )
    (d / "2026-02-01-third-essay.md").write_text(
        "---\ntitle: Third Essay\ndate: 2026-02-01\n---\n\nNo slug field.\n"
    )
    return d


class TestBackfillDryRun:
    def test_dry_run_lists_posts(self, pipeline, posts_dir):
        result = pipeline.backfill_from_posts(posts_dir)
        assert result["total_files"] == 3
        assert result["scheduled"] == 3
        assert result["execute"] is False

    def test_dry_run_reports_would_schedule(self, pipeline, posts_dir):
        result = pipeline.backfill_from_posts(posts_dir)
        actions = [p["action"] for p in result["posts"]]
        assert all(a == "would_schedule" for a in actions)

    def test_stagger_spacing(self, pipeline, posts_dir):
        result = pipeline.backfill_from_posts(posts_dir, stagger_minutes=60)
        times = [p["scheduled_time"] for p in result["posts"]]
        t0 = datetime.fromisoformat(times[0])
        t1 = datetime.fromisoformat(times[1])
        t2 = datetime.fromisoformat(times[2])
        assert (t1 - t0).total_seconds() == 3600
        assert (t2 - t1).total_seconds() == 3600


class TestBackfillEdgeCases:
    def test_empty_dir(self, pipeline, tmp_path):
        empty = tmp_path / "empty_posts"
        empty.mkdir()
        result = pipeline.backfill_from_posts(empty)
        assert result["total_files"] == 0
        assert result["scheduled"] == 0

    def test_nonexistent_dir(self, pipeline, tmp_path):
        result = pipeline.backfill_from_posts(tmp_path / "does-not-exist")
        assert "error" in result

    def test_skips_no_frontmatter(self, pipeline, tmp_path):
        d = tmp_path / "no_fm"
        d.mkdir()
        (d / "2026-01-01-no-fm.md").write_text("Just plain text, no frontmatter.\n")
        result = pipeline.backfill_from_posts(d)
        assert result["skipped"] == 1
        assert result["scheduled"] == 0


class TestBackfillExecute:
    def test_execute_creates_entries(self, pipeline, posts_dir):
        result = pipeline.backfill_from_posts(posts_dir, execute=True)
        assert result["execute"] is True
        assert result["scheduled"] == 3
        actions = [p["action"] for p in result["posts"]]
        assert all(a == "scheduled" for a in actions)
        # Scheduler should have entries
        assert pipeline._scheduler.total_entries >= 3

    def test_skips_already_delivered(self, pipeline, posts_dir):
        """Posts already in delivery log are skipped."""
        # Manually mark first-essay as delivered
        pipeline._delivery_log.append(DeliveryRecord(
            record_id="backfill-test-1",
            post_id="first-essay",
            platform="mastodon",
            status="success",
        ))
        result = pipeline.backfill_from_posts(posts_dir, execute=True)
        skipped_posts = [p for p in result["posts"] if p["action"] == "skip_already_delivered"]
        assert len(skipped_posts) == 1
        assert skipped_posts[0]["title"] == "First Essay"


class TestJekyllFrontmatterParser:
    def test_valid_frontmatter(self, tmp_path):
        f = tmp_path / "valid.md"
        f.write_text("---\ntitle: Hello World\nslug: hello\n---\n\nBody text.\n")
        result = KerygmaPipeline._parse_jekyll_frontmatter(f)
        assert result == {"title": "Hello World", "slug": "hello"}

    def test_no_frontmatter(self, tmp_path):
        f = tmp_path / "no_fm.md"
        f.write_text("Just text, no dashes.\n")
        result = KerygmaPipeline._parse_jekyll_frontmatter(f)
        assert result is None

    def test_no_closing_dashes(self, tmp_path):
        f = tmp_path / "broken.md"
        f.write_text("---\ntitle: Broken\n")
        result = KerygmaPipeline._parse_jekyll_frontmatter(f)
        assert result is None

    def test_empty_frontmatter(self, tmp_path):
        f = tmp_path / "empty_fm.md"
        f.write_text("---\n---\nBody.\n")
        result = KerygmaPipeline._parse_jekyll_frontmatter(f)
        # yaml.safe_load of empty string returns None
        assert result is None
