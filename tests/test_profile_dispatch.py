"""Tests for profile-aware dispatch in the pipeline."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from kerygma_pipeline import KerygmaPipeline


TEMPLATES_DIR = Path(__file__).parent.parent.parent / "announcement-templates" / "templates"


@pytest.fixture
def sample_registry(tmp_path):
    reg = {
        "organs": {
            "ORGAN-V": {
                "repos": [{
                    "name": "public-process",
                    "description": "Public accountability ledger",
                    "tier": "flagship",
                    "github_url": "https://github.com/organvm-v-logos/public-process",
                    "implementation_status": "PRODUCTION",
                }]
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
    )
    return cfg


@pytest.fixture
def profiles_dir(tmp_path):
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "_default.yaml").write_text(
        "profile_id: _default\n"
        "display_name: System Default\n"
        "organ: null\n"
        "repos: []\n"
        "voice:\n"
        "  tone: institutional\n"
        "  hashtags: ['#organvm']\n"
        "  tagline: Eight-organ system\n"
        "platforms:\n"
        "  mastodon:\n"
        "    instance_url: https://mastodon.social\n"
        "    access_token: test-token\n"
        "    visibility: public\n"
        "channels:\n"
        "  - channel_id: mastodon-primary\n"
        "    platform: mastodon\n"
        "    max_length: 500\n"
        "    enabled: true\n"
    )
    return profiles


@pytest.fixture
def pipeline_with_profiles(sample_registry, social_config, profiles_dir, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=sample_registry,
        social_config_path=social_config,
        analytics_store_path=tmp_path / "analytics.json",
        profiles_dir=profiles_dir,
    )


@pytest.fixture
def pipeline_without_profiles(sample_registry, social_config, tmp_path):
    return KerygmaPipeline(
        templates_dir=TEMPLATES_DIR,
        registry_path=sample_registry,
        social_config_path=social_config,
        analytics_store_path=tmp_path / "analytics.json",
    )


class TestProfileResolution:
    def test_pipeline_loads_profiles(self, pipeline_with_profiles):
        assert pipeline_with_profiles._profile_registry is not None
        assert pipeline_with_profiles._profile_registry.total_profiles >= 1

    def test_pipeline_without_profiles_still_works(self, pipeline_without_profiles):
        """Pipeline without profiles_dir should work exactly as before."""
        result = pipeline_without_profiles.run_full_pipeline(
            event_type="essay-published",
            repo_name="public-process",
            channels=["mastodon"],
        )
        assert result["status"] in ("complete", "no_channels_passed")

    def test_status_includes_profiles_section(self, pipeline_with_profiles):
        status = pipeline_with_profiles.status()
        assert "profiles" in status
        assert status["profiles"]["loaded"] >= 1
        assert "_default" in status["profiles"]["ids"]


class TestProfileDispatch:
    def test_dispatch_with_profile(self, pipeline_with_profiles):
        """Dispatch with profile should not crash."""
        channel_texts = {"mastodon": "Test post https://example.com"}
        records = pipeline_with_profiles.dispatch(
            channel_texts, repo_name="public-process",
        )
        assert len(records) >= 1

    def test_dispatch_with_explicit_profile_id(self, pipeline_with_profiles):
        channel_texts = {"mastodon": "Test post https://example.com"}
        records = pipeline_with_profiles.dispatch(
            channel_texts, profile_id="_default",
        )
        assert len(records) >= 1

    def test_full_pipeline_with_profiles(self, pipeline_with_profiles):
        result = pipeline_with_profiles.run_full_pipeline(
            event_type="essay-published",
            repo_name="public-process",
            channels=["mastodon"],
        )
        assert result["status"] in ("complete", "no_channels_passed")


class TestBackwardCompatibility:
    def test_dispatch_without_repo_name(self, pipeline_with_profiles):
        """dispatch() without repo_name should fall back to global config."""
        channel_texts = {"mastodon": "Test post https://example.com"}
        records = pipeline_with_profiles.dispatch(channel_texts)
        assert len(records) >= 1

    def test_render_works_without_profiles(self, pipeline_without_profiles):
        results = pipeline_without_profiles.render_and_check(
            "essay-announce", "public-process", ["mastodon"],
        )
        assert len(results) >= 0  # may fail quality check, but should not crash


class TestProcessDueWithProfiles:
    def test_due_entries_thread_repo_name_to_dispatch(
        self, pipeline_with_profiles,
    ):
        """process_due_entries should pass content_id as repo_name to dispatch."""
        from datetime import datetime, timedelta

        pipeline_with_profiles.schedule_content(
            content_id="public-process",
            channels=["mastodon"],
            scheduled_time=datetime.now() - timedelta(hours=1),
        )

        with patch.object(
            pipeline_with_profiles, "render_and_check",
            return_value={"mastodon": "Test post https://example.com"},
        ), patch.object(
            pipeline_with_profiles, "dispatch",
            return_value=[{"channel": "mastodon", "status": "published", "url": "", "error": ""}],
        ) as mock_dispatch:
            results = pipeline_with_profiles.process_due_entries()

        assert len(results) >= 1
        assert results[0]["status"] == "dispatched"
        mock_dispatch.assert_called_once_with(
            {"mastodon": "Test post https://example.com"},
            repo_name="public-process",
        )
