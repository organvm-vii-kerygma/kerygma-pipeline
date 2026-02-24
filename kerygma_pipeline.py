"""Kerygma Pipeline — full orchestrator for ORGAN VII distribution.

Ties together announcement-templates, social-automation, and distribution-strategy
into a single end-to-end pipeline:
  poll → select template → render → quality check → dispatch → record analytics

Usage:
    python kerygma_pipeline.py dispatch --template <id> --repo <name> [--channels mastodon,discord]
    python kerygma_pipeline.py poll
    python kerygma_pipeline.py --help
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# --- Imports from the three ORGAN-VII packages ---

from kerygma_templates.engine import TemplateEngine
from kerygma_templates.quality_checker import QualityChecker
from kerygma_templates.registry_loader import RegistryLoader, EventContext

from kerygma_social.config import load_config as load_social_config
from kerygma_social.factory import build_distributor
from kerygma_social.posse import PosseDistributor, Platform
from kerygma_social.delivery_log import DeliveryLog
from kerygma_social.rss_poller import RssPoller

from kerygma_strategy.analytics import AnalyticsCollector, EngagementMetric
from kerygma_strategy.persistence import JsonStore
from kerygma_strategy.scheduler import ContentScheduler, ScheduleEntry, Frequency, PrioritizedEntry
from kerygma_strategy.calendar import DistributionCalendar, CalendarEvent
from kerygma_strategy.channels import ChannelRegistry, ChannelConfig
from kerygma_strategy.report_generator import ReportGenerator, ReportPeriod, ReportData

logger = logging.getLogger("kerygma.pipeline")


# --- Default paths ---


def _find_templates_dir() -> Path:
    """Resolve templates directory relative to superproject root.

    Walks up from __file__ looking for announcement-templates/templates/.
    Handles running from kerygma-pipeline/ subdir or superproject root.
    """
    here = Path(__file__).resolve().parent
    for ancestor in (here, here.parent, here.parent.parent):
        candidate = ancestor / "announcement-templates" / "templates"
        if candidate.is_dir():
            return candidate
    # Fallback: assume superproject layout (kerygma-pipeline/ is sibling)
    return here.parent / "announcement-templates" / "templates"


TEMPLATES_DIR = _find_templates_dir()
REGISTRY_PATH = Path.home() / "Workspace/meta-organvm/organvm-corpvs-testamentvm/registry-v2.json"


# --- Template-to-event mapping ---

EVENT_TEMPLATE_MAP: dict[str, str] = {
    # ORGAN-I: Theoria
    "research-published": "essay-announce",
    "framework-updated": "feature-release",
    # ORGAN-II: Poiesis
    "artwork-released": "repo-launch",
    "performance-scheduled": "salon-invite",
    # ORGAN-III: Ergon
    "feature-released": "feature-release",
    "bugfix-released": "bugfix-release",
    "repo-launched": "repo-launch",
    # ORGAN-IV: Taxis
    "milestone-reached": "system-milestone",
    "audit-completed": "system-milestone",
    # ORGAN-V: Logos
    "essay-published": "essay-announce",
    # ORGAN-VI: Koinonia
    "salon-scheduled": "salon-invite",
    "workshop-scheduled": "workshop-sprint",
    "reading-group-scheduled": "reading-group",
    "community-milestone": "community-milestone",
    "partnership-announced": "partnership",
    # ORGAN-VII: Kerygma
    "press-release": "press-release",
    "grant-update": "grant-supplement",
    # System-level
    "organ-launched": "organ-launch",
    "breaking-change": "breaking-change",
}


class KerygmaPipeline:
    """End-to-end distribution pipeline for ORGAN VII."""

    def __init__(
        self,
        templates_dir: Path = TEMPLATES_DIR,
        registry_path: Path | None = None,
        social_config_path: Path | None = None,
        analytics_store_path: Path | None = None,
        calendar_config_path: Path | None = None,
        schedule_store_path: Path | None = None,
    ) -> None:
        # Template engine
        self._engine = TemplateEngine()
        if templates_dir.is_dir():
            self._engine.load_directory(templates_dir)
        self._checker = QualityChecker()

        # Registry
        reg_path = registry_path or REGISTRY_PATH
        self._registry = RegistryLoader(reg_path if reg_path.exists() else None)

        # Social config
        self._social_config = load_social_config(social_config_path)

        # Analytics
        self._analytics_store = JsonStore(analytics_store_path) if analytics_store_path else None
        self._analytics = AnalyticsCollector(store=self._analytics_store)

        # Delivery log
        log_path = Path(self._social_config.delivery_log_path) if self._social_config.delivery_log_path else None
        self._delivery_log = DeliveryLog(log_path)

        # Strategy layer: calendar, channels, scheduler, report generator
        self._calendar = self._load_calendar(calendar_config_path or social_config_path)
        self._channel_registry = self._load_channel_registry(social_config_path)
        self._scheduler = ContentScheduler(calendar=self._calendar)
        self._report_generator = ReportGenerator(self._analytics)

        # Schedule persistence
        self._schedule_store_path = schedule_store_path
        self._schedule_store = JsonStore(schedule_store_path) if schedule_store_path else None
        if self._schedule_store:
            self._load_schedule_from_store()

    def _load_calendar(self, config_path: Path | None) -> DistributionCalendar:
        """Load calendar from YAML config, falling back to empty."""
        if config_path and config_path.exists():
            try:
                return DistributionCalendar.from_yaml(config_path)
            except Exception as exc:
                logger.warning("Failed to load calendar from %s: %s", config_path, exc)
        return DistributionCalendar()

    def _load_channel_registry(self, config_path: Path | None) -> ChannelRegistry:
        """Load channel registry from YAML config, falling back to empty."""
        if config_path and config_path.exists():
            try:
                return ChannelRegistry.from_yaml(config_path)
            except Exception as exc:
                logger.warning("Failed to load channel registry from %s: %s", config_path, exc)
        return ChannelRegistry()

    def _load_schedule_from_store(self) -> None:
        """Restore schedule entries from persistent store."""
        if not self._schedule_store:
            return
        entries = self._schedule_store.get("schedule_entries", [])
        for item in entries:
            try:
                entry = ScheduleEntry(
                    entry_id=item["entry_id"],
                    content_id=item["content_id"],
                    channel=item["channel"],
                    scheduled_time=datetime.fromisoformat(item["scheduled_time"]),
                    frequency=Frequency(item.get("frequency", "once")),
                    published=item.get("published", False),
                )
                self._scheduler.schedule(entry)
            except (KeyError, ValueError) as exc:
                logger.warning("Skipping invalid schedule entry: %s", exc)

    def _save_schedule_to_store(self) -> None:
        """Persist current schedule entries to store."""
        if not self._schedule_store:
            return
        entries = []
        for entry in self._scheduler.get_due() + self._scheduler.get_upcoming(hours=8760):
            entries.append({
                "entry_id": entry.entry_id,
                "content_id": entry.content_id,
                "channel": entry.channel,
                "scheduled_time": entry.scheduled_time.isoformat(),
                "frequency": entry.frequency.value,
                "published": entry.published,
            })
        self._schedule_store.set("schedule_entries", entries)
        self._schedule_store.save()

    def _build_distributor(self) -> PosseDistributor:
        return build_distributor(self._social_config, delivery_log=self._delivery_log)

    def select_template(self, event_type: str) -> str:
        """Map an event type to a template ID."""
        template_id = EVENT_TEMPLATE_MAP.get(event_type)
        if not template_id:
            raise ValueError(f"No template mapped for event type: {event_type}")
        if not self._engine.get_template(template_id):
            raise ValueError(f"Template '{template_id}' not found in engine")
        return template_id

    def render_and_check(
        self,
        template_id: str,
        repo_name: str,
        channels: list[str],
        event: EventContext | None = None,
    ) -> dict[str, str]:
        """Render a template for all channels and run quality checks.

        Returns: {channel: rendered_text} for channels that pass checks.
        """
        if event is None:
            event = EventContext(
                event_type=template_id,
                repo_name=repo_name,
            )

        context = self._registry.build_context(event, repo_name)
        results: dict[str, str] = {}

        for channel in channels:
            render = self._engine.render(template_id, context, channel)
            report = self._checker.check(
                render.text, channel, template_id, render.unresolved_vars,
            )
            if report.passed:
                results[channel] = render.text
            else:
                errors = "; ".join(c.message for c in report.errors)
                logger.warning("Quality check failed for %s/%s: %s", template_id, channel, errors)

        return results

    def dispatch(self, channel_texts: dict[str, str]) -> list[dict[str, Any]]:
        """Dispatch rendered texts to platforms via POSSE."""
        distributor = self._build_distributor()
        records = []

        for channel, text in channel_texts.items():
            platform = Platform(channel)
            post = distributor.create_post(
                post_id=f"pipeline-{channel}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}",
                title=text[:100],
                body=text,
                canonical_url="",
                platforms=[platform],
            )
            syndication = distributor.syndicate(post.post_id)
            for rec in syndication:
                records.append({
                    "channel": channel,
                    "status": rec.status.value,
                    "url": rec.external_url or "",
                    "error": rec.error or "",
                })
        return records

    def record_analytics(self, records: list[dict[str, Any]]) -> None:
        """Record dispatch results as analytics metrics."""
        for rec in records:
            self._analytics.record(EngagementMetric(
                channel_id=rec["channel"],
                content_id=f"dispatch-{datetime.now().strftime('%Y%m%d')}",
                timestamp=datetime.now(),
                impressions=1 if rec["status"] == "published" else 0,
            ))
        self._analytics.flush()

    def status(self) -> dict[str, Any]:
        """Return a health report: loaded templates, analytics summary, config state."""
        templates = self._engine.list_templates()
        analytics_summary = {}
        if self._analytics_store:
            for channel in ("mastodon", "discord", "bluesky", "ghost"):
                records = self._analytics.get_by_channel(channel)
                analytics_summary[channel] = {
                    "total_records": len(records),
                    "total_impressions": sum(r.impressions for r in records),
                }

        # Calendar state
        active_events = self._calendar.get_active_events()
        upcoming_events = self._calendar.get_upcoming(days=30)
        current_modifier = self._calendar.get_posting_modifier()

        return {
            "templates_loaded": len(templates),
            "template_ids": [t.template_id for t in templates],
            "event_map_entries": len(EVENT_TEMPLATE_MAP),
            "analytics": analytics_summary,
            "delivery_log_entries": self._delivery_log.total_records,
            "social_config": {
                "mastodon": bool(self._social_config.mastodon_instance_url),
                "discord": bool(self._social_config.discord_webhook_url),
                "bluesky": bool(self._social_config.bluesky_handle),
                "ghost": bool(self._social_config.ghost_api_url),
                "live_mode": self._social_config.live_mode,
            },
            "scheduler": {
                "total_entries": self._scheduler.total_entries,
                "pending": self._scheduler.pending_count,
                "due_now": len(self._scheduler.get_due()),
            },
            "calendar": {
                "total_events": self._calendar.total_events,
                "active_events": [e.name for e in active_events],
                "current_modifier": current_modifier,
                "upcoming_30d": [
                    {"name": e.name, "start": e.start_date.isoformat(), "modifier": e.posting_modifier}
                    for e in upcoming_events
                ],
            },
            "channels": {
                "total": self._channel_registry.total_channels,
                "enabled": len(self._channel_registry.get_enabled()),
            },
        }

    def preview(self, template_id: str, repo_name: str, channel: str) -> str:
        """Render a single template+channel for preview without dispatching."""
        event = EventContext(event_type=template_id, repo_name=repo_name)
        context = self._registry.build_context(event, repo_name)
        render = self._engine.render(template_id, context, channel)
        return render.text

    def generate_report(self, period_days: int = 7) -> str:
        """Generate a Markdown report using ReportGenerator + supplemental sections."""
        now = datetime.now()
        if period_days <= 7:
            period = ReportPeriod.weekly(end=now)
        else:
            period = ReportPeriod.monthly(end=now)

        report_data = self._report_generator.generate(period)
        lines = [self._report_generator.to_markdown(report_data)]

        # Platform metrics
        platform_metrics = self._collect_platform_metrics()
        if platform_metrics:
            lines.append("")
            lines.append("## Platform Metrics")
            lines.append("")
            lines.append("| Platform | Metric | Value |")
            lines.append("|----------|--------|-------|")
            for platform, metrics in platform_metrics.items():
                for metric_name, value in metrics.items():
                    lines.append(f"| {platform} | {metric_name} | {value} |")

        # Schedule summary
        lines.append("")
        lines.append("## Schedule Summary")
        lines.append("")
        lines.append(f"- Pending entries: {self._scheduler.pending_count}")
        lines.append(f"- Due now: {len(self._scheduler.get_due())}")
        upcoming = self._scheduler.get_upcoming(hours=168)  # 7 days
        lines.append(f"- Upcoming (7 days): {len(upcoming)}")

        # Calendar events
        upcoming_events = self._calendar.get_upcoming(days=30)
        if upcoming_events:
            lines.append("")
            lines.append("## Upcoming Calendar Events (30 days)")
            lines.append("")
            for ev in upcoming_events:
                end_str = f" to {ev.end_date.isoformat()}" if ev.end_date else ""
                lines.append(f"- **{ev.name}** ({ev.start_date.isoformat()}{end_str}) — modifier: {ev.posting_modifier}")

        lines.append("")
        lines.append("---")
        lines.append("*Generated by kerygma_pipeline.py report*")
        return "\n".join(lines)

    def _collect_platform_metrics(self) -> dict[str, dict[str, Any]]:
        """Pull site-level metrics from configured platforms."""
        metrics: dict[str, dict[str, Any]] = {}

        if self._social_config.ghost_api_url and self._social_config.ghost_admin_api_key:
            try:
                from kerygma_strategy.ghost_metrics import GhostMetricsClient, GhostMetricsConfig
                client = GhostMetricsClient(
                    GhostMetricsConfig(
                        api_url=self._social_config.ghost_api_url,
                        admin_api_key=self._social_config.ghost_admin_api_key,
                    ),
                    live=self._social_config.live_mode,
                )
                metrics["ghost"] = client.get_site_metrics()
            except Exception as exc:
                logger.warning("Failed to collect Ghost metrics: %s", exc)

        if self._social_config.mastodon_instance_url and self._social_config.mastodon_access_token:
            try:
                from kerygma_strategy.mastodon_metrics import MastodonMetricsClient, MastodonMetricsConfig
                client = MastodonMetricsClient(
                    MastodonMetricsConfig(
                        instance_url=self._social_config.mastodon_instance_url,
                        access_token=self._social_config.mastodon_access_token,
                    ),
                    live=self._social_config.live_mode,
                )
                metrics["mastodon"] = client.get_account_stats()
            except Exception as exc:
                logger.warning("Failed to collect Mastodon metrics: %s", exc)

        return metrics

    def activate(self) -> dict[str, Any]:
        """Run comprehensive pre-flight checks for pipeline readiness."""
        checks: dict[str, Any] = {}

        # 1. Templates loaded
        templates = self._engine.list_templates()
        checks["templates_loaded"] = len(templates)
        checks["templates_ok"] = len(templates) > 0

        # 2. EVENT_TEMPLATE_MAP entries all resolve
        unresolved = []
        for event_type, template_id in EVENT_TEMPLATE_MAP.items():
            if not self._engine.get_template(template_id):
                unresolved.append(f"{event_type} -> {template_id}")
        checks["event_map_total"] = len(EVENT_TEMPLATE_MAP)
        checks["event_map_unresolved"] = unresolved
        checks["event_map_ok"] = len(unresolved) == 0

        # 3. Registry accessible
        try:
            reg_path = REGISTRY_PATH
            checks["registry_exists"] = reg_path.exists()
        except Exception:
            checks["registry_exists"] = False

        # 4. Social config with at least 1 platform
        platforms_configured = sum([
            bool(self._social_config.mastodon_instance_url),
            bool(self._social_config.discord_webhook_url),
            bool(self._social_config.bluesky_handle),
            bool(self._social_config.ghost_api_url),
        ])
        checks["platforms_configured"] = platforms_configured
        checks["social_config_ok"] = platforms_configured > 0

        # 5. Sample render quality check
        sample_ok = False
        if templates:
            try:
                sample = templates[0]
                channels = sample.channels[:1]
                if channels:
                    results = self.render_and_check(
                        sample.template_id, "sample-repo", channels,
                    )
                    sample_ok = len(results) > 0
            except Exception as exc:
                logger.warning("Sample render failed: %s", exc)
        checks["sample_render_ok"] = sample_ok

        # 6. Calendar loaded
        checks["calendar_events"] = self._calendar.total_events
        checks["calendar_ok"] = self._calendar.total_events > 0

        # 7. Channel registry has enabled channels
        enabled_channels = self._channel_registry.get_enabled()
        checks["channels_registered"] = self._channel_registry.total_channels
        checks["channels_enabled"] = len(enabled_channels)
        checks["channels_ok"] = len(enabled_channels) > 0

        # Overall go/no-go
        checks["ready"] = all([
            checks["templates_ok"],
            checks["event_map_ok"],
            checks["sample_render_ok"],
            checks["calendar_ok"],
            checks["channels_ok"],
        ])

        return checks

    def poll_for_events(self) -> list[dict[str, str]]:
        """Poll RSS feed for new content entries."""
        if not self._social_config.rss_feed_url:
            return []
        seen_path = None
        if self._social_config.delivery_log_path:
            seen_path = Path(self._social_config.delivery_log_path).with_name("rss_seen.json")
        poller = RssPoller(feed_url=self._social_config.rss_feed_url, seen_path=seen_path)
        entries = poller.poll()
        return [
            {"title": e.title, "url": e.url, "id": e.entry_id}
            for e in entries
        ]

    def schedule_content(
        self,
        content_id: str,
        channels: list[str],
        scheduled_time: datetime | None = None,
        frequency: Frequency = Frequency.ONCE,
    ) -> list[ScheduleEntry]:
        """Create schedule entries for content across channels.

        Uses calendar-aware scheduling to delay posts during quiet periods.
        """
        at = scheduled_time or datetime.now()
        created: list[ScheduleEntry] = []

        for channel in channels:
            entry_id = f"{content_id}-{channel}-{at.strftime('%Y%m%d%H%M%S')}"
            entry = ScheduleEntry(
                entry_id=entry_id,
                content_id=content_id,
                channel=channel,
                scheduled_time=at,
                frequency=frequency,
            )
            scheduled = self._scheduler.schedule_with_calendar(entry)
            created.append(scheduled)

        self._save_schedule_to_store()
        return created

    def process_due_entries(self) -> list[dict[str, Any]]:
        """Process all due schedule entries: render, check, dispatch, record.

        Returns a list of result dicts per processed entry.
        """
        prioritized = self._scheduler.get_due_with_priority()
        results: list[dict[str, Any]] = []

        for p_entry in prioritized:
            entry = p_entry.entry
            try:
                # Attempt to select template via event map, fall back to content_id as template
                template_id = EVENT_TEMPLATE_MAP.get(entry.content_id, entry.content_id)
                channel_texts = self.render_and_check(
                    template_id, entry.content_id, [entry.channel],
                )

                if channel_texts:
                    records = self.dispatch(channel_texts)
                    self.record_analytics(records)
                    self._scheduler.publish_entry(entry.entry_id)
                    results.append({
                        "entry_id": entry.entry_id,
                        "status": "dispatched",
                        "priority": p_entry.priority,
                        "records": records,
                    })
                else:
                    results.append({
                        "entry_id": entry.entry_id,
                        "status": "no_channels_passed",
                        "priority": p_entry.priority,
                    })
            except Exception as exc:
                logger.warning("Failed to process entry %s: %s", entry.entry_id, exc)
                results.append({
                    "entry_id": entry.entry_id,
                    "status": "error",
                    "error": str(exc),
                })

        self._save_schedule_to_store()
        return results

    def backfill_from_posts(
        self,
        posts_dir: Path,
        channels: list[str] | None = None,
        stagger_minutes: int = 30,
        execute: bool = False,
    ) -> dict[str, Any]:
        """Scan a Jekyll _posts/ directory and schedule undistributed posts.

        In dry-run mode (execute=False): reports what would be scheduled.
        In execute mode: creates staggered ScheduleEntry per post.
        """
        if channels is None:
            channels = ["mastodon", "discord"]

        if not posts_dir.is_dir():
            return {"error": f"Posts directory not found: {posts_dir}", "posts": []}

        post_files = sorted(posts_dir.glob("*.md"))
        if not post_files:
            post_files = sorted(posts_dir.glob("*.markdown"))

        report: list[dict[str, Any]] = []
        scheduled_count = 0
        skipped_count = 0
        base_time = datetime.now()

        for i, post_file in enumerate(post_files):
            frontmatter = self._parse_jekyll_frontmatter(post_file)
            if not frontmatter:
                skipped_count += 1
                continue

            content_id = frontmatter.get("slug") or post_file.stem
            title = frontmatter.get("title", content_id)

            # Check delivery log for dedup (check all target channels)
            already_delivered = any(
                self._delivery_log.has_been_delivered(content_id, ch)
                for ch in channels
            )
            if already_delivered:
                skipped_count += 1
                report.append({
                    "file": post_file.name,
                    "title": title,
                    "action": "skip_already_delivered",
                })
                continue

            stagger_time = base_time + timedelta(minutes=stagger_minutes * scheduled_count)

            if execute:
                entries = self.schedule_content(
                    content_id=content_id,
                    channels=channels,
                    scheduled_time=stagger_time,
                )
                report.append({
                    "file": post_file.name,
                    "title": title,
                    "action": "scheduled",
                    "scheduled_time": stagger_time.isoformat(),
                    "entries": len(entries),
                })
            else:
                report.append({
                    "file": post_file.name,
                    "title": title,
                    "action": "would_schedule",
                    "scheduled_time": stagger_time.isoformat(),
                    "channels": channels,
                })

            scheduled_count += 1

        return {
            "total_files": len(post_files),
            "scheduled": scheduled_count,
            "skipped": skipped_count,
            "execute": execute,
            "posts": report,
        }

    @staticmethod
    def _parse_jekyll_frontmatter(path: Path) -> dict[str, Any] | None:
        """Parse YAML frontmatter from a Jekyll post file.

        Returns the frontmatter dict, or None if no valid frontmatter found.
        """
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return None

        if not text.startswith("---"):
            return None

        # Find closing ---
        end = text.find("---", 3)
        if end == -1:
            return None

        import yaml
        try:
            return yaml.safe_load(text[3:end])
        except Exception:
            return None

    def run_full_pipeline(
        self,
        event_type: str,
        repo_name: str,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Execute the full pipeline: select → render → check → dispatch → record."""
        if channels is None:
            channels = ["mastodon", "discord", "ghost"]

        template_id = self.select_template(event_type)
        channel_texts = self.render_and_check(template_id, repo_name, channels)

        if not channel_texts:
            return {"status": "no_channels_passed", "dispatched": 0}

        records = self.dispatch(channel_texts)
        self.record_analytics(records)

        return {
            "status": "complete",
            "template": template_id,
            "dispatched": len(records),
            "records": records,
        }


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        prog="kerygma_pipeline",
        description="ORGAN VII Kerygma distribution pipeline",
    )
    parser.add_argument("--social-config", type=Path, default=None)
    parser.add_argument("--analytics-store", type=Path, default=None)
    parser.add_argument("--calendar-config", type=Path, default=None)
    sub = parser.add_subparsers(dest="command")

    dispatch_p = sub.add_parser("dispatch", help="Render + dispatch announcement")
    dispatch_p.add_argument("--template", required=True, help="Template ID")
    dispatch_p.add_argument("--repo", required=True, help="Repository name")
    dispatch_p.add_argument("--channels", default="mastodon,discord",
                            help="Comma-separated channels")

    sub.add_parser("poll", help="Poll RSS for new events")
    sub.add_parser("templates", help="List available templates")
    sub.add_parser("status", help="Pipeline health report")
    sub.add_parser("activate", help="Pre-flight checks for pipeline readiness")
    sub.add_parser("metrics", help="Pull and display current platform metrics")

    preview_p = sub.add_parser("preview", help="Render template preview without dispatching")
    preview_p.add_argument("--template", required=True, help="Template ID")
    preview_p.add_argument("--repo", required=True, help="Repository name")
    preview_p.add_argument("--channel", default="mastodon", help="Channel to preview")

    report_p = sub.add_parser("report", help="Generate weekly distribution report")
    report_p.add_argument("--period", default="weekly", choices=["daily", "weekly", "monthly"],
                           help="Report period")

    # Schedule subcommands
    schedule_p = sub.add_parser("schedule", help="Manage publication schedule")
    schedule_sub = schedule_p.add_subparsers(dest="schedule_command")

    schedule_sub.add_parser("list", help="Show upcoming entries (next 7 days)")
    schedule_sub.add_parser("due", help="Show entries due now with priority scores")
    schedule_sub.add_parser("process", help="Render + dispatch all due entries")

    schedule_add_p = schedule_sub.add_parser("add", help="Add a schedule entry")
    schedule_add_p.add_argument("--content-id", required=True, help="Content identifier")
    schedule_add_p.add_argument("--channels", required=True, help="Comma-separated channels")
    schedule_add_p.add_argument("--at", required=True, help="Scheduled time (ISO 8601)")
    schedule_add_p.add_argument("--frequency", default="once",
                                choices=["once", "daily", "weekly", "biweekly", "monthly"],
                                help="Recurrence frequency")

    # Backfill subcommand
    backfill_p = sub.add_parser("backfill", help="Schedule undistributed Jekyll posts")
    backfill_p.add_argument("--posts-dir", type=Path, required=True,
                            help="Path to Jekyll _posts/ directory")
    backfill_p.add_argument("--channels", default="mastodon,discord",
                            help="Comma-separated channels")
    backfill_p.add_argument("--stagger", type=int, default=30,
                            help="Minutes between staggered posts (default: 30)")
    backfill_p.add_argument("--execute", action="store_true",
                            help="Actually create entries (default: dry-run)")

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return

    # Derive schedule_store_path from analytics_store if provided
    schedule_store = None
    if args.analytics_store:
        schedule_store = args.analytics_store.parent / "schedule.json"

    pipeline = KerygmaPipeline(
        social_config_path=args.social_config,
        analytics_store_path=args.analytics_store,
        calendar_config_path=args.calendar_config or args.social_config,
        schedule_store_path=schedule_store,
    )

    if args.command == "dispatch":
        channels = [c.strip() for c in args.channels.split(",")]
        event = EventContext(
            event_type=args.template,
            repo_name=args.repo,
        )
        channel_texts = pipeline.render_and_check(args.template, args.repo, channels, event)
        if channel_texts:
            records = pipeline.dispatch(channel_texts)
            pipeline.record_analytics(records)
            for r in records:
                print(f"  [{r['status'].upper()}] {r['channel']}: {r['url'] or r['error']}")
        else:
            print("No channels passed quality checks.")
    elif args.command == "poll":
        events = pipeline.poll_for_events()
        print(f"Found {len(events)} new events.")
        for ev in events:
            print(f"  - {ev['title']}: {ev['url']}")
    elif args.command == "templates":
        for t in pipeline._engine.list_templates():
            print(f"  {t.template_id}: {', '.join(t.channels)}")
    elif args.command == "status":
        report = pipeline.status()
        print(json.dumps(report, indent=2))
    elif args.command == "activate":
        report = pipeline.activate()
        print(json.dumps(report, indent=2))
    elif args.command == "metrics":
        metrics = pipeline._collect_platform_metrics()
        print(json.dumps(metrics, indent=2))
    elif args.command == "preview":
        text = pipeline.preview(args.template, args.repo, args.channel)
        print(text)
    elif args.command == "report":
        period_days = {"daily": 1, "weekly": 7, "monthly": 30}[args.period]
        report = pipeline.generate_report(period_days=period_days)
        print(report)
    elif args.command == "schedule":
        if not args.schedule_command or args.schedule_command == "list":
            upcoming = pipeline._scheduler.get_upcoming(hours=168)
            if upcoming:
                for e in upcoming:
                    print(f"  [{e.entry_id}] {e.channel} @ {e.scheduled_time.isoformat()} "
                          f"({'published' if e.published else 'pending'})")
            else:
                print("No upcoming entries in the next 7 days.")
        elif args.schedule_command == "due":
            prioritized = pipeline._scheduler.get_due_with_priority()
            if prioritized:
                for p in prioritized:
                    print(f"  [{p.entry.entry_id}] {p.entry.channel} "
                          f"priority={p.priority:.2f} modifier={p.modifier:.1f}")
            else:
                print("No entries due right now.")
        elif args.schedule_command == "process":
            results = pipeline.process_due_entries()
            print(f"Processed {len(results)} entries.")
            for r in results:
                print(f"  [{r['status'].upper()}] {r['entry_id']}")
        elif args.schedule_command == "add":
            channels = [c.strip() for c in args.channels.split(",")]
            at = datetime.fromisoformat(args.at)
            freq = Frequency(args.frequency)
            entries = pipeline.schedule_content(
                content_id=args.content_id,
                channels=channels,
                scheduled_time=at,
                frequency=freq,
            )
            print(f"Scheduled {len(entries)} entries.")
            for e in entries:
                print(f"  [{e.entry_id}] {e.channel} @ {e.scheduled_time.isoformat()}")
    elif args.command == "backfill":
        channels = [c.strip() for c in args.channels.split(",")]
        result = pipeline.backfill_from_posts(
            posts_dir=args.posts_dir,
            channels=channels,
            stagger_minutes=args.stagger,
            execute=args.execute,
        )
        mode = "EXECUTE" if result["execute"] else "DRY-RUN"
        print(f"[{mode}] Backfill: {result['scheduled']} to schedule, "
              f"{result['skipped']} skipped, {result['total_files']} total files.")
        for post in result["posts"]:
            print(f"  [{post['action'].upper()}] {post['file']}: {post.get('title', '')}")


if __name__ == "__main__":
    main()
