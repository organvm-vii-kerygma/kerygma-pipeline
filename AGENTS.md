<!-- ORGANVM:AUTO:START -->
## Agent Context (auto-generated — do not edit)

This repo participates in the **ORGAN-VII (Marketing)** swarm.

### Active Subscriptions
- Event: `product.release` → Action: Poll for new release and dispatch announcement
- Event: `essay.published` → Action: Trigger POSSE distribution pipeline
- Event: `promotion.completed` → Action: Orchestrate multi-channel announcement

### Production Responsibilities
- **Produce** `distribution_event` for ORGAN-IV

### External Dependencies
- **Consume** `template_registry` from `announcement-templates`
- **Consume** `social_config` from `social-automation`
- **Consume** `distribution_strategy` from `distribution-strategy`
- **Consume** `research-publication` from `ORGAN-I`
- **Consume** `creative-release` from `ORGAN-II`
- **Consume** `community-announcement` from `ORGAN-VI`
- **Consume** `client-launch` from `ORGAN-III`

### Governance Constraints
- Adhere to unidirectional flow: I→II→III
- Never commit secrets or credentials

*Last synced: 2026-04-14T21:32:13Z*
<!-- ORGANVM:AUTO:END -->
