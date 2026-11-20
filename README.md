# NEXUS AI MAX

Android-first personal AI assistant with a cloud execution backend.

## Architecture

The target direction is a single NEXUS conversational interface backed by a hybrid model gateway, durable cloud task execution, inspectable memory and evaluated skills. The initial strategy uses existing hosted/open models; training or merging a proprietary model is a later, evidence-driven option.

See [NEXUS Hybrid Intelligence Architecture](docs/NEXUS_HYBRID_INTELLIGENCE_ARCHITECTURE.md) for the system design, security boundaries, cost controls, and phased implementation roadmap.

## Current implementation

- Android application: `app/`
- Cloud agent prototype: `agent_server/`
- CI: `.github/workflows/`

The architecture document distinguishes implemented prototype features from planned capabilities.