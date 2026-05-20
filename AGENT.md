---
description: "Generic dispatcher agent that loads admin-uploaded markdown skill files from Redis and routes user prompts to the right one."
tags:
  - dispatcher
  - skills
  - redis
  - mastra
  - anthropic
authors: []
capabilities:
  - "Serves an admin web UI at /admin (password-gated) for uploading, listing, and deleting markdown skill files"
  - "Stores skill files in a Redis hash; refreshes its in-memory cache via Redis pub/sub when the admin changes them"
  - "Exposes a single run_skill tool the agent calls to load the matching skill's instructions before answering"
  - "Supports explicit /skill-name dispatch in chat messages to bypass automatic routing"
  - "Optional per-skill cron schedule with admin-supplied prompt; last-run output is persisted in Redis and surfaced in the admin UI"
  - "Ships with a bundled `example` skill that is ignored automatically once any user skill is uploaded"
integrations:
  - "anthropic"
  - "redis"
---

## Overview

`skillit` is a generic dispatcher agent. Operators upload markdown files via the
admin UI; each filename becomes the name of a "skill" whose content is treated
as the system instructions for handling matching user requests. The agent
loads skills from Redis at boot, refreshes via pub/sub on admin changes, and
either routes a user message to the best matching skill via the `run_skill`
tool, or executes an explicitly-named skill when the user prefixes their
message with `/<skill-name>`.

## Required inputs

| Input | Description |
|---|---|
| `ADMIN_PASSWORD` | Password gating the `/admin` upload UI |
| `SESSION_SECRET` | HMAC secret used to sign admin session cookies |
