---
name: a2v-track
description: Create or advance a bean's A2V track (Asked, Built, Proven, Reviewed, Shipped, Verified) with Who, Why, Evidence and When.
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob, Edit
---

# A2V track

Rule: `.claude/rules/a2v-track.md`.

1. Locate or create the bean in `.beans/`.
2. Ensure the `## A2V track` table exists with all six steps (`Asked`, `Built`, `Proven`, `Reviewed`, `Shipped`, `Verified`).
3. Advance steps only when backed by concrete evidence (commits, CI / pytest output, review approval).
4. Verify with `uv run python scripts/check_docs_layout.py`.
