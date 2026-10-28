# AI Agent Skills (`Claude Code`, `Antigravity`, `Cursor`, `Codex`)

`dbt-virtual` ships ready-to-use, DRY **Agent Skills** in [`.claude/skills/`](https://github.com/Flip-to/dbt-virtual/tree/main/.claude/skills) so AI coding assistants (Claude Code, Google Antigravity, Cursor, GitHub Copilot, OpenAI Codex) can automatically set up `dbt-virtual`, migrate existing multi-terabyte projects in zero compute, and run day-to-day virtual environment operations.

---

## 1. Available Skills

| Skill Name | Path | Slash Command | Purpose |
|---|---|---|---|
| **`dbt-virtual-setup`** | `.claude/skills/dbt-virtual-setup/SKILL.md` | `/dbt-virtual-setup` | Inspects an existing or new dbt project, installs `dbt-virtual`, configures `vars.dbt_virtual` in `dbt_project.yml`, wires `generate_schema_name` and `generate_alias_name` without breaking non-virtualized models, and runs zero-compute table adoption (`dbt-virtual adopt`). |
| **`dbt-virtual-ops`** | `.claude/skills/dbt-virtual-ops/SKILL.md` | `/dbt-virtual-ops` | Runs day-to-day lifecycle workflows: `status`, `plan`, `build`, column schema `diff`, pre-promotion `audit`, zero-rebuild `promote`, instant `rollback`, ephemeral PR environments (`init-env` / `drop-env`), and `gc`. |
| **`record-decision`** | `.claude/skills/record-decision/SKILL.md` | `/record-decision` | Records an Architectural Decision Record in `docs/decisions/YYYYMMDD_<slug>.md` and regenerates the decision index. |
| **`record-tech-debt`** | `.claude/skills/record-tech-debt/SKILL.md` | `/record-tech-debt` | Records or marks paid a tech debt item in `docs/tech_debt/<slug>.md` with a linked `.beans/` work item. |
| **`a2v-track`** | `.claude/skills/a2v-track/SKILL.md` | `/a2v-track` | Creates or advances a bean's `Asked → Built → Proven → Reviewed → Shipped → Verified` track in `.beans/`. |

---

## 2. How to Install the Skills in Your Own dbt Project

Because the skills follow a strict **DRY (Don't Repeat Yourself)** design, they are lightweight (`~60 lines` each) and reference the canonical documentation guides (`docs/guides/`).

To add `/dbt-virtual-setup` and `/dbt-virtual-ops` to your own dbt repository (e.g. for Claude Code, Antigravity, or Cursor):

### Option A: Copy from `dbt_packages/dbt_virtual` after `dbt deps`
Once you add `dbt-virtual` to `packages.yml` and run `dbt deps`, copy the skills into your project's `.claude/skills/` directory:

```bash
mkdir -p .claude/skills
cp -r dbt_packages/dbt_virtual/.claude/skills/dbt-virtual-setup .claude/skills/
cp -r dbt_packages/dbt_virtual/.claude/skills/dbt-virtual-ops .claude/skills/
```

*(Note: If your `dbt deps` only copies `macros/`, you can fetch the two skill files directly from GitHub)*:
```bash
mkdir -p .claude/skills/dbt-virtual-setup .claude/skills/dbt-virtual-ops
curl -sSL https://raw.githubusercontent.com/Flip-to/dbt-virtual/main/.claude/skills/dbt-virtual-setup/SKILL.md \
  -o .claude/skills/dbt-virtual-setup/SKILL.md
curl -sSL https://raw.githubusercontent.com/Flip-to/dbt-virtual/main/.claude/skills/dbt-virtual-ops/SKILL.md \
  -o .claude/skills/dbt-virtual-ops/SKILL.md
```

### Option B: Global Skill Installation (Available Across All Your Repositories)
Install the skills once into your user-level agent skills directory so every project on your machine has `/dbt-virtual-setup` and `/dbt-virtual-ops`:

```bash
# For Claude Code (~/.claude/skills) or Antigravity (~/.gemini/config/skills):
cp -r .claude/skills/dbt-virtual-setup ~/.claude/skills/
cp -r .claude/skills/dbt-virtual-ops ~/.claude/skills/
```

---

## 3. Example Prompts with `/dbt-virtual-setup` and `/dbt-virtual-ops`

Once installed, you can simply tell your agent:

- **Initial Setup & Zero-Compute Migration**:
  > `/dbt-virtual-setup Migrate our existing BigQuery dbt project to dbt-virtual starting with the models/metrics folder, using a two-phase zero-compute adopt so we don't rebuild any existing production tables.`

- **Creating a New Virtualized TVF or Incremental Model**:
  > `/dbt-virtual-ops Add a new column discount_amount NUMERIC to orders_metrics using virtual_add_columns so staging clones from prod without a full refresh, then run plan and diff.`

- **Promoting a Release with Pre-Promotion Audit Gate**:
  > `/dbt-virtual-ops Audit release v2026.10.27 between staging and prod with a max 10% row drop threshold, and if it passes, promote it to prod.`
