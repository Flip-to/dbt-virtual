# A2V Track

Applies to: every bean, and every PR (each PR has a bean)

Work moves along **A2V: Asked to Verified**:

```
Asked → Built → Proven → Reviewed → Shipped → Verified
```

Every bean carries an `## A2V track` table, filled as the work moves. The `/a2v-track` skill
creates and advances it; `scripts/check_docs_layout.py` checks it on all beans in `.beans/`.

```markdown
## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: <person> | <the problem, one line> | issue / chat request | YYYY-MM-DD |
| Built | Agent: Claude (session <id>), approved by <person> | <approach, link decision> | PR #N, commits | |
| Proven | CI | <what the check proves> | L3: <test>, fails without the fix | |
| Reviewed | Reviewer: <person> | | PR approval | |
| Shipped | Developer: <person> | | merged | |
| Verified | Developer: <person> | | emulator E2E / post-release check | |
```

## Rules

1. **Evidence moves a step; claims do not.** Each step names its artifact.
2. **Proof expires.** A commit after the proof drops the item back to Built: re-run and update.
3. **Proof level** (L0-L4) on the Proven row says how strong "it works" is (`docs/conventions/docs_and_specs.md`). Only L1 and above is proof.
4. **No fake green.** A step that does not apply says `n/a: <reason>`; one not reached yet says `pending`.
5. **Who** starts with `Product:`, `Developer:`, `Reviewer:`, `CI`, or `Agent: Claude (session <id>), approved by <person>`.
