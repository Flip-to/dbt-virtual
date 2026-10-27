# Beans Tracking

Applies to: every task that produces or follows a PR

Work is tracked as beans in `.beans/` (git-tracked, prefix `dbt-virtual-`). The bean must mirror the PR, not lag behind it.

## Bean status mirrors PR state

- While the PR is a draft or under review, keep the bean `in-progress` in `.beans/` while work continues, then flip it to `completed` + archived in the last commit before merge.
- Every bean carries an `## A2V track` (Asked to Verified, with Who / Why / Evidence per step); see `.claude/rules/a2v-track.md`.
- The same final commit flips the status of any `docs/specs/` file the PR implements, and of any `docs/tech_debt/` item it pays (`paid` + `paid_by`, marker removed). See `docs/conventions/docs_and_specs.md`.
