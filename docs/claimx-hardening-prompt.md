# ClaimX Production Hardening — Session Prompt

Copy everything below the line into a new Claude Code session.

---

Work through the production hardening fixes in `todo.md` in the prescribed order. The todo contains exact file paths, line numbers, current code, and proposed fixes for each issue.

## Rules

1. **Follow the fix order** in todo.md: C1 → H4 → H1 → H3 → M4 → M8 → H2 → M6 → M1 → M2 → H5 → M7 → M3
2. **Read before editing** — always read the target file and surrounding context before making changes. Line numbers in the todo may have shifted from earlier edits in this session.
3. **Run tests after each fix** — run `python -m pytest` scoped to the affected module after each change (e.g. `python -m pytest tests/core/security/ -x` after C1). Fix any test failures before moving on.
4. **Run mypy after each fix** — run `python -m mypy src/<changed-file> --strict` and fix type errors.
5. **One commit per fix** — after each fix passes tests and mypy, commit with a message like `fix(claimx): C1 — SSL bypass fails closed on unrecognized environments`. Do not batch unrelated fixes into one commit.
6. **Keep changes minimal** — implement exactly what the todo prescribes. Don't refactor surrounding code, add docstrings to unchanged functions, or make style changes.
7. **For H3** — before removing tables from `_APPEND_ONLY_TABLES`, verify that `MERGE_KEYS` in `delta_entities.py` already has entries for `contacts` and `media`. If not, add them with the keys specified in the todo.
8. **For H2** — after adding `_call_api_with_retry` to `base.py`, update all five handler files listed in the todo to wrap their API calls. Add tests for the retry utility.
9. **For H6** — write focused tests for the failure scenarios listed. Each test file should cover 3-5 specific failure cases. Use `pytest.mark.asyncio` and mock external dependencies.
10. **Skip M2 and M7 for now** — these require broader design decisions. Mark them as skipped in todo.md and move on.
11. **If a fix causes cascading test failures** — stop and explain the situation before proceeding. Don't apply speculative fixes to make tests pass.

## Context

- This is a Python 3.12 async pipeline using Azure EventHub, Delta Lake, and Polars
- The codebase uses mypy strict mode and ruff for linting
- Test framework is pytest-asyncio
- See `docs/claimx-production-readiness-review.md` for the full analysis behind each todo item

Start with C1 (SSL bypass fails open) and work down the list. After each fix, briefly report what you changed and the test results before moving to the next item.
