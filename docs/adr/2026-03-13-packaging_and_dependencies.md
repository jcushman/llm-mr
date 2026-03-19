# Packaging and Dependencies

**Date:** 2026-03-13
**Status:** Accepted

## Context

`llm-mr` is an llm plugin distributed via PyPI. It needs a build backend,
version management, dependency declarations, and typing support. The project
uses `uv` for development.

## Decision

**`uv_build` backend with static version.** `pyproject.toml` uses `uv_build`
as the build backend. The version lives only in `pyproject.toml` — `uv_build`
does not support dynamic versioning. Code that needs the version uses
`importlib.metadata.version("llm-mr")`.

**No `__version__` in `__init__.py`.** The package does not export
`__version__`. This matches the pattern used by click, rich, httpx, and other
modern Python libraries. Callers use `importlib.metadata` instead.

**openpyxl is a default dependency.** openpyxl is included in the main
dependency list, not as an optional extra. It is small and pure-Python, and
we want Excel support without an extra install step.

**`py.typed` marker.** An empty `llm_mr/py.typed` file is included per
PEP 561, signaling that the package ships inline type annotations.

**Dev deps in `[dependency-groups]`.** Development dependencies (ruff, pytest)
live in `[dependency-groups] dev`, not in `[project.optional-dependencies]`.
End users don't run tests; the dev group is sufficient and matches current
`uv` conventions.

## Alternatives Considered

**Dynamic versioning (e.g. `setuptools-scm`).** Would derive version from git
tags, but `uv_build` doesn't support it. A static version in one place is
simpler.

**openpyxl as optional extra.** Would require `pip install llm-mr[xlsx]` for
Excel support. The dependency is small enough (~3 MB) that the extra install
friction isn't worth it.

**`[project.optional-dependencies] test`.** Exposes test deps to end users
via `pip install llm-mr[test]`. Unnecessary — only developers run tests, and
they use `uv sync` which picks up `[dependency-groups]`.
