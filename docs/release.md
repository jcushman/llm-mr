# Releasing llm-mr

Releases are published to [PyPI](https://pypi.org/project/llm-mr/) from GitHub Actions when a version tag is pushed. Builds use [`uv`](https://docs.astral.sh/uv/) (`uv build`, `uv publish`).

## One-time setup (maintainers)

### PyPI trusted publishing

Configure **Trusted Publishing** on PyPI so GitHub Actions can upload without a long-lived API token:

1. On [pypi.org](https://pypi.org), open **llm-mr** → **Publishing** (or create the project on first upload via token, then add the publisher).
2. Add a **GitHub** trusted publisher:
   - **Repository:** `jcushman/llm-mr` (or your fork if you publish from elsewhere).
   - **Workflow name:** `publish.yml`.
   - **Environment:** `pypi` (must match the workflow).

### GitHub environment (optional but recommended)

In the repo **Settings → Environments**, create an environment named **`pypi`**. You can require reviewers or restrict to `main` so only intentional tag pushes trigger publishes.

The workflow attaches `environment: pypi` so PyPI’s trusted-publisher settings and GitHub protections stay aligned.

## Cutting a release

1. **Branch / PR:** Merge everything that should ship (usually on `main`).
2. **Version:** Set `[project] version` in `pyproject.toml` to the new release (e.g. `0.2.0`).
3. **Changelog:** Under `## [Unreleased]`, add a `## [0.2.0] — YYYY-MM-DD` section and move notes from *Unreleased* into it. Update the compare links at the bottom of `CHANGELOG.md`.
4. **Commit** the version and changelog changes so the working tree is clean.

### Automated tag and push

From the repo root, with `origin` set and your branch tracking it:

```bash
just release
```

This runs `just check`, reads the version with `uv version --short`, aborts if `vX.Y.Z` already exists locally or on `origin` (with a reminder to bump `pyproject.toml` / `CHANGELOG.md`), requires a clean working tree, then **`git push`** (current branch), **`git tag -a`**, and **`git push origin vX.Y.Z`**.

The **Publish** workflow runs on that tag push and runs `uv build` plus `uv publish --trusted-publishing always`.

### Manual tag (same outcome)

```bash
git tag -a v0.2.0 -m "Release 0.2.0"
git push origin v0.2.0
```

5. **Verify:** Check the workflow run on GitHub and confirm the new version on PyPI.

Tag `vX.Y.Z` must match `version = "X.Y.Z"` in `pyproject.toml`.

## Manual publish (fallback)

If you cannot use trusted publishing:

```bash
uv build
UV_PUBLISH_TOKEN=pypi-... uv publish
```

Prefer trusted publishing for CI; rotate or avoid storing long-lived tokens in secrets when possible.

## Pre-release checklist

- `just check` passes locally (or rely on `just release`, which runs it first).
- `CHANGELOG.md` and `pyproject.toml` versions agree with the tag you will push.
- For a new PyPI project, ensure the package name **llm-mr** is available and you have maintainer access.
