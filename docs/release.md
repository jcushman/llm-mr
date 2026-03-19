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

1. **Branch / PR:** Merge everything that should ship.
2. **Version:** Set `[project] version` in `pyproject.toml` to the new release (e.g. `0.2.0`).
3. **Changelog:** Under `## [Unreleased]`, add a `## [0.2.0] — YYYY-MM-DD` section and move notes from *Unreleased* into it. Update the compare links at the bottom of `CHANGELOG.md`.
4. **Tag:** Create and push an annotated tag whose name matches the version with a `v` prefix:

   ```bash
   git tag -a v0.2.0 -m "Release 0.2.0"
   git push origin v0.2.0
   ```

   The **Publish** workflow runs on `push` of tags matching `v*`. It runs `uv build` and `uv publish --trusted-publishing always`.

5. **Verify:** Check the workflow run on GitHub and confirm the new version on PyPI.

Tag `vX.Y.Z` should match `version = "X.Y.Z"` in `pyproject.toml`.

## Manual publish (fallback)

If you cannot use trusted publishing:

```bash
uv build
UV_PUBLISH_TOKEN=pypi-... uv publish
```

Prefer trusted publishing for CI; rotate or avoid storing long-lived tokens in secrets when possible.

## Pre-release checklist

- `just check` (Ruff + tests) passes locally.
- `CHANGELOG.md` and `pyproject.toml` versions agree with the tag you will push.
- For a new PyPI project, ensure the package name **llm-mr** is available and you have maintainer access.
