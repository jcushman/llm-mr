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

### GitHub environment (`pypi`)

The workflow sets `environment: pypi` so PyPI **Trusted Publishing** can require that
exact environment name, and so you can use GitHub’s deployment protections.

**Important:** The publish job runs on a **tag** push (`v*`), not a branch push. If
the environment uses **Deployment branches and tags → Selected branches and tags**,
allowing only **`main`** is *not* enough — GitHub will reject the run with errors
like *“Tag 'v0.1.0' is not allowed to deploy… due to environment protection rules”*.

Configure it one of these ways:

1. **Recommended (tags + main):** In **Settings → Environments → pypi → Deployment
   branches and tags**, keep **Selected branches and tags**, then **Add deployment
   branch or tag rule**:
   - Add **`main`** (so any future branch-based jobs could use this environment), and
   - Add a **tag** rule with pattern **`v*`** (matches `v0.1.0`, `v1.2.3`, etc.).

2. **Simpler (all refs):** Set the environment to **All branches and tags** if you
   don’t need a whitelist. (Weaker restriction; fine for many small projects.)

3. **No GitHub environment:** You could remove `environment:` from `.github/workflows/publish.yml`
   and change PyPI’s trusted publisher to use an **empty** environment name — then
   GitHub won’t apply environment rules, but you lose that layer of control.

Wait for **Required reviewers** only if you want a human to approve each PyPI upload;
that’s separate from branch/tag rules.

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
