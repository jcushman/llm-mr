test *args:
    uv run pytest {{args}}

lint:
    uv run ruff check .
    uv run ruff format --check .

fix:
    uv run ruff check --fix .
    uv run ruff format .

check: lint test

# Tag v{version} from pyproject.toml, push branch + tag (triggers PyPI publish).
release:
    #!/usr/bin/env bash
    set -euo pipefail
    just check
    VERSION="$(uv version --short)"
    TAG="v${VERSION}"
    if ! git rev-parse --git-dir >/dev/null 2>&1; then
      echo "Not a git repository." >&2
      exit 1
    fi
    if ! git remote get-url origin >/dev/null 2>&1; then
      echo "No git remote named 'origin'. Add it before releasing." >&2
      exit 1
    fi
    if git show-ref --verify --quiet "refs/tags/${TAG}"; then
      echo "Tag ${TAG} already exists locally." >&2
      echo "Bump [project] version in pyproject.toml and CHANGELOG.md, commit, then run again." >&2
      echo "Or delete the tag: git tag -d ${TAG}" >&2
      exit 1
    fi
    if git ls-remote origin "refs/tags/${TAG}" 2>/dev/null | grep -q .; then
      echo "Tag ${TAG} already exists on origin." >&2
      echo "Bump [project] version in pyproject.toml and CHANGELOG.md, commit, then run again." >&2
      exit 1
    fi
    if [ -n "$(git status --porcelain)" ]; then
      echo "Working tree is not clean. Commit or stash before releasing." >&2
      exit 1
    fi
    echo "Pushing current branch, then tagging ${TAG} (version ${VERSION})..."
    git push
    git tag -a "${TAG}" -m "Release ${VERSION}"
    git push origin "${TAG}"
    echo "Pushed ${TAG}. GitHub Actions should publish to PyPI."

# Create a new ADR
adr title:
    #!/usr/bin/env -S uv run python
    import re, datetime, pathlib
    title = '{{title}}'
    slug = re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')
    date = datetime.date.today().isoformat()
    adr_dir = pathlib.Path('docs/adr')
    template = adr_dir.joinpath('TEMPLATE.md').read_text()
    adr_file = adr_dir.joinpath(f'{date}-{slug}.md')
    for i in range(1, 100):
        if not adr_file.exists():
            break
        adr_file = adr_dir.joinpath(f'{date}-{slug}-{i}.md')
    adr_file.write_text(template.replace('TITLE', title).replace('DATE', date))
    readme = adr_dir.joinpath('README.md')
    readme_text = readme.read_text().replace('*', f'* {date}: [{title}]({adr_file.name})\n*', 1)
    readme.write_text(readme_text)
    print(f'Created {adr_file}')