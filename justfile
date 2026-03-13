test *args:
    uv run pytest {{args}}

lint:
    uv run ruff check .
    uv run ruff format --check .

fix:
    uv run ruff check --fix .
    uv run ruff format .

check: lint test

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