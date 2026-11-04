# ApexBase Documentation

This directory holds the Markdown source for the ApexBase documentation site.

## Navigation

- Getting Started: installation, quick start, core concepts.
- What's New in 2.0: V5 GA highlights and migration notes.
- User Guide: Python client, SQL, data import, server protocols, Agent Memory & RAG.
- Performance: public benchmark results.
- Reference and internals: Python API, Rust embedded API, architecture pages.
- Release notes and the full page inventory: [documentation-index.md](documentation-index.md).

## Local Preview

```bash
python -m pip install -r docs/requirements.txt
python -m mkdocs serve            # live preview at http://127.0.0.1:8000
python -m mkdocs build --strict   # fails on warnings and broken links
```

The site is built from the repository root. [mkdocs.yml](../mkdocs.yml) owns
navigation, and `.github/workflows/docs.yml` publishes versioned docs with
`mike`; start at [index.md](index.md) or [documentation-index.md](documentation-index.md).
