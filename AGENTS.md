# AGENTS.md

## Overview

GitHarbor (`githarbor`) is a unified Python client for Git hosting platforms (GitHub, GitLab,
Gitea/Codeberg, Azure DevOps, local git repositories). `create_repository(url)` picks a provider
by URL and returns a `Repository` proxy with one API (branches, issues, PRs, commits, releases,
tags, workflows, file download). The package also ships an MCP server (`githarbor` CLI) and a
jinjarope extension that exposes the functional API as Jinja globals/filters.

## Architecture

- `src/githarbor/core/base.py`: `BaseRepository` / `BaseOwner`. Abstract-ish base classes; every
  method raises `FeatureNotSupportedError` unless a provider overrides it. Sync methods
  (`get_branch`) and async ones (`get_branch_async`) both exist. `is_async` says which ones a
  provider implements natively; `url_patterns` + `supports_url()` drive URL matching.
- `src/githarbor/core/proxy.py`: `Repository`, the proxy users get. Each sync method calls
  `asyncio.run(provider.x_async())` for async providers; each `*_async` method uses
  `asyncio.to_thread` for sync providers. Also holds composite fallbacks
  (`get_recent_activity`, `compare_branches`, ...). `core/user_proxy.py` does the same for `Owner`.
- `src/githarbor/core/models.py`: dataclasses (`User`, `Branch`, `Commit`, `Issue`, `PullRequest`,
  `Release`, `Tag`, `Workflow`, `WorkflowRun`, ...) that all providers convert into.
- `src/githarbor/core/filechanges.py`: unified-diff parsing (needs `unidiff`, `diff` extra).
- `src/githarbor/registry.py`: `RepoRegistry`, a class-level registry of provider classes plus a
  `WeakValueDictionary` instance cache. `from_url()` tries providers in registration order.
- `src/githarbor/repositories.py` / `src/githarbor/users.py`: register providers if their SDK is
  importable (`importlib.util.find_spec`), and define `create_repository()` / `create_owner()`.
  `local` is registered last because `LocalRepository.supports_url()` accepts any existing path.
- `src/githarbor/providers/<name>_provider/`: one package per backend, each with
  `repository.py` (provider class), `utils.py` (SDK object -> model converters plus an error-handling
  decorator) and sometimes `owner.py`. Backends: `github` (PyGithub), `aiogithubapi`, `githubkit`,
  `gitlab` (python-gitlab), `gitea` (giteapy), `pygitea` (py-gitea), `azure` (azure-devops),
  `local` (GitPython).
- `src/githarbor/functional.py`: module-level `*_async(url, ...)` functions and sync versions
  built with `make_sync`; `setup_env()` is the `jinjarope.environment` entry point.
- `src/githarbor/mcp_server.py`: FastMCP server (`githarbor` console script, `python -m githarbor`).
  Tools are async and must call the `*_async` functional API (the sync wrappers use
  `asyncio.run` and fail inside the server's event loop).

## Where to look

| Task | Location |
|------|----------|
| Add/change a provider | `src/githarbor/providers/<name>_provider/`, register in `repositories.py` (and `users.py` for owners) |
| Add a repository operation | `core/base.py` (sync + async stub), `core/proxy.py` (forwarding), providers, `functional.py`, `mcp_server.py` |
| Data models | `src/githarbor/core/models.py` |
| URL -> provider resolution, caching | `src/githarbor/registry.py` |
| Exceptions | `src/githarbor/exceptions.py` |
| MCP tools / CLI args | `src/githarbor/mcp_server.py` |
| Tests | `tests/` (`conftest.py` has model fixtures) |
| Docs site config | `mkdocs.yml` (mkdocs-mknodes default site, `docs/` is empty) |

## Commands

```bash
uv sync --all-extras                    # setup (dev, lint, docs groups are default groups)
uv run pytest                           # tests; integration tests are deselected by default
uv run pytest -m integration            # live API tests (need GITHUB_TOKEN / GITLAB_TOKEN)
uv run ruff check . && uv run ruff format --check .
uv run ty check                         # type check (src/ only); warnings fail CI
uv run deptry src                       # dependency check
uv run mkdocs-mknodes build             # docs build (same as CI)
duty lint | lint_check | test | build | serve | update | version <bump>  # duties.py; duty is not a project dependency
```

CI (`.github/workflows/build.yml`) runs ruff, `ty check` and pytest on Python 3.13/3.14 across
Linux/macOS/Windows; 3.14 is `continue-on-error`. Tags trigger the PyPI release job.

## Conventions

- Python >= 3.13, `from __future__ import annotations`, PEP 695 generics (`def f[**P, T]`).
- Ruff with a broad rule set, line length 100, Google-style docstrings.
- Provider SDK imports happen lazily (inside functions/`__init__` or under `TYPE_CHECKING`), so
  importing any module works without that SDK installed. The docs build imports every submodule.
- Providers raise `githarbor.exceptions` types (`ResourceNotFoundError`, `AuthenticationError`,
  `FeatureNotSupportedError`, ...), usually via the error-handling decorator in their `utils.py` (`handle_github_errors`, `handle_api_errors`, ...).
- Type-check suppressions: `# ty: ignore[rule]` with a rule name; no blanket `# type: ignore`.

## Specs

Hosting APIs are used through third-party SDKs (extras in `pyproject.toml`); none is pinned to an API version beyond what the SDK sends.

| Spec | URL | Targets | Code | Update check |
|---|---|---|---|---|
| GitHub REST API (via PyGithub, githubkit, aiogithubapi) | https://docs.github.com/en/rest | unpinned (SDK default REST API version) | `src/githarbor/providers/github_provider/`, `githubkit_provider/`, `aiogithubapi_provider/` | https://docs.github.com/en/rest/about-the-rest-api/api-versions |
| GitLab REST API v4 (via python-gitlab) | https://docs.gitlab.com/api/rest/ | API v4 | `src/githarbor/providers/gitlab_provider/` | same URL |
| Gitea / Codeberg API v1 (via giteapy, py-gitea) | https://gitea.com/api/swagger | API v1 (`/api/v1`) | `src/githarbor/providers/gitea_provider/`, `pygitea_provider/` | https://docs.gitea.com/api/1.24/ |
| Azure DevOps REST API (via azure-devops) | https://learn.microsoft.com/en-us/rest/api/azure/devops/ | unpinned (SDK default api-version) | `src/githarbor/providers/azure_provider/` | same URL |
| Model Context Protocol (server, via fastmcp) | https://modelcontextprotocol.io/specification/latest | unpinned (follows the installed `mcp` SDK) | `src/githarbor/mcp_server.py` | same URL |

## Pitfalls

- Copier-managed (template `copier-phil65`): `.github/workflows/*`, `.pre-commit-config.yaml`,
  `duties.py`, `mkdocs.yml`, most of `pyproject.toml`. Don't hand-edit template-owned parts;
  add dependencies below the `# Only add below (Copier)` markers. Update with `copier update`.
- Every backend SDK is an optional extra (`github`, `gitlab`, `gitea`, `pygitea`, `azure`,
  `aiogithubapi`, `local`, `diff`, `mcp`, `all`). The `githubkit` extra is commented out and the
  githubkit provider targets an older githubkit API (`githubkit.versions.latest`); ty is told to
  allow its unresolved imports (`[tool.ty.analysis]`).
- Providers read tokens from env vars when not passed: `GITHUB_TOKEN`, `GITLAB_TOKEN`,
  `GITEA_TOKEN`, `AZURE_DEVOPS_PAT`. GitLab/Gitea constructors fail without a token.
- Tests that hit real APIs must be marked `@pytest.mark.integration` (`addopts = -m "not integration"`).
- `RepoRegistry` caches instances per URL + kwargs; call `RepoRegistry.clear_cache()` in tests
  that need fresh instances.
- Several GitHub providers match `github.com`; the first registered (`github`, PyGithub) wins.
