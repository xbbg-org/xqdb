# Release Rules

<!-- last-analyzed: 2026-08-28T02:50:00Z -->

## Version Sources

Every source below must hold the identical version string, and the tag must be
exactly `v<version>`. `.github/workflows/NPM.yml` job `source` enforces this and
fails the release on any mismatch.

| File                                          | Field                                                          |
| --------------------------------------------- | -------------------------------------------------------------- |
| `Cargo.toml`                                  | `[workspace.package] version`                                  |
| `Cargo.lock`                                  | `version` of packages `xqdb`, `napi-xqdb`, `py-xqdb`            |
| `js-xqdb/package.json`                        | `version` and all three `optionalDependencies` pins (exact)     |
| `js-xqdb/package-lock.json`                   | `version`, `packages[""].version`, `packages[""].optionalDependencies` |
| `js-xqdb/npm/win32-x64-msvc/package.json`     | `version`                                                      |
| `js-xqdb/npm/linux-x64-gnu/package.json`      | `version`                                                      |
| `js-xqdb/npm/darwin-arm64/package.json`       | `version`                                                      |
| `js-xqdb/test/package.test.ts`                | `expect(root.version).toBe(...)` — asserts the literal version   |

The Python distribution has no version field: `pyproject.toml` sets
`dynamic = ["version"]` and setuptools-scm derives it from the git tag, so
tagging is what versions the wheel.

`js-xqdb/package-lock.json` must not contain resolved `node_modules/@xbbg/xqdb-*`
entries. A local `npm install` adds them pinned to the previously published
version, which then contradicts the new `optionalDependencies` pin. Remove them
before tagging.

Be aware that removing them does not make `npm ci` work either: with the entries
absent, npm 11 aborts with `EUSAGE ... package.json and package-lock.json are not
in sync`, naming all three natives as `Missing`, and `--omit=optional` fails
identically. Between a version bump and the natives being published there is no
lock state that satisfies `npm ci`, because the exact pin refers to a version the
registry does not yet serve. This is why every workflow in `JS.yml` and `NPM.yml`
uses `npm install`, never `npm ci`; do not "fix" them to `npm ci`. After the
natives are published, `npm install --package-lock-only` will record matching
entries, and `npm ci` works again until the next bump.

Declaring `"workspaces": ["npm/*"]` to satisfy the pins locally does not work:
workspace packages are linked unconditionally, so npm enforces their `os`/`cpu`
fields and fails on any host with `notsup Valid cpu: arm64 / Actual cpu: x64`.

No release automation script exists; the bump is a manual edit across the files
above.

## Release Trigger

- **npm**: push a `v*` tag. `NPM.yml` builds all three native targets, packs,
  smoke-tests, and publishes.
- **PyPI**: manual `workflow_dispatch` of `CI.yml` with `dry-run: false`, from the
  default branch, and only on repository `xbbg-org/xqdb`. A tag push does not
  publish to PyPI.
- Tags must be pushed to the `xqdb` remote (`xbbg-org/xqdb`). `Taskfile.yml`'s
  `tag` task pushes to `origin`, which is `jshinonome/kola` — the upstream this
  project forked from. Do not use it.

## Test Gate

From `.github/workflows/Quality.yml`:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --exclude py-xqdb --all-targets --locked
cargo test --workspace --exclude py-xqdb --doc --locked
python -m pytest -q py-xqdb/test
```

Plus `npm test` in `js-xqdb` from `JS.yml`.

`py-xqdb` is excluded from `cargo test` deliberately: it is a `cdylib` and its
test binary cannot resolve the Python DLL. `cargo test --workspace` without the
exclusion fails with `STATUS_DLL_NOT_FOUND`.

Run each gate without a masking pipeline. `cmd | tail` reports `tail`'s exit
status, so a failing suite looks green.

Python tests exercise a live q fixture when `XQDB_TEST_Q_EXTERNAL=1`; otherwise
the live cases skip. `XQDB_Q_ROWS` must match the running fixture.

## Registry / Distribution

| Target | Package                                            | Publisher                        |
| ------ | -------------------------------------------------- | -------------------------------- |
| npm    | `@xbbg/xqdb` plus three platform packages          | `NPM.yml`, on `v*` tag push      |
| PyPI   | `xqdb`                                             | `CI.yml` job `release`, manual   |

## Release Notes Strategy

Conventional Commits, no `CHANGELOG.md`. Tags are annotated with the message
`XQDB <version>`.

Notes live in `docs/release-notes/v<version>.md`, checked in before tagging. The
`github-release` job in `NPM.yml` passes that file to `gh release create
--notes-file` when it exists, and `gh release edit --notes-file` when the release
was pre-created; it falls back to `--generate-notes` only when the file is
absent. That job therefore checks the repository out — without a checkout the
file is not on disk, and the release silently degrades to commit subjects.

Subject lines alone are not sufficient when a release changes observable
behaviour, because `--generate-notes` emits only subjects and never a commit
body. Any release containing a breaking change MUST ship a notes file spelling
out the old behaviour, the new behaviour, and the migration. Draft the routine
part from `git log <prev-tag>..HEAD --no-merges --format="%s"`.

`v0.1.4` carries one such change — Python q timestamp atoms are now naive where
0.1.3 returned UTC-aware values — and its notes file already documents it.

## CI Workflow Files

- `.github/workflows/NPM.yml` — npm release on tag
- `.github/workflows/CI.yml` — PyPI release on manual dispatch
- `.github/workflows/Quality.yml` — Rust and Python gates
- `.github/workflows/JS.yml` — Node gates

## First-Time Setup Gaps

None. Release workflows exist, build artifacts are gitignored, and tags are in
use (`v0.1.0` through `v0.1.3`). `v0.1.4` is prepared in the manifests but not
yet tagged.
