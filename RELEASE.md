# Releasing Galexie

Galexie uses [Semantic Versioning](https://semver.org/) and is released as:
- A git tag of the form `galexie-vX.Y.Z` (e.g. `galexie-v26.0.0`, `galexie-v26.0.0-rc1`).
- A Docker image published to `stellar/stellar-galexie:X.Y.Z` on Docker Hub.
  When the tag does not contain `-rc`, the image is also tagged `latest`.

1. [Major release (with optional RC)](#1-major-release-with-optional-rc) — new protocol
   versions or any release that wants an RC cycle / isolation from ongoing `main` work.
2. [Minor or patch release (including `stellar-core` refresh)](#2-minor-or-patch-release-including-stellar-core-refresh)
   — bug fixes, small features, or refreshing the `stellar-core` bundled in the Docker image,
   shipped directly from `main`.

---

## How releases are wired up

Two jobs in `.github/workflows/galexie.yml` do the publishing:

- `publish-sha-tag-image` — runs on every push to `main` or `release/**`. Builds and pushes
  `stellar/stellar-galexie:<short-sha>`.
- `publish-release-tag-image` — runs on any `galexie-v*` tag push. Pulls the sha-tagged image
  from that commit, re-tags it as `stellar/stellar-galexie:X.Y.Z`, and pushes `:latest` for
  non-rc tags. **`publish-sha-tag-image` must have succeeded on the target commit first**,
  since this job pulls rather than rebuilds.

---

## 1. Major release (with optional RC)

Use this flow when shipping a new major protocol version (e.g. `v26.0.0` for Protocol 26) or
any release where you want an RC cycle and/or to isolate the release from ongoing work on
`main`, using a dedicated `release/vX.Y.Z` branch.

Protocol work is developed on the long-running `protocol-next` branch in parallel with `main`.
`main` is periodically merged **into** `protocol-next` to keep it up to date (see e.g. PR #67).
When the protocol is ready to ship, `release/vX.Y.Z` is cut **from `protocol-next`** — not from
`main`. Once the release is published, the release branch is merged back into `main`, which is
how the protocol work ultimately lands on `main`.

### Steps

1. **Create the release branch** off `protocol-next` once the work to be shipped is ready:
   ```
   git checkout protocol-next
   git pull
   git checkout -b release/vX.Y.Z
   git push -u origin release/vX.Y.Z
   ```

2. **Verify the `stellar-core` pins** in `.github/workflows/galexie.yml` are set to the
   intended versions: `STELLAR_CORE_VERSION` (baked into the shipped image) and
   `CAPTIVE_CORE_DEBIAN_PKG_VERSION` (the integration-test core binary, which should match
   `STELLAR_CORE_VERSION` so tests run against the same binary that ships).

3. **Update `CHANGELOG.md`** with a new `## [vX.Y.Z]` section describing the changes since the
   last release. Follow the style of previous entries (Updates / New Features / Bug Fixes /
   Breaking Changes) and include PR links. Open as a PR against `release/vX.Y.Z`.

4. **Confirm the sha-tag image exists.** Verify that `publish-sha-tag-image` on the tip of
   `release/vX.Y.Z` has succeeded and that `stellar/stellar-galexie:<short-sha>` is on Docker
   Hub for that commit.

5. **Publish the GitHub Release.** If the bundled `stellar-core` is itself at RC, first run
   through this step with `galexie-vX.Y.Z-rc1` as the tag and **Set as a pre-release** checked
   instead of **Set as the latest release**. The `latest` Docker tag is not moved for `-rc`
   tags. Validate the RC end-to-end, then repeat this step for the final release.

   On <https://github.com/stellar/stellar-galexie/releases/new>:
   - **Choose a tag:** type `galexie-vX.Y.Z` and pick "Create new tag on publish".
   - **Target:** `release/vX.Y.Z`.
   - **Set as the latest release:** checked.
   - Paste the changelog section as the notes.

   Publishing creates the tag and triggers `publish-release-tag-image`. Wait for it to finish
   and confirm `stellar/stellar-galexie:X.Y.Z` and `stellar/stellar-galexie:latest` are on
   Docker Hub.

6. **Merge the release branch back to `main`.** Open a PR from `release/vX.Y.Z` to `main`.

---

## 2. Minor or patch release (including `stellar-core` refresh)

Use this flow for minor or patch releases shipped directly from `main` (e.g. `v25.1.1`) —
bug fixes, small features, or **refreshing the `stellar-core` bundled in the Docker image**
(e.g. picking up a new patch/minor of core with fixes).

For a pure core refresh, the source change is to update `STELLAR_CORE_VERSION` in
`.github/workflows/galexie.yml` (the version baked into the shipped image), plus
`CAPTIVE_CORE_DEBIAN_PKG_VERSION` to match. Always cut a new patch version so consumers pinned
to the previous patch keep getting the old image.

### Steps

1. **Confirm `main` is in a releasable state** — CI is green and any version-pinning changes
   you want to ship are already merged.

2. **Update `CHANGELOG.md`** on `main` with the new `## [vX.Y.Z]` section. Submit as a PR and
   merge before publishing the release. For a pure core refresh a single-line entry is fine:
   ```
   ## [vX.Y.Z]

   ### Updates
   - Bumped bundled stellar-core to <new-core-version>.
   ```

3. **Wait for the `publish-sha-tag-image` workflow** on the merge commit to succeed.

4. **Publish a GitHub Release.** On
   <https://github.com/stellar/stellar-galexie/releases/new>:
   - **Choose a tag:** type `galexie-vX.Y.Z` and pick "Create new tag on publish".
   - **Target:** `main` (at the merge commit from step 2).
   - **Set as the latest release:** checked.
   - Paste the changelog section as the notes.

5. **Verify** `publish-release-tag-image` succeeds and the new `stellar/stellar-galexie:X.Y.Z`
   (and `latest`) image is on Docker Hub. For a core refresh, also confirm the image contains
   the intended `stellar-core` version.

---

## Quick checklist

- [ ] `STELLAR_CORE_VERSION` and `CAPTIVE_CORE_DEBIAN_PKG_VERSION` in
      `.github/workflows/galexie.yml` point at the intended core version and match each other.
- [ ] `CHANGELOG.md` has an entry for the new version.
- [ ] `publish-sha-tag-image` workflow has succeeded on the commit being released.
- [ ] Tag is of the form `galexie-vX.Y.Z` (or `galexie-vX.Y.Z-rcN` for release candidates).
- [ ] `publish-release-tag-image` workflow succeeded after the GitHub Release was published.
- [ ] Docker Hub shows the new `X.Y.Z` tag (and `latest`, for non-rc releases).
- [ ] For major releases: release branch merged back to `main`.
