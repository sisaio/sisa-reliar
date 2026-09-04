#!/usr/bin/env python3
"""Print the `--exclude <pkg>` arguments the MSRV job must pass, one token per line.

ADR 0025 lets a provider crate declare its driver's MSRV (sqlx 0.9 needs 1.94) while the
workspace floor stays lower (ADR 0024). Those packages cannot build on the floor toolchain, so
the `msrv` job excludes them and the `check` job covers them on the pinned toolchain instead.

The list is computed rather than hard-coded. A hard-coded `--exclude reliar-store-postgres` left
`examples/axum-outbox` — which merely depends on it — in the build, and the job failed.

`cargo --exclude` only skips building a package as a *root*; a package is still built when an
included member depends on it. So the dependent check here is not a nicety: it is what makes the
exclusion sound. A member that depends on an above-floor package without declaring its own
rust-version can neither build on the floor toolchain nor be excluded from it, and is an error.

Reads `cargo metadata --no-deps` JSON on stdin; MSRV floor from $MSRV. Diagnostics to stderr.
Run with `--selftest` to check the version-comparison rule without any cargo metadata.
"""

import json
import os
import sys


def version_tuple(version):
    """Normalise to exactly three components so comparisons are total and length-independent.

    Without the padding a bare tuple compares by length once the prefixes match, so
    `(1, 88, 0) > (1, 88)` is `True` and a package declaring `1.88.0` against a floor written
    `1.88` reads as "above the floor". That excludes it — and if the *workspace* floor is spelled
    with a patch component while `ci.yaml` is not, it excludes every member and the MSRV job
    silently checks nothing while staying green. A gate that passes by building nothing is worse
    than no gate.
    """
    parts = tuple(int(part) for part in version.split("."))
    return (*parts, 0, 0)[:3]


def parse(version, floor):
    """The package's declared MSRV, or the floor when it declares none."""
    return version_tuple(version) if version else floor


def selftest():
    """Guards the comparison rule the whole gate rests on. Run as a step in the `msrv` job."""
    floor = version_tuple("1.88")

    assert version_tuple("1.88") == version_tuple("1.88.0") == (1, 88, 0)
    assert version_tuple("1.88.0") == version_tuple("1.88.0.0")
    # The regression this exists for: a patch-suffixed equal version is NOT above the floor.
    assert not version_tuple("1.88.0") > floor
    assert not version_tuple("1.88.0") > version_tuple("1.88")
    assert not version_tuple("1.88") > version_tuple("1.88.0")
    # Genuinely-above and genuinely-below still compare correctly.
    assert version_tuple("1.94") > floor
    assert version_tuple("1.88.1") > floor
    assert not version_tuple("1.87") > floor
    assert not version_tuple("1.87.9") > floor
    # A package declaring nothing inherits the floor and is therefore never excluded.
    assert parse(None, floor) == floor
    assert not parse(None, floor) > floor

    print("msrv-exclusions selftest: ok", file=sys.stderr)
    return 0


def main() -> int:
    if "--selftest" in sys.argv[1:]:
        return selftest()

    floor = version_tuple(os.environ["MSRV"])
    packages = json.load(sys.stdin)["packages"]

    members = {pkg["name"] for pkg in packages}
    above = {p["name"] for p in packages if parse(p.get("rust_version"), floor) > floor}

    problems = []
    for pkg in packages:
        if pkg["name"] in above:
            continue
        for dep in pkg["dependencies"]:
            # `cargo check` without --all-targets does not build dev-dependencies.
            if dep.get("kind") not in (None, "build"):
                continue
            if dep["name"] in members and dep["name"] in above:
                problems.append((pkg["name"], dep["name"]))

    if problems:
        for pkg_name, dep_name in problems:
            print(
                f"::error::{pkg_name} depends on {dep_name}, which declares an MSRV above the "
                f"workspace floor {os.environ['MSRV']}, but {pkg_name} declares no rust-version of "
                f"its own. It can neither build on the MSRV toolchain nor be excluded from it. "
                f"Declare rust-version on {pkg_name} (ADR 0025).",
                file=sys.stderr,
            )
        return 1

    for name in sorted(above):
        print(f"excluding {name} (ADR 0025)", file=sys.stderr)
        print("--exclude")
        print(name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
