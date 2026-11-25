# Versioning and release policy

English | [简体中文](versioning.zh-CN.md)

This document defines how the spark-pudding product version, Spark compatibility lines,
branches and release artifacts are managed. It is a policy for maintainers and consumers, not a
history of past releases.

## Version scheme

spark-pudding uses `MAJOR.MINOR.PATCH`:

```text
1.1.0
│ │ └─ PATCH: bug fixes only
│ └─── MINOR: backward-compatible features and new Spark minor support
└───── MAJOR: Spark major baseline or breaking API/configuration changes
```

The product version is independent of the Spark version:

```text
spark-pudding 1.1.0 + Spark 3.4
spark-pudding 1.1.0 + Spark 3.5
spark-pudding 2.0.0 + Spark 4.0
```

## What changes which number

| Change | Version position | Example |
| --- | --- | --- |
| Bug fix only | PATCH | `1.1.1` |
| New source, sink or transform capability | MINOR | `1.2.0` |
| New Spark 3.5 support | MINOR | `1.1.0` |
| New Spark 4.0 support | MAJOR | `2.0.0` |
| Breaking API, configuration or default behavior change | MAJOR | `2.0.0` |

Rules:

- A PATCH release must not add connectors, transforms or new configuration surface.
- A MINOR release may add components and Spark minor support, but existing job files must remain
  valid and existing behavior must remain compatible.
- A MAJOR release may change the Spark baseline, Scala/JDK baseline, public API or job semantics;
  it must include migration notes.
- Adding a source, sink or transform is a MINOR feature, not a patch.
- Supporting a new Spark major version is a MAJOR release even when the spark-pudding API is
  unchanged, because the runtime, dependency and compatibility baseline changes.

## Release lines

| Product line | Spark baseline | Status |
| --- | --- | --- |
| `1.0.x` | Spark 3.4 | Current maintenance line |
| `1.1.x` | Spark 3.4 and 3.5 | Planned minor line |
| `1.2.x` | Spark 3.4 and 3.5 plus new components | Future minor line when capabilities are added |
| `2.0.x` | Spark 4.0 | Planned major line |

Each minor line owns its own patch series. For example, fixes for `1.2.0` are released as
`1.2.1`, `1.2.2`, not `1.1.x`.

## Branch model

```text
main                  Development trunk for the next minor or major release
v-1.0                 1.0.x maintenance
v-1.1                 1.1.x maintenance
v-2.0                 2.0.x maintenance
```

Rules:

- New features and new Spark support are developed on `main`.
- Release branches receive bug fixes only.
- Fixes are merged to `main` first, then cherry-picked to the affected maintenance branches.
- A maintenance branch is created when its product line is released, not for every Spark build.
- Spark 3.4 and 3.5 are built from the same branch and source tree; they do not get separate
  branches.
- A separate branch is created only when the source or dependency baseline cannot be shared.

## Tags and release artifacts

A release uses one immutable product tag:

```text
v1.1.0
```

The same tag can publish multiple Spark-specific distributions:

```text
spark-pudding-1.1.0-spark3.4.tar.gz
spark-pudding-1.1.0-spark3.5.tar.gz
```

Do not create separate tags such as `v1.1.0-spark3.4` and `v1.1.0-spark3.5` unless the product
lines are intentionally released independently.

Every distribution must identify its compatibility baseline:

```yaml
productVersion: 1.1.0
sparkVersion: 3.5.x
sparkCompatVersion: "3.5"
scalaBinaryVersion: "2.12"
```

The client must reject or warn when the distribution Spark version does not match the target
runtime.

## Compatibility promises

| Release type | Source compatibility | Job configuration | Components |
| --- | --- | --- | --- |
| PATCH | No public API break | No new required fields | Bug fixes only |
| MINOR | Additive APIs only | Existing jobs remain valid | New sources/sinks/transforms allowed |
| MAJOR | Breaking changes allowed | Migration required | Spark/API baseline may change |

Within a Spark major line, spark-pudding should keep its own public API stable across PATCH and
MINOR releases. Spark internal APIs are not a compatibility surface.

## Release checklist

1. Choose the version position using the table above.
2. Update the Maven reactor version with `mvn versions:set`.
3. Update Spark and runtime properties for every supported Spark profile.
4. Update `build/doc/compatibility.md` and this document when the matrix changes.
5. Run `mvn clean verify` for each supported Spark profile.
6. Run the container-backed regression suites for each Spark profile.
7. Verify the assembled distributions and capability manifests.
8. Create one immutable product tag.
9. Publish the Spark-specific distribution assets under that tag.
10. Publish release notes, including migration notes for MAJOR releases.
11. Keep maintenance branches in sync through cherry-picks.

## Examples

| Change | Version |
| --- | --- |
| Fix a CDC checkpoint bug in 1.1.0 | `1.1.1` |
| Add a new Elasticsearch source | `1.2.0` |
| Add Spark 3.5 builds to the 1.0 line | `1.1.0` |
| Add Spark 4.0 support | `2.0.0` |
| Change the default sink behavior | `2.0.0` |

## Spark 4.0

Spark 4.0 changes the platform baseline, including the Scala/JDK and dependency matrix and
potentially some Spark APIs. spark-pudding treats Spark 4.0 support as `2.0.0`, even if the
spark-pudding user-facing API remains mostly unchanged. Spark 3.4/3.5 remain on the 1.x lines.
