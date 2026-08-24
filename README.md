# meta-feeder-internetarchive

Internet Archive free audio (etree, netlabels, 78rpm). Open, keyless.

**Upstream:** Internet Archive (archive.org)
**Role:** bytes

A MetaMesh gateway feeder — one upstream, one job. Implements `FeederPlugin`
from [`meta-feeder-sdk`](https://github.com/worph/meta-feeder-sdk).

```bash
cargo build --release --bin internetarchive-feeder
cargo test
```

Scope and naming conventions: `docs/project-architecture/feeder-architecture.md`
in the meta-root.
