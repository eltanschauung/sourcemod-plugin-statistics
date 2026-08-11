# Contributing

Run these checks before submitting a change:

```bash
cargo fmt --all -- --check
cargo test --all-targets
SM_PATH=/path/to/addons/sourcemod ./scripts/build_sourcemod.sh
```

Keep the SourceMod API nonblocking. Database access and durable storage belong in the Rust service.
