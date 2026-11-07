# Engram

An asynchronous Rust cache with a deserialized Moka L1 and a durable SQLite L2. L2 values are Serde-serialized and optionally Zstandard-compressed.

See [USAGE.md](USAGE.md) for setup, API examples, expiration, maintenance, and fetch-through behavior.
