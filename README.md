# `redb-bincode`

This crate is a wrapper around [`redb`](https://crates.io/crates/redb)
that makes keys and values require `T: bincode::Encode + bincode::Decode`
and serialize as a big-endian `bincode`, which makes working with `redb`
much more convenient.

It does also include a couple of minor deviations from `redb`, but stays
faithful to the original API.

It was born from personal need, and at the time of writing this,
contains only things that I actually needed. Having said that adding
missing bits should be as simple as copy-paste-modify existing code,
and contributions are welcome. Just be aware that you might need to
get your hands dirty if you're planning to use it.

Notably `bincode` pre-release version is used.

The approach used in this crate might be a bit heavy, but should
work for any serialization format, `serde`-based or not.
