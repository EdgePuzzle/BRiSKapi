BRiSK is a product of its respective owners. This independent project is not
endorsed by BRiSK, Tachibana, SBI, TSE or JPX.

The pinned public demo at https://next-demo.brisk.jp/ supplies proprietary WASM
and historical market data. Those files are fetched at runtime, are not included
here and are not covered by this repository's MIT software license.
Recording redistribution depends on the applicable data/provider permissions.

The BRiSK site client (`briskapi/brisk.py`) uses the endpoint sequence documented by
pybrisk (https://github.com/obichan117/pybrisk), Copyright (c) 2026 obichan117,
MIT License; its notice ships as `briskapi/LICENSE-pybrisk.txt`. The live transport
(`briskapi/decoder/engineio.cjs`, `brisk.cjs`) also relies on pybrisk's published
protocol research (the Engine.IO/Socket.IO layer, frame types, heartbeat and the
market token) and on the public Next demo's client behavior. briskapi's own calls
replace pybrisk's interface. The master file layout and the 18000 master record
layout rest on pybrisk's research too: `tools/brisk_mock/sbi_master_layout.py`
downloads, at the user's request and into the user's own cache, pinned copies of the
SBI decoder and master file published there. No vendor bundles or raw captures from
that repository are bundled here. A broker's BRiSK decoder (SBI's, e-shiten's, Matsui's, Monex's or SMBC Nikko's) is
downloaded at runtime with the user's own session and is never stored or redistributed by this project.
Dependencies retain their own licenses. Cargo.lock records Rust dependencies;
boto3 uses Apache-2.0.
