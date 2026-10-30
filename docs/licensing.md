# Licensing

## Project license

Source code is licensed under MIT OR Apache-2.0.

## Slint

Slint 1.18 is offered under three options:

1. GPL-3.0-only
2. Slint Royalty-free 2.0
3. Slint Software 3.0 (commercial)

Lingo is intended to ship as a closed-source desktop application. Using Slint
under GPL-3.0 would require releasing the application under GPL-3.0. The
royalty-free option is the current assumption recorded in `deny.toml`
(`LicenseRef-Slint-Royalty-free-2.0`); the exact obligations (attribution,
revenue/usage limits, redistribution terms) MUST be reviewed and confirmed
before any public release. If the royalty-free terms do not fit, obtain a
commercial license.

This note is an engineering decision record, not legal advice.

## Assets

All icons, illustrations and fonts shipped with Lingo must be original or
licensed under a permissive license (MIT/ISC/OFL/Apache-2.0) and recorded here.
Do not import assets extracted from third-party applications.

## Dependencies

`cargo-deny` enforces the allow-list in `deny.toml` for every dependency.
