# Message contracts

One JSON Schema per message type and version, named `<type>.v<version>.json`. Each describes the whole message on the
wire: the envelope, with `type` and `version` fixed, and the message's `data`.

The files are generated from the Pydantic models, so don't edit them by hand:

```bash
uv run finguard-contracts generate   # after changing a message model
uv run finguard-contracts check      # what CI runs: fails if a file is out of date
```

Within a version only backward-compatible changes are allowed, such as adding an optional field. CI registers the base
branch's schemas in Apicurio (for a push to `main`, the previous commit's) and asks it whether the branch's schemas are
compatible. A breaking change needs a new version, which is a new contract and a new file.

Removing a contract fails CI unless its subject (`<type>.v<version>`) is listed in `RETIRED` in
`src/finguard/core/contracts.py`, since consumers can still meet its messages until the topics' retention expires.

Data models must forbid unknown fields at every level (`extra="forbid"`) and can't use fixed-length tuples: the
registry can't judge changes to either correctly, so `generate` refuses them.
