# Message contracts

One JSON Schema per message type and version, named `<type>.v<version>.json`. Each describes the whole message on the
wire: the envelope, with `type` and `version` fixed, and the message's `data`.

The files are generated from the Pydantic models, so don't edit them by hand:

```bash
uv run finguard-contracts generate   # after changing a message model
uv run finguard-contracts check      # what CI runs: fails if a file is out of date
```

Within a version only backward-compatible changes are allowed, such as adding an optional field. CI registers `main`'s
schemas in Apicurio and asks it whether the branch's schemas are compatible. A breaking change needs a new version,
which is a new contract and a new file.
