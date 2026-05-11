# data-target-timeseries-standalone

**Temporary repo.** Lifted from `data-target-assets`'s `feature/timeseries-data-target` branch so the timeseries import flow can run as a regular processor node while the real data-target story is still being figured out. Expect this to be folded back (or deleted) once the orchestrator-side work on data-target auth lands.

Does the same thing as `cmd/timeseries` in `data-target-assets`: discover staged chunks, find/create a viewer asset, create channels under it, upload chunks to S3 with STS creds from the create-asset call, register ranges via timeseries-service, mark the asset ready. See `internal/timeseries/handler.go` for the full flow.

## Auth

Two API hosts, two schemes:

- **`PENNSIEVE_API_HOST2` (api2)** — always uses the workflow callback scheme: `Callback workflow-service:<EXECUTION_RUN_ID>:<CALLBACK_TOKEN>`. Nothing to configure beyond the standard workflow env vars.
- **`PENNSIEVE_API_HOST` (legacy api)** — needs a Bearer JWT. Resolved in this order:

  1. `SESSION_TOKEN` set → used directly as the Bearer. **This is the path that matters when running as a processor**, since the orchestrator injects `SESSION_TOKEN` (and `REFRESH_TOKEN`) natively for processor nodes.
  2. `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` set → mint a Cognito access token via `USER_PASSWORD_AUTH` and cache it. Fallback for running outside the processor path (local dev, manual ECS runs, etc.).
  3. Neither set → falls back to the Callback scheme, which the legacy host will reject. `config.Load()` fails fast before reaching this case.

`PENNSIEVE_COGNITO_REGION` is optional; defaults to `us-east-1`.

The original `data-target-assets` baked the API key, secret, and Cognito app id into the binary as constants. Those are gone here — everything comes from env.

## Required env vars

| Var | Purpose |
|---|---|
| `INPUT_DIR` | Directory of staged chunk files |
| `EXECUTION_RUN_ID` | Workflow execution run id |
| `CALLBACK_TOKEN` | Orchestrator callback token (api2 auth) |
| `DATASET_ID` | Pennsieve dataset id |
| `PENNSIEVE_API_HOST` | Legacy api host, e.g. `https://api.pennsieve.io` |
| `PENNSIEVE_API_HOST2` | Api2 host, e.g. `https://api2.pennsieve.io` |
| `SESSION_TOKEN` *or* `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` | Legacy-host Bearer (see above) |

Optional: `ORGANIZATION_ID` (logging), `PENNSIEVE_COGNITO_REGION` (defaults `us-east-1`), `ASSET_NAME`, `ASSET_TYPE`, `ASSET_PROPERTIES_FILE`.

## Known caveat carried over

`runCleanup` in `internal/timeseries/handler.go` is still commented out at the failure-path call site so failed runs leave the asset + channels behind for inspection. Re-enable before treating this as production-ready.
