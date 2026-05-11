# data-target-timeseries-standalone

**Temporary repo.** Lifted from `data-target-assets`'s `feature/timeseries-data-target` branch so the timeseries import flow can run as a regular processor node while the real data-target story is still being figured out. Expect this to be folded back (or deleted) once the orchestrator-side work on data-target auth lands.

Does the same thing as `cmd/timeseries` in `data-target-assets`: discover staged chunks, find/create a viewer asset, create channels under it, upload chunks to S3 with STS creds from the create-asset call, register ranges via timeseries-service, mark the asset ready. See `internal/timeseries/handler.go` for the full flow.

## Auth

Two operating modes, depending on what the orchestrator injects:

### Processor mode (the normal case for this repo)

The orchestrator injects `SESSION_TOKEN` (and `REFRESH_TOKEN`) for every processor node — confirmed in `compute-node-aws-provisioner-v2/internal/aslconverter/asl.go` for both ECS and Lambda dispatch. When `SESSION_TOKEN` is set, it's used as the Bearer on **both** API hosts (legacy and api2). The workflow callback scheme isn't used at all.

The orchestrator does **not** inject `CALLBACK_TOKEN`, `DATASET_ID`, or `ORGANIZATION_ID` for processors — those are target-only. The binary derives `DATASET_ID` from the execution-run lookup (`GET /compute/workflows/runs/{runId}`), so it doesn't need to come from env.

### Target mode (legacy fallback)

If running as a data-target node, the orchestrator injects `CALLBACK_TOKEN` + `DATASET_ID` but not `SESSION_TOKEN`. The Callback scheme works on api2 but the legacy host needs a real Bearer, so the binary falls back to minting one via Cognito `USER_PASSWORD_AUTH` using `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID`. This is the workaround the original `data-target-assets` was built around. It's kept for back-compat and local dev — when running as a processor, none of these env vars are needed.

### Resolution order in `setAuthHeader`

1. `SESSION_TOKEN` set → `Bearer <session>` on both hosts. (Processor mode.)
2. Else for legacy host: `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` set → mint + cache a Cognito token. (Target mode.)
3. Else: fall back to `Callback workflow-service:<runId>:<token>` (works on api2; legacy host will 401).

`PENNSIEVE_COGNITO_REGION` is optional; defaults to `us-east-1`. The original `data-target-assets` baked the API key, secret, and Cognito app id into the binary as constants — those are gone here, everything comes from env.

## Required env vars

| Var | Purpose |
|---|---|
| `INPUT_DIR` | Directory of staged chunk files |
| `EXECUTION_RUN_ID` | Workflow execution run id |
| `PENNSIEVE_API_HOST` | Legacy api host, e.g. `https://api.pennsieve.io` |
| `PENNSIEVE_API_HOST2` | Api2 host, e.g. `https://api2.pennsieve.io` |
| `SESSION_TOKEN` *or* `CALLBACK_TOKEN` | One of the two — see Auth above |

Conditionally required:

- `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` — only when `SESSION_TOKEN` is empty and you need legacy-host access (target mode).
- `DATASET_ID` — only in target mode; processor mode derives it from the execution-run lookup.

Optional: `ORGANIZATION_ID` (logging), `PENNSIEVE_COGNITO_REGION` (defaults `us-east-1`), `ASSET_NAME`, `ASSET_TYPE`, `ASSET_PROPERTIES_FILE`.

## Known caveat carried over

`runCleanup` in `internal/timeseries/handler.go` is still commented out at the failure-path call site so failed runs leave the asset + channels behind for inspection. Re-enable before treating this as production-ready.
