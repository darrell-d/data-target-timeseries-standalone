# data-target-timeseries-standalone

**Temporary repo.** Lifted from `data-target-assets`'s `feature/timeseries-data-target` branch so the timeseries import flow can run as a regular processor node while the real data-target story is still being figured out. Expect this to be folded back (or deleted) once the orchestrator-side work on data-target auth lands.

Does the same thing as `cmd/timeseries` in `data-target-assets`: discover staged chunks, find/create a viewer asset, create channels under it, upload chunks to S3 with STS creds from the create-asset call, register ranges via timeseries-service, mark the asset ready. See `internal/timeseries/handler.go` for the full flow.

## Auth

Two operating modes, depending on what the orchestrator injects:

### Processor mode (the normal case for this repo)

The orchestrator injects `SESSION_TOKEN` (and `REFRESH_TOKEN`) for every processor node — confirmed in `compute-node-aws-provisioner-v2/internal/aslconverter/asl.go` for both ECS and Lambda dispatch. A Cognito Bearer works on **both** API hosts (legacy and api2), so the workflow callback scheme isn't used at all.

**`SESSION_TOKEN` will usually be expired by the time this stage runs.** It's minted once when the workflow run starts, stored in Secrets Manager, and every stage's `ResolveToken` reads that same value — nothing refreshes it. Cognito access tokens live ~60 minutes; a chain that transfers a large NWB file and chunks it takes longer than that. The symptom is a bare `HTTP 403: {"message":"Forbidden"}` from api2 — API Gateway's own response when its authorizer rejects the JWT, not a workflow-service error.

So the binary treats `SESSION_TOKEN` as a cache entry, not a constant: it reads the `exp` claim and, when the token is expired or within 5 minutes of expiring, exchanges `REFRESH_TOKEN` via Cognito `REFRESH_TOKEN_AUTH` for a fresh one. Refresh tokens are long-lived (30d by default), so this works hours into a run. Every request re-checks, so a stage uploading thousands of chunks stays authenticated as it crosses the boundary.

`REFRESH_TOKEN_AUTH` needs the Cognito app client id, resolved in order: `PENNSIEVE_COGNITO_APP_ID` if set, then the `client_id` claim on the session token itself (present on Cognito access tokens, readable even when expired), then the unauthenticated `GET /authentication/cognito-config` on the legacy host. Processors need none of these configured.

The orchestrator does **not** inject `CALLBACK_TOKEN`, `DATASET_ID`, or `ORGANIZATION_ID` for processors — those are target-only. The binary derives `DATASET_ID` from the execution-run lookup (`GET /compute/workflows/runs/{runId}`), so it doesn't need to come from env.

### Target mode (legacy fallback)

If running as a data-target node, the orchestrator injects `CALLBACK_TOKEN` + `DATASET_ID` but not `SESSION_TOKEN`. The Callback scheme works on api2 but the legacy host needs a real Bearer, so the binary falls back to minting one via Cognito `USER_PASSWORD_AUTH` using `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID`. This is the workaround the original `data-target-assets` was built around. It's kept for back-compat and local dev — when running as a processor, none of these env vars are needed.

### Resolution order

`setAuthHeader` uses a Bearer on both hosts whenever any bearer source is configured, and falls back to `Callback workflow-service:<runId>:<token>` only when none is (target mode with just a callback token). `getBearer` derives that Bearer in order:

1. `SESSION_TOKEN`, while more than 5 minutes of its `exp` remain.
2. `REFRESH_TOKEN` exchanged via Cognito `REFRESH_TOKEN_AUTH`. (The usual path here — see above.)
3. `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` minted via `USER_PASSWORD_AUTH`. (Target mode / local dev.)

Results are cached until near expiry. If `SESSION_TOKEN` has no readable `exp` and there's no refresh token, it's used as-is rather than failing early.

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

- `REFRESH_TOKEN` — injected alongside `SESSION_TOKEN` for processor nodes. Not strictly required, but without it a run longer than the session token's lifetime fails with a 403 partway through.
- `PENNSIEVE_API_KEY` + `PENNSIEVE_API_SECRET` + `PENNSIEVE_COGNITO_APP_ID` — only when `SESSION_TOKEN` is empty and you need legacy-host access (target mode).
- `DATASET_ID` — only in target mode; processor mode derives it from the execution-run lookup.

Optional: `ORGANIZATION_ID` (logging), `PENNSIEVE_COGNITO_REGION` (defaults `us-east-1`), `ASSET_NAME`, `ASSET_TYPE`, `ASSET_PROPERTIES_FILE`.

## Re-running a workflow

By default a re-run is a **no-op** when the asset is already `ready`: the binary finds it by name+type on the workflow's packages and returns early without uploading anything. That's what makes orchestrator retries cheap.

Set `FORCE_REINGEST=true` on the node to rebuild instead. Nothing is overwritten in place — the asset id is part of the S3 prefix, so a rebuild deletes the existing asset's channels, deletes the asset (packages-service deletes its S3 prefix inside the same DELETE request), and ingests fresh under a new prefix with new channel node ids. There is a window where the asset is gone and the new one isn't ready yet, so don't force a re-ingest on something someone is actively viewing.

The new channel node ids are load-bearing. `timeseries.ranges` has an `EXCLUDE USING gist (channel, range)` constraint and lives in a different database from `channels` and `viewer_assets`, so there is no FK and nothing cascades: deleting a channel or an asset leaves its range rows behind. Rebuilding under fresh channel node ids means the re-registered ranges never collide with the old ones. The orphans are dead weight — reads select by channel node id, so nothing queries them again — but they do accumulate, one row per chunk per forced re-ingest.

Channels are deleted before the asset on every replacement, forced or not. `channels.viewer_asset_id` has no FK, so deleting only the asset leaves channels pointing at a dead row, and the reuse guard in `createOrResolveChannels` then hard-errors on every later run (`... is linked to viewer_asset_id=... but the current ingest expects ...`). Runs that die hard enough to skip cleanup entirely can still leave that state behind — `cmd/orphaned-channels` clears it.

## Known caveat carried over

`runCleanup` in `internal/timeseries/handler.go` is still commented out at the failure-path call site so failed runs leave the asset + channels behind for inspection. Re-enable before treating this as production-ready.
