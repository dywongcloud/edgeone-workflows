/**
 * KV key layout for the EdgeOne KV-backed world.
 *
 * All keys are prefixed by a configurable namespace (default `owf`) so a
 * single KV namespace can host multiple deployments without colliding.
 *
 * | Purpose          | Key                                            | Value                       |
 * | ---------------- | ---------------------------------------------- | --------------------------- |
 * | Run blob         | {p}/run/{runId}                                | CBOR(WorkflowRun)           |
 * | Run status index | {p}/idx-run-status/{status}/{ulidSortable}     | runId                       |
 * | Step blob        | {p}/step/{runId}/{stepId}                      | CBOR(Step)                  |
 * | Step list index  | {p}/idx-step/{runId}/{stepId}                  | "" (presence)               |
 * | Event blob       | {p}/evt/{runId}/{eventId}                      | CBOR(Event)                 |
 * | Hook blob        | {p}/hook/{hookId}                              | CBOR(Hook)                  |
 * | Hook by run idx  | {p}/idx-hook-run/{runId}/{hookId}              | "" (presence)               |
 * | Hook token claim | {p}/tok/{tokenHash}                            | hookId                      |
 * | Wait blob        | {p}/wait/{runId}/{correlationId}               | CBOR(Wait)                  |
 * | Schedule job     | {p}/job/{paddedRunAtMs}/{messageId}            | CBOR(QueueJob)              |
 * | Stream chunk     | {p}/chunk/{runId}/{streamName}/{paddedIdx}     | CBOR(StreamChunk)           |
 * | Stream info      | {p}/strm/{runId}/{streamName}                  | CBOR(StreamInfo)            |
 *
 * The `idx-run-status` value sorts by ULID so listing is reverse-chronological
 * naturally; we list and reverse for the "latest N runs" queries.
 *
 * Schedule jobs use padded ms-since-epoch in the key so list-by-prefix
 * over `{p}/job/` returns due jobs in correct chronological order, and a
 * "due jobs up to now" query is a prefix-range filter on the listed keys.
 */

import { padTs } from './codec.js';

export interface KeyPrefixes {
  readonly p: string;
}

export function keys(p: string) {
  const root = p.endsWith('/') ? p.slice(0, -1) : p;
  return {
    root,
    runBlob: (runId: string) => `${root}/run/${runId}`,
    runStatusIdx: (status: string, sortKey: string) =>
      `${root}/idx-run-status/${status}/${sortKey}`,
    runStatusIdxPrefix: (status: string) => `${root}/idx-run-status/${status}/`,
    stepBlob: (runId: string, stepId: string) =>
      `${root}/step/${runId}/${stepId}`,
    stepIdxPrefix: (runId: string) => `${root}/idx-step/${runId}/`,
    stepIdx: (runId: string, stepId: string) =>
      `${root}/idx-step/${runId}/${stepId}`,
    evtBlob: (runId: string, eventId: string) =>
      `${root}/evt/${runId}/${eventId}`,
    evtPrefix: (runId: string) => `${root}/evt/${runId}/`,
    /** Dedup claim for a workflow-body attr_set (put-if-absent). */
    attrClaim: (runId: string, correlationId: string) =>
      `${root}/attr-claim/${runId}/${correlationId}`,
    hookBlob: (hookId: string) => `${root}/hook/${hookId}`,
    hookRunIdx: (runId: string, hookId: string) =>
      `${root}/idx-hook-run/${runId}/${hookId}`,
    hookRunIdxPrefix: (runId: string) => `${root}/idx-hook-run/${runId}/`,
    tokenBlob: (tokenHash: string) => `${root}/tok/${tokenHash}`,
    waitBlob: (runId: string, correlationId: string) =>
      `${root}/wait/${runId}/${correlationId}`,
    waitPrefix: (runId: string) => `${root}/wait/${runId}/`,
    jobBlob: (runAtMs: number, messageId: string) =>
      `${root}/job/${padTs(runAtMs)}/${messageId}`,
    jobPrefix: () => `${root}/job/`,
    jobPrefixUpTo: (runAtMs: number) => `${root}/job/${padTs(runAtMs)}`,
    streamChunk: (runId: string, name: string, paddedIdx: string) =>
      `${root}/chunk/${runId}/${name}/${paddedIdx}`,
    streamChunksPrefix: (runId: string, name: string) =>
      `${root}/chunk/${runId}/${name}/`,
    streamInfo: (runId: string, name: string) =>
      `${root}/strm/${runId}/${name}`,
    streamInfoPrefix: (runId: string) => `${root}/strm/${runId}/`,
  };
}

export type Keys = ReturnType<typeof keys>;
