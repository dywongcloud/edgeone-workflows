/**
 * Centralized Redis key schema. All keys are namespaced under the configured
 * prefix (default "owf"). `:` is the separator; entity ids are validated by
 * assertSafeEntityId so they cannot contain it.
 */
export class Keys {
  constructor(private readonly p: string) {}

  // --- runs ---
  run(runId: string): string {
    return `${this.p}:run:${runId}`;
  }
  /** ZSET: score = createdAt ms, member = runId. */
  runsIndex(): string {
    return `${this.p}:runs`;
  }
  /** ZSET per status: member = runId. */
  runsByStatus(status: string): string {
    return `${this.p}:runs:status:${status}`;
  }

  // --- steps ---
  step(runId: string, stepId: string): string {
    return `${this.p}:step:${runId}:${stepId}`;
  }
  /** ZSET: score = createdAt ms, member = stepId. */
  stepsIndex(runId: string): string {
    return `${this.p}:steps:${runId}`;
  }

  // --- events ---
  /** LIST: append-ordered eventIds for a run. */
  eventList(runId: string): string {
    return `${this.p}:events:${runId}`;
  }
  /** HASH: eventId -> base64(CBOR(event)). */
  eventData(runId: string): string {
    return `${this.p}:eventdata:${runId}`;
  }
  /**
   * LIST: eventIds within one run sharing a correlationId. A correlation id is
   * only unique within its run, so the index is scoped to the run.
   */
  corrIndex(runId: string, correlationId: string): string {
    return `${this.p}:corr:${runId}:${correlationId}`;
  }

  /** STRING (SET NX): dedup claim for a workflow-body attr_set. */
  attrClaim(runId: string, correlationId: string): string {
    return `${this.p}:attrclaim:${runId}:${correlationId}`;
  }

  // --- hooks ---
  hook(hookId: string): string {
    return `${this.p}:hook:${hookId}`;
  }
  /** STRING (SET NX claim): tokenHash -> "runId|hookId". */
  hookToken(tokenHash: string): string {
    return `${this.p}:hooktoken:${tokenHash}`;
  }
  /** ZSET: all hooks, member = hookId. */
  hooksIndex(): string {
    return `${this.p}:hooks`;
  }
  /** ZSET: hookIds owned by a run. */
  hooksByRun(runId: string): string {
    return `${this.p}:hooks:run:${runId}`;
  }

  // --- waits ---
  wait(runId: string, correlationId: string): string {
    return `${this.p}:wait:${runId}:${correlationId}`;
  }
  /** ZSET: correlationIds of waits owned by a run. */
  waitsByRun(runId: string): string {
    return `${this.p}:waits:${runId}`;
  }

  // --- queue / scheduler ---
  /** HASH: queueName, body, attempt, headers, route. */
  job(messageId: string): string {
    return `${this.p}:job:${messageId}`;
  }
  /** ZSET: score = runAt ms, member = messageId. */
  schedule(): string {
    return `${this.p}:sched`;
  }
  /** STRING (SET NX): claim used by the 307 trampoline / dispatcher. */
  jobClaim(messageId: string): string {
    return `${this.p}:jobclaim:${messageId}`;
  }

  // --- locks ---
  runLock(runId: string): string {
    return `${this.p}:lock:run:${runId}`;
  }

  // --- streams ---
  /** LIST: base64 chunk payloads. */
  streamChunks(runId: string, name: string): string {
    return `${this.p}:stream:${runId}:${name}`;
  }
  /** HASH: { done: "1" } when closed. */
  streamMeta(runId: string, name: string): string {
    return `${this.p}:streammeta:${runId}:${name}`;
  }
  /** ZSET: stream names for a run. */
  streamList(runId: string): string {
    return `${this.p}:streamnames:${runId}`;
  }
  /** Pub/sub channel for live stream reads. */
  streamChannel(runId: string, name: string): string {
    return `${this.p}:streamch:${runId}:${name}`;
  }
}
