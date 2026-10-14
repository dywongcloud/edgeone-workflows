import {
  MessageId,
  type Queue,
  type QueuePayload,
  ValidQueueName,
} from '@workflow/world';
import { monotonicFactory } from 'ulid';
import type { ResolvedZeploConfig } from './config.js';

const ulid = monotonicFactory();
const newMessageId = () => `msg_${ulid()}`;

// Workflow orchestration and queued step invocations share the combined flow
// handler.
function routeFor(_queueName: string): 'flow' | 'step' {
  return 'flow';
}

/**
 * Build a Zeplo-backed implementation of the Workflow Queue interface.
 *
 * Enqueue: POST to
 *   `{endpoint}/{targetBaseUrl}/.well-known/workflow/v1/{flow|step}?msg={id}`
 *   with `_token` / `_delay` / `_retry` query params controlling Zeplo's
 *   queue behaviour. Zeplo persists the message and later POSTs it to the
 *   target URL — your `createQueueHandler` wrapper picks it up.
 *
 * createQueueHandler returns a fetch-style `(Request) => Response` that
 * verifies the webhook secret (if configured), runs your handler, and maps
 * the result to an HTTP response Zeplo understands:
 *   - undefined / void          → 200 OK             (delivered)
 *   - { timeoutSeconds: N }     → 200 OK + re-enqueue with _delay=N
 *                                  (Zeplo doesn't natively expose a "delay
 *                                  this retry by N seconds" semantic, so
 *                                  we re-enqueue ourselves)
 *   - thrown error              → 500                (Zeplo will retry)
 */
export function createZeploQueue(config: ResolvedZeploConfig): Queue {
  function buildZeploUrl(
    target: string,
    opts: { delaySeconds?: number; retry?: number } = {}
  ): string {
    // Zeplo URL pattern: {endpoint}/{full-target-url}?_token=...&_delay=...&_retry=...
    const u = new URL(`${config.endpoint}/${target}`);
    u.searchParams.set('_token', config.token);
    if (opts.delaySeconds && opts.delaySeconds > 0) {
      u.searchParams.set('_delay', String(Math.ceil(opts.delaySeconds)));
    }
    u.searchParams.set('_retry', String(opts.retry ?? config.defaultRetry));
    return u.toString();
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    const messageId = newMessageId();
    const route = routeFor(queueName);
    const target = `${config.targetBaseUrl}/.well-known/workflow/v1/${route}?msg=${encodeURIComponent(messageId)}`;
    const zeploUrl = buildZeploUrl(target, {
      delaySeconds: opts?.delaySeconds,
    });

    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'x-vqs-queue-name': queueName,
      'x-vqs-message-id': messageId,
      'x-vqs-message-attempt': '1',
      ...(opts?.headers ?? {}),
    };
    if (config.webhookSecret) {
      headers['x-zeplo-secret'] = config.webhookSecret;
    }

    const res = await fetch(zeploUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify(message),
    });

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(
        `[world-zeplo] enqueue to ${queueName} failed: HTTP ${res.status} ${text.slice(0, 200)}`
      );
    }

    return { messageId: MessageId.parse(messageId) };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => {
    return async (req: Request): Promise<Response> => {
      // Verify shared secret if configured.
      if (config.webhookSecret) {
        const sent = req.headers.get('x-zeplo-secret');
        if (sent !== config.webhookSecret) {
          return new Response('forbidden', { status: 403 });
        }
      }

      const url = new URL(req.url);
      const messageId =
        url.searchParams.get('msg') ??
        req.headers.get('x-vqs-message-id') ??
        newMessageId();
      const queueName = req.headers.get('x-vqs-queue-name') ?? '';
      // Zeplo exposes its current attempt count on x-zeplo-retry; fall back
      // to x-vqs-message-attempt for callers (e.g. the dashboard probe) that
      // don't go through Zeplo at all.
      const attempt = Number(
        req.headers.get('x-zeplo-retry') ??
          req.headers.get('x-vqs-message-attempt') ??
          '1'
      );

      if (!queueName.startsWith(prefix)) {
        return Response.json({ error: 'Unhandled queue' }, { status: 400 });
      }

      let message: QueuePayload;
      try {
        const text = await req.text();
        message = JSON.parse(text) as QueuePayload;
      } catch (err) {
        return Response.json(
          { error: `bad body: ${String(err)}` },
          { status: 400 }
        );
      }

      let result: unknown;
      try {
        result = await handler(message, {
          attempt,
          queueName: ValidQueueName.parse(queueName),
          messageId: MessageId.parse(messageId),
          requestId: req.headers.get('x-request-id') ?? undefined,
        });
      } catch (err) {
        // Let Zeplo retry per its retry policy.
        return Response.json(String(err), { status: 500 });
      }

      const timeoutSeconds =
        typeof result === 'object' && result !== null
          ? (result as { timeoutSeconds?: unknown }).timeoutSeconds
          : undefined;
      if (typeof timeoutSeconds === 'number') {
        const seconds = Math.max(0, timeoutSeconds);
        if (seconds > 0) {
          // Re-enqueue with the requested delay. Zeplo doesn't have a native
          // "delay this retry" header, so we just push a fresh message.
          await queue(ValidQueueName.parse(queueName), message, {
            delaySeconds: seconds,
          }).catch((err) => {
            console.error(
              '[world-zeplo] failed to re-enqueue after timeoutSeconds:',
              err
            );
          });
        }
        return Response.json({ timeoutSeconds: seconds });
      }

      return Response.json({ ok: true });
    };
  };

  const getDeploymentId: Queue['getDeploymentId'] = async () =>
    process.env.WORKFLOW_DEPLOYMENT_ID ?? 'dpl_zeplo';

  return { queue, createQueueHandler, getDeploymentId };
}
