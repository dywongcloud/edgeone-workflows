import type {
  PubSub,
  RedisClient,
  SetOptions,
  ZRangeByScoreOptions,
} from './types.js';

interface Expiring {
  value: string;
  expireAt?: number;
}

function normalizeRange(
  len: number,
  start: number,
  stop: number
): [number, number] {
  let s = start < 0 ? len + start : start;
  let e = stop < 0 ? len + stop : stop;
  if (s < 0) s = 0;
  if (e >= len) e = len - 1;
  return [s, e];
}

/**
 * In-memory implementation of {@link RedisClient}. Faithful to the Redis
 * semantics world-redirect relies on (string values, NX/PX, sorted sets ordered
 * by score then member, list ranges, hashes, pub/sub, and the two Lua scripts world-redirect
 * runs: compare-and-del unlock and append-event).
 *
 * NOT durable — state lives in the process. Intended for tests and zero-setup
 * local development. Use a real Redis (node-redis / Upstash) in production.
 */
export class MemoryRedisClient implements RedisClient {
  readonly label = 'memory';
  private strings = new Map<string, Expiring>();
  private zsets = new Map<string, Map<string, number>>();
  private hashes = new Map<string, Map<string, string>>();
  private lists = new Map<string, string[]>();
  private subscribers = new Map<string, Set<(m: string) => void>>();

  private alive(key: string): boolean {
    const e = this.strings.get(key);
    if (!e) return false;
    if (e.expireAt != null && e.expireAt <= Date.now()) {
      this.strings.delete(key);
      return false;
    }
    return true;
  }

  async get(key: string): Promise<string | null> {
    return this.alive(key) ? this.strings.get(key)!.value : null;
  }

  async set(key: string, value: string, opts?: SetOptions): Promise<boolean> {
    if (opts?.nx && this.alive(key)) return false;
    this.strings.set(key, {
      value,
      expireAt: opts?.pxMs != null ? Date.now() + opts.pxMs : undefined,
    });
    return true;
  }

  async del(...keys: string[]): Promise<number> {
    let n = 0;
    for (const k of keys) {
      if (
        this.strings.delete(k) ||
        this.zsets.delete(k) ||
        this.hashes.delete(k) ||
        this.lists.delete(k)
      ) {
        n++;
      }
    }
    return n;
  }

  async incr(key: string): Promise<number> {
    const cur = this.alive(key) ? Number(this.strings.get(key)!.value) : 0;
    const next = (Number.isFinite(cur) ? cur : 0) + 1;
    this.strings.set(key, { value: String(next) });
    return next;
  }

  async pexpire(key: string, ms: number): Promise<void> {
    const e = this.strings.get(key);
    if (e) e.expireAt = Date.now() + ms;
  }

  private zset(key: string): Map<string, number> {
    let m = this.zsets.get(key);
    if (!m) {
      m = new Map();
      this.zsets.set(key, m);
    }
    return m;
  }

  private sortedMembers(key: string): string[] {
    const m = this.zsets.get(key);
    if (!m) return [];
    return [...m.entries()]
      .sort((a, b) => (a[1] === b[1] ? (a[0] < b[0] ? -1 : 1) : a[1] - b[1]))
      .map(([member]) => member);
  }

  async zadd(key: string, score: number, member: string): Promise<void> {
    this.zset(key).set(member, score);
  }

  async zrem(key: string, ...members: string[]): Promise<number> {
    const m = this.zsets.get(key);
    if (!m) return 0;
    let n = 0;
    for (const member of members) if (m.delete(member)) n++;
    if (m.size === 0) this.zsets.delete(key);
    return n;
  }

  async zcard(key: string): Promise<number> {
    return this.zsets.get(key)?.size ?? 0;
  }

  async zscore(key: string, member: string): Promise<number | null> {
    const s = this.zsets.get(key)?.get(member);
    return s == null ? null : s;
  }

  async zrangeByScore(
    key: string,
    min: number,
    max: number,
    opts?: ZRangeByScoreOptions
  ): Promise<string[]> {
    const m = this.zsets.get(key);
    if (!m) return [];
    let entries = [...m.entries()]
      .filter(([, s]) => s >= min && s <= max)
      .sort((a, b) => (a[1] === b[1] ? (a[0] < b[0] ? -1 : 1) : a[1] - b[1]))
      .map(([member]) => member);
    if (opts?.count != null) {
      const offset = opts.offset ?? 0;
      entries = entries.slice(offset, offset + opts.count);
    }
    return entries;
  }

  async zrangeByRank(
    key: string,
    start: number,
    stop: number,
    rev?: boolean
  ): Promise<string[]> {
    let members = this.sortedMembers(key);
    if (rev) members = members.reverse();
    const [s, e] = normalizeRange(members.length, start, stop);
    if (s > e) return [];
    return members.slice(s, e + 1);
  }

  private hash(key: string): Map<string, string> {
    let m = this.hashes.get(key);
    if (!m) {
      m = new Map();
      this.hashes.set(key, m);
    }
    return m;
  }

  async hset(key: string, field: string, value: string): Promise<void> {
    this.hash(key).set(field, value);
  }

  async hsetMany(key: string, entries: Record<string, string>): Promise<void> {
    const m = this.hash(key);
    for (const [f, v] of Object.entries(entries)) m.set(f, v);
  }

  async hget(key: string, field: string): Promise<string | null> {
    return this.hashes.get(key)?.get(field) ?? null;
  }

  async hgetAll(key: string): Promise<Record<string, string>> {
    const m = this.hashes.get(key);
    return m ? Object.fromEntries(m) : {};
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    const m = this.hashes.get(key);
    if (!m) return 0;
    let n = 0;
    for (const f of fields) if (m.delete(f)) n++;
    if (m.size === 0) this.hashes.delete(key);
    return n;
  }

  async hlen(key: string): Promise<number> {
    return this.hashes.get(key)?.size ?? 0;
  }

  private list(key: string): string[] {
    let l = this.lists.get(key);
    if (!l) {
      l = [];
      this.lists.set(key, l);
    }
    return l;
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    const l = this.list(key);
    l.push(...values);
    return l.length;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const l = this.lists.get(key);
    if (!l) return [];
    const [s, e] = normalizeRange(l.length, start, stop);
    if (s > e) return [];
    return l.slice(s, e + 1);
  }

  async llen(key: string): Promise<number> {
    return this.lists.get(key)?.length ?? 0;
  }

  async eval(script: string, keys: string[], args: string[]): Promise<unknown> {
    // Emulate the scripts world-redirect runs. Each runs synchronously
    // between awaits, which gives it the same atomicity Redis gives a script.
    if (script.startsWith('-- world-redirect:append-event')) {
      const [listKey, hashKey, corrKey] = keys as [string, string, string];
      const [prefix, digits, blob, hasCorrelationId] = args as [
        string,
        string,
        string,
        string,
      ];
      const list = this.lists.get(listKey) ?? [];
      const slot = String(list.length + 1);
      const id = prefix + slot.padStart(Number(digits), '0');
      const hash = this.hashes.get(hashKey) ?? new Map<string, string>();
      hash.set(id, blob);
      this.hashes.set(hashKey, hash);
      list.push(id);
      this.lists.set(listKey, list);
      if (hasCorrelationId === '1') {
        const corr = this.lists.get(corrKey) ?? [];
        corr.push(id);
        this.lists.set(corrKey, corr);
      }
      return id;
    }
    if (script.includes("redis.call('del'") && script.includes("redis.call('get'")) {
      const key = keys[0]!;
      if (this.alive(key) && this.strings.get(key)!.value === args[0]) {
        this.strings.delete(key);
        return 1;
      }
      return 0;
    }
    throw new Error('[MemoryRedisClient] unsupported eval script');
  }

  get pubsub(): PubSub {
    return {
      publish: async (channel, message) => {
        const subs = this.subscribers.get(channel);
        if (subs) for (const h of subs) queueMicrotask(() => h(message));
      },
      subscribe: async (channel, handler) => {
        let set = this.subscribers.get(channel);
        if (!set) {
          set = new Set();
          this.subscribers.set(channel, set);
        }
        set.add(handler);
        return async () => {
          set!.delete(handler);
        };
      },
    };
  }

  async close(): Promise<void> {
    this.strings.clear();
    this.zsets.clear();
    this.hashes.clear();
    this.lists.clear();
    this.subscribers.clear();
  }
}
