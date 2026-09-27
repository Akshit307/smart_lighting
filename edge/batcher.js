// edge/batcher.js
// Edge-side event aggregator (HD research implementation).
//
// Coalesces individual device events into a single "batch envelope" before they
// leave the edge. An envelope is flushed when EITHER:
//   - it reaches maxBatch events (size trigger), or
//   - the oldest event in it has waited maxWaitMs (time trigger), or
//   - an urgent event arrives (e.g. a manual switch override), which flushes
//     immediately so user-facing actions never wait for the time window.
//
// One envelope = one MQTT publish / one SQS message, so the number of messages
// the cloud has to poll and invoke Lambda for drops by up to maxBatch times.

class EdgeBatcher {
  /**
   * @param {object}   opts
   * @param {number}   opts.maxBatch   max events per envelope (default 25 = DynamoDB BatchWriteItem limit)
   * @param {number}   opts.maxWaitMs  max time an event may wait at the edge (default 100ms)
   * @param {string}   opts.gatewayId  identifies the gateway in each envelope
   * @param {function} opts.sink       async (envelope, groupKey) => void — sends the envelope upstream
   * @param {function} [opts.groupBy]  event => key; events with different keys never share an envelope
   * @param {function} [opts.isUrgent] event => boolean; urgent events trigger an immediate flush
   */
  constructor({ maxBatch = 25, maxWaitMs = 100, gatewayId = 'gateway', sink, groupBy, isUrgent }) {
    if (typeof sink !== 'function') throw new Error('EdgeBatcher needs a sink function');
    this.maxBatch = maxBatch;
    this.maxWaitMs = maxWaitMs;
    this.gatewayId = gatewayId;
    this.sink = sink;
    this.groupBy = groupBy || (() => 'default');
    this.isUrgent = isUrgent || (() => false);

    this.buffers = new Map(); // groupKey -> { events: [], timer }
    this.pending = new Set(); // in-flight sink promises
    this.seq = 0;
    this.stats = { eventsIn: 0, envelopesOut: 0, sizeFlushes: 0, timeFlushes: 0, urgentFlushes: 0, manualFlushes: 0, sinkErrors: 0 };
  }

  add(event) {
    this.stats.eventsIn++;
    const key = this.groupBy(event);
    let buf = this.buffers.get(key);
    if (!buf) {
      buf = { events: [], timer: null };
      this.buffers.set(key, buf);
    }
    buf.events.push(event);

    if (this.isUrgent(event)) return this._flush(key, 'urgent');
    if (buf.events.length >= this.maxBatch) return this._flush(key, 'size');
    if (!buf.timer) {
      buf.timer = setTimeout(() => this._flush(key, 'time'), this.maxWaitMs);
    }
    return null;
  }

  _flush(key, reason) {
    const buf = this.buffers.get(key);
    if (!buf || buf.events.length === 0) return null;
    if (buf.timer) clearTimeout(buf.timer);
    this.buffers.delete(key);

    const envelope = {
      type: 'batch',
      gatewayId: this.gatewayId,
      seq: this.seq++,
      batchedAt: Date.now(),
      flushReason: reason,
      count: buf.events.length,
      events: buf.events
    };

    this.stats.envelopesOut++;
    this.stats[`${reason}Flushes`]++;

    const p = Promise.resolve()
      .then(() => this.sink(envelope, key))
      .catch(err => {
        this.stats.sinkErrors++;
        console.error(`[EdgeBatcher] sink failed for envelope ${envelope.seq} (${envelope.count} events):`, err.message || err);
      })
      .finally(() => this.pending.delete(p));
    this.pending.add(p);
    return p;
  }

  // Flush everything still buffered and wait for all sends to finish.
  async drain() {
    for (const key of [...this.buffers.keys()]) this._flush(key, 'manual');
    await Promise.all([...this.pending]);
  }
}

module.exports = { EdgeBatcher };
