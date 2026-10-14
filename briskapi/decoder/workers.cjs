'use strict';
// Quote extraction in parallel. Reading a quote's book costs the decoder far more than decoding its frame
// (on the 2021 demo the whole day decodes in about a second, while reading its quotes takes about 20 µs
// each), and in the opening auction the whole market changes at once. Each worker thread runs a decoder of
// its own, fed every frame the host's decoder is fed, in the same order, and reads the quotes of its share of
// the issues (issue_id % count), which it hands back as JSON. The host's own decoder keeps everything else:
// the stream's callbacks (ping replies, heartbeats), its clock, the changed issues, the trades.
const { Worker, isMainThread, parentPort, workerData } = require('node:worker_threads');
const os = require('node:os');
const { Decoder } = require('./decoder.cjs');

// Below this many selected issues the host reads its quotes itself.
const POOL_MIN_ISSUES = 500;

// How many workers suit this computer: its cores less four (for the Python feed's reading, parsing and
// writing), from two to twelve. On an M5 Pro six take the demo's whole-market opening through the feed faster
// than a live open sends it (about 130,000 quote updates a second); slower cores need more. Each holds a decoder
// of about 270 MB.
function workerCount(env = process.env) {
  if (env.BRISK_WORKERS !== undefined && env.BRISK_WORKERS !== '') {
    const count = Number(env.BRISK_WORKERS);
    if (!Number.isInteger(count) || count < 0) throw new Error('BRISK_WORKERS must be a whole number (0 turns the workers off)');
    return count;
  }
  return Math.min(12, Math.max(2, os.availableParallelism() - 4));
}

class QuotePool {
  // assets: the decoder's files; options: what the host's decoder was created with (protocolVersion,
  // schedule, boot), as plain data; ids: the selected issue IDs.
  static async start({ assets, options, ids, count }) {
    const pool = new QuotePool(count);
    const shared = { assets, options, ids: [...ids], count };
    pool.workers = Array.from({ length: count }, (_, index) => {
      const worker = new Worker(__filename, { workerData: { ...shared, index, quoteWorker: true } });
      worker.unref();   // a host that ends never waits on its workers
      return worker;
    });
    try {
      await Promise.all(pool.workers.map((worker, index) => new Promise((resolve, reject) => {
        const failed = error => { pool.fail(error); reject(error); };
        worker.on('message', message => pool.receive(index, message, resolve, failed));
        worker.once('error', failed);
        worker.once('exit', code => failed(new Error(`A quote worker stopped (exit code ${code})`)));
      })));
    } catch (error) {
      pool.close();
      throw error;
    }
    return pool;
  }

  constructor(count) {
    this.count = count;
    this.pending = [];   // per extracted frame, in order: { results, waiting, resolve, reject }
    this.failure = null;
    this.closed = false;
  }

  receive(index, message, ready, failed) {
    if (message.ready) return ready();
    if (message.error) return failed(new Error(message.error));
    const frame = this.pending.find(p => !p.results[index]);
    if (!frame) return this.fail(new Error('A quote worker answered a frame it was not given'));
    frame.results[index] = message;
    if (--frame.waiting === 0) {
      this.pending.splice(this.pending.indexOf(frame), 1);
      frame.resolve(frame.results);
    }
  }

  // Every frame still waiting fails, and so does anything posted from now on.
  fail(error) {
    if (this.failure || this.closed) return;
    this.failure = error;
    for (const frame of this.pending.splice(0)) frame.reject(error);
  }

  post(message) {
    if (this.failure) throw this.failure;
    for (const worker of this.workers) worker.postMessage(message);
  }

  // Every frame the host's decoder is fed, in the same order. With `changed` (the issues the host's decoder
  // traced for the frame, in its order, as selected), the workers also read those quotes; the result is
  // { quotes: their JSON, in that order, skipped, decode_ns: the longest worker's }. `only` instead names the
  // changed issues to read (the fast lane's, see brisk.cjs), which each worker checks against its own trace.
  feed(frame, changed = null, { only = false } = {}) {
    this.post({ op: 'feed', frame, extract: changed !== null, ...(only && { ids: changed }) });
    return changed === null ? null : this.expect(changed);
  }

  // The quotes of `ids` as they are now (after the last frame fed), changed or not.
  join(ids) {
    this.post({ op: 'join', ids });
    return this.expect(ids);
  }

  expect(ids) {
    return new Promise((resolve, reject) => {
      this.pending.push({ results: new Array(this.count), waiting: this.count, resolve, reject });
    }).then(results => merge(ids, results, this.count));
  }

  catchUp(reply, lagging) { this.post({ op: 'catchUp', reply, lagging }); }

  // begin(), then the bootstrap's read of `ids` (each worker reads its share, as the host's decoder read them).
  begin(ids) { this.post({ op: 'begin', ids }); }

  close() {
    this.closed = true;
    for (const worker of this.workers || []) worker.terminate();
  }
}

// The workers' quotes in the host's order. Each worker must have traced exactly its share of the host's
// changed issues: anything else means its decoder is no longer in the host's state.
function merge(changed, results, count) {
  const cursor = new Array(count).fill(0);
  for (let k = 0; k < count; k++) {
    const expected = changed.filter(id => id % count === k);
    const got = results[k].ids;
    if (got.length !== expected.length || got.some((id, i) => id !== expected[i])) {
      throw new Error(`Quote worker ${k} decoded other issues than the host (${got.length} against ${expected.length}): `
        + 'its decoder has diverged');
    }
  }
  const quotes = [], skipped = [];
  for (const id of changed) {
    const k = id % count;
    const quote = results[k].quotes[cursor[k]++];
    if (quote === null) skipped.push(id); else quotes.push(quote);
  }
  return { quotes, skipped, decode_ns: Math.max(...results.map(r => r.decode_ns)) };
}

const view = data => Buffer.from(data.buffer, data.byteOffset, data.byteLength);   // a cloned Buffer arrives as a Uint8Array

// The worker: a decoder replaying the host's operations.
async function work() {
  const { assets, options, ids, count, index } = workerData;
  const selected = new Set(ids);
  const mine = id => selected.has(id) && id % count === index;
  const queue = [];
  let decoder = null, running = false;
  const read = (share, began) => {
    const { quotes } = decoder.quotes(share);
    const byIssue = new Map(quotes.map(q => [q.issue_id, q]));
    const json = share.map(id => (byIssue.has(id) ? JSON.stringify(byIssue.get(id)) : null));
    parentPort.postMessage({ ids: share, quotes: json, decode_ns: Number(process.hrtime.bigint() - began) });
  };
  const step = message => {
    if (message.op === 'feed') {
      const began = process.hrtime.bigint();
      decoder.feed(view(message.frame));
      decoder.applyBasePrice();
      if (!decoder.began) return;
      const traced = decoder.changed();
      decoder.qrm.take();   // the host's decoder reports the trades
      if (!message.extract) return;
      let share = traced.filter(mine);
      if (message.ids) {
        const changed = new Set(share);
        share = message.ids.filter(mine);
        if (share.some(id => !changed.has(id))) throw new Error('the host named issues this decoder did not trace: it has diverged');
      }
      read(share, began);
    } else if (message.op === 'join') {
      read(message.ids.filter(mine), process.hrtime.bigint());
    } else if (message.op === 'catchUp') {
      decoder.catchUp(view(message.reply), message.lagging);
    } else if (message.op === 'begin') {
      decoder.begin();
      decoder.quotes(message.ids.filter(mine));
    }
  };
  const drain = () => {
    if (running || !decoder) return;
    running = true;
    try {
      while (queue.length) step(queue.shift());
    } catch (error) {
      parentPort.postMessage({ error: `Quote worker ${index}: ${error.message}` });
      queue.length = 0;
      decoder = null;   // nothing more is read from a decoder that failed
    } finally { running = false; }
  };
  // Listen at once: the host feeds frames while the decoder loads.
  parentPort.on('message', message => { queue.push(message); drain(); });
  const files = Object.fromEntries(Object.entries(assets).map(([name, data]) => [name, view(data)]));
  decoder = await Decoder.create(files, { ...options, callbacks: {} });
  decoder.restored = [];   // the host's decoder reports what the snapshot restored
  parentPort.postMessage({ ready: true });
  drain();
}

if (!isMainThread && workerData?.quoteWorker === true) {
  work().catch(error => parentPort.postMessage({ error: `Quote worker ${workerData.index}: ${error.message}` }));
}

module.exports = { QuotePool, workerCount, POOL_MIN_ISSUES, merge };
