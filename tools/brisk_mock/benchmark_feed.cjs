'use strict';
// Fake broker transport for benchmark_feed.py; every HTTP response and frame is local.
const fs = require('node:fs'), path = require('node:path');
const { once } = require('node:events');
const { performance } = require('node:perf_hooks');
const root = path.resolve(__dirname, '../..');
const { live, readLanes } = require(path.join(root, 'briskapi/decoder/brisk.cjs'));
const { frames } = require(path.join(root, 'briskapi/decoder/decoder.cjs'));
const files = Object.fromEntries(['fita.js', 'fita.wasm', 'master.dat', 'snapshot.dat', 'ws.dat'].map(n =>
  [n, fs.readFileSync(path.join(process.env.BRISK_MOCK_CACHE, n))]));
const chunks = [...frames(files['ws.dat'])].slice(0, Number(process.env.BENCH_FRAMES || 600));
const speed = Number(process.env.BENCH_SPEED || 0);
const routes = {
  '/api/frontend/boot': () => JSON.stringify({ api_token: 'fake', identity: 'a'.repeat(64) }),
  '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=fake' }),
  '/': () => '<html><script src="/main.test.js"></script></html>',
  '/main.test.js': () => 'load("assets/wasm/fita.test.js");load("assets/wasm/fita.test.wasm")',
  '/assets/wasm/fita.test.js': () => files['fita.js'], '/assets/wasm/fita.test.wasm': () => files['fita.wasm'],
  '/api/master/m1': () => files['master.dat'], '/api/snapshot/s1': () => files['snapshot.dat'],
};
const fetchImpl = async url => new Response(routes[url.pathname] ? routes[url.pathname]() : '',
  { status: routes[url.pathname] ? 200 : 404 });
let socket;
class Socket extends EventTarget {
  constructor() {
    super(); socket = this; this.i = 0;
    setImmediate(() => { while (this.i < 3) this.deliver(); });
  }
  deliver() {
    const chunk = chunks[this.i++].data;
    this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
  }
  start() {
    if (this.started) return;
    this.started = true;
    const began = performance.now(), first = chunks[this.i].time;
    const pump = () => {
      if (this.closed) return;
      if (!speed) {
        const end = Math.min(chunks.length, this.i + 16);
        while (this.i < end) this.deliver();
      } else {
        while (this.i < chunks.length && (chunks[this.i].time - first) / speed <= performance.now() - began) this.deliver();
      }
      if (this.i === chunks.length) return this.close(1000);
      this.timer = speed ? setTimeout(pump, Math.max(0, (chunks[this.i].time - first) / speed - (performance.now() - began)))
        : setImmediate(pump);
    };
    pump();
  }
  send() {}
  close(code = 1000) {
    clearTimeout(this.timer); clearImmediate(this.timer);
    if (this.closed) return;
    this.closed = true;
    this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
  }
}
require('node:readline').createInterface({ input: process.stdin }).on('line', line => {
  try { if (JSON.parse(line).benchmark_start) socket.start(); } catch (error) { console.error(error); process.exitCode = 1; }
});
let peakRss = 0;
const sample = setInterval(() => { peakRss = Math.max(peakRss, process.memoryUsage().rss); }, 50);
live({ cookies: { fake: 'v' }, protocolVersion: 16000, fetchImpl, WebSocketImpl: Socket,
  staleSeconds: 0, heartbeatMs: 1e9, lanes: readLanes(process.stdin),
  emit: async (record, line = JSON.stringify(record)) => { if (!process.stdout.write(line + '\n')) await once(process.stdout, 'drain'); } })
  .then(summary => {
    clearInterval(sample);
    fs.writeFileSync(process.env.BENCH_STATS, JSON.stringify({ summary,
      source_span_ms: chunks.at(-1).time - chunks[3].time, frames: chunks.length,
      peak_node_rss_mib: peakRss / 2 ** 20 }));
    process.stdout.write('', () => process.exit(0));
  }, error => { clearInterval(sample); console.error(error); process.exit(1); });
