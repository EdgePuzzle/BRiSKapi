'use strict';
// How much memory a reconnect costs the live host: the demo's whole market recorded with quote workers and the fast lane, the
// link dropped (1006) DROPS times, the process's memory sampled every 25 ms, labelled by session, and at each reconnect's end.
//
//   BRISK_MOCK_CACHE=DIR DROPS=3 FRAMES=300 RESTORED=2500000 \
//     NODE_OPTIONS="--require ./tools/brisk_mock/restored_trades.cjs" node tools/brisk_mock/reconnect_memory.cjs
//
// RESTORED (with restored_trades.cjs preloaded, which the workers must share) is how many trades each decoder restores from
// the snapshot, as in a real reconnect during the day: 2.5 million took the host to 8 GB before the quote workers stopped
// keeping trade records (4 GB after), and left 4-6 GB behind between sessions (2 GB after). BRISK_WORKERS=0 measures the
// main decoder alone.
const fs = require('node:fs'), path = require('node:path');
const { record } = require('../../briskapi/decoder/brisk.cjs');
const { frames } = require('../../briskapi/decoder/decoder.cjs');
const cache = process.env.BRISK_MOCK_CACHE;
const drops = Number(process.env.DROPS || 5), frameCount = Number(process.env.FRAMES || 1500);
const files = Object.fromEntries(['fita.js', 'fita.wasm', 'master.dat', 'snapshot.dat', 'ws.dat'].map(n => [n, fs.readFileSync(path.join(cache, n))]));
const chunks = [...frames(files['ws.dat'])].slice(0, frameCount).map(f => f.data);
const routes = {
  '/api/frontend/boot': () => JSON.stringify({ api_token: 't', identity: 'a'.repeat(64) }),
  '/api/app/market-token': () => JSON.stringify({ token: 'v2.local.X' }),
  '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc' }),
  '/': () => '<html><script src="/main.abc.js"></script></html>',
  '/main.abc.js': () => 'load("assets/wasm/fita.test.js");load("assets/wasm/fita.test.wasm")',
  '/assets/wasm/fita.test.js': () => files['fita.js'], '/assets/wasm/fita.test.wasm': () => files['fita.wasm'],
  '/api/master/m1': () => files['master.dat'], '/api/snapshot/s1': () => files['snapshot.dat'],
};
const fetchImpl = async url => new Response(routes[url.pathname] ? routes[url.pathname]() : '', { status: routes[url.pathname] ? 200 : 404 });
let sockets = 0;
const WebSocketImpl = class extends EventTarget {
  constructor() {
    super();
    const last = sockets++ >= drops;
    setImmediate(() => {
      this.dispatchEvent(new MessageEvent('message', { data: 'hello' }));
      for (const c of chunks) this.dispatchEvent(new MessageEvent('message', { data: c.buffer.slice(c.byteOffset, c.byteOffset + c.length) }));
      setImmediate(() => this.close(last ? 1000 : 1006));
    });
  }
  send() {}
  close(code = 1000) { if (this.closed) return; this.closed = true; this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' })); }
};
const mb = n => Math.round(n / 2 ** 20);
let session = -1;
const peak = [];       // session index -> max rss
const marks = [];      // labelled readings
const take = label => { const m = process.memoryUsage(); marks.push(`${label}: rss ${mb(m.rss)} MB (external ${mb(m.external)}, arrayBuffers ${mb(m.arrayBuffers)}, heap ${mb(m.heapUsed)})`); };
const timer = setInterval(() => { const r = process.memoryUsage().rss; peak[session] = Math.max(peak[session] ?? 0, r); }, 25);
const emit = async b => { if (b.type === 'bootstrap') { session++; take(`session ${session} starts`); } if (b.type === 'history') JSON.stringify(b); };   // as the real emit writes it
const lanes = { codes: new Set(), onChange: null };
record({ days: 1, cookies: { s: 'v' }, codes: [], emit, fetchImpl, WebSocketImpl, protocolVersion: 16000, lanes, log: () => {},
  sleep: async () => {
    take(`session ${session} over (before the wait)`);
    await new Promise(r => setTimeout(r, 300));    // the real wait is 5 s and more
    take(`session ${session} after a 0.3 s wait`);
  } }).then(() => {
  clearInterval(timer); take('done');
  console.log(marks.join('\n'));
  console.log('peak rss by session (MB):', peak.map(mb).join(', '), '| baseline = first session');
}, e => { clearInterval(timer); console.error(e); process.exit(1); });
