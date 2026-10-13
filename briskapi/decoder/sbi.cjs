'use strict';
// Experimental live host for broker-hosted BRiSK: SBI BRiSK (sbi.brisk.jp) and
// Tachibana's e-shiten BRiSK Next (next.e-shiten.brisk.jp), BRISK_SBI_SITE=e-shiten.
// The session's own WASM decoder under Node, fed from the authenticated WebSocket.
// No browser or Chrome DevTools. Emits the same JSON batches as decoder.cjs
// (source=sbi_live or eshiten_live). Cookies come from BRISK_SBI_COOKIES (JSON
// object), never from the command line.
//
// It follows the vendor client's connect sequence (public Next demo bundle) and
// upstream's SBI research (pybrisk): feed the WASM from the first frame, wait for its
// frame numbers, catch the snapshot up to them, answer the server's pings through the WASM,
// and watch the server heartbeat. The stream is expected to be plain binary (see engineio.cjs
// for the Socket.IO fallback and why it is unverified); the profile (BRISK_SBI_PROFILE) can
// set that fallback's parameters and the catch-up format, and `--trace-protocol` shows what
// the server answers.
const crypto = require('node:crypto');
const { once } = require('node:events');
const { Decoder } = require('./decoder.cjs');
const { Link, resolveProfile, heartbeatMonitor, staleDataCheck, redact } = require('./engineio.cjs');

// Brokers whose BRiSK is this app. On 2026-10-08 both served the same boot (flex_version 18000),
// master, snapshot and decoder, so only the address differs.
const SITES = {
  sbi: { origin: 'https://sbi.brisk.jp', name: 'SBI BRiSK', source: 'sbi_live', transport: 'sbi_websocket' },
  'e-shiten': { origin: 'https://next.e-shiten.brisk.jp', name: 'e-shiten BRiSK Next', source: 'eshiten_live',
    transport: 'eshiten_websocket' },
};
const PROTOCOL_VERSION = 18000;
const MAX_BYTES = 64 * 1024 * 1024;
// SBI's decoder must provide everything the host calls (the demo ABI minus portfolio).
const REQUIRED = ['_initialize', '_push', '_applyBasePriceQueue', '_stockCount', '_getStockMaster', '_unserialize',
  '_getDate', '_getTime', '_pushWs', '_getFrameNumbers', '_apiRecieved', '_addTraceUpdate', '_getTrace',
  '_clearTrace', '_getStockView', '_fitItaViewRowPrice10', '_getItaRows', '_malloc', 'addFunction'];

function site(id = 'sbi') {
  if (!Object.hasOwn(SITES, id)) throw new Error(`Unknown BRiSK site ${JSON.stringify(id)}; use ${Object.keys(SITES).join(' or ')}`);
  return { id, ...SITES[id] };
}

class Session {
  constructor(cookies, fetchImpl = fetch, where = site()) {
    if (!cookies || typeof cookies !== 'object' || !Object.keys(cookies).length) {
      throw new Error(`Set BRISK_SBI_COOKIES to your ${where.name} session cookies (JSON object)`);
    }
    this.cookie = Object.entries(cookies).map(([k, v]) => `${k}=${v}`).join('; ');
    this.fetch = fetchImpl;
    this.origin = where.origin;
    this.name = where.name;
    this.token = null;
  }

  get(path, kind = 'json') {
    return this.request(path, { kind });
  }

  async request(path, { method = 'GET', body, contentType, kind = 'json' } = {}) {
    const headers = { cookie: this.cookie };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (contentType) headers['content-type'] = contentType;
    // Redirects mean a login page; following them would forward credentials elsewhere.
    const response = await this.fetch(new URL(path, this.origin), { method, headers, body, redirect: 'manual', signal: AbortSignal.timeout(30000) });
    if ([301, 302, 303, 307, 308, 401, 403].includes(response.status) || response.type === 'opaqueredirect') {
      throw new Error(`${this.name} session expired or invalid; log in again`);
    }
    if (!response.ok) {
      const error = new Error(`${this.name} ${path}: HTTP ${response.status}`);
      error.status = response.status;
      error.reason = response.headers?.get?.('x-error-reason') ?? null;
      throw error;
    }
    const data = Buffer.from(await response.arrayBuffer());
    if (data.length > MAX_BYTES) throw new Error(`${this.name} ${path}: response too large`);
    return kind === 'json' ? JSON.parse(data) : kind === 'text' ? data.toString('utf8') : data;
  }
}

// How the catch-up request is written. The vendor client's, as pybrisk's public capture of
// sbi.brisk.jp (March 2026) recorded it: one JSON object of three parallel lists, each issue's
// update numbers from the snapshot's up to the stream's first. Its binary reply is pushed into
// the decoder as it comes. A profile can still name another format (catchUp).
const CATCH_UP_FORMATS = {
  'issue-lists': issues => ({ contentType: 'application/json',
    body: JSON.stringify({ issue_ids: issues.map(i => i.issue_id), update_numbers_from: issues.map(i => i.from),
      update_numbers_to: issues.map(i => i.to) }) }),
};
const DEFAULT_CATCH_UP = 'issue-lists';

// Fetch the frames between the snapshot and the stream's first frame. Mirrors the vendor
// client: up to five retries, but never for an expired session or a snapshot that is too old.
async function fetchCatchUp(session, { app, issues, format = DEFAULT_CATCH_UP, retries = 5, backoffMs = n => 1000 + 1000 * n, trace }) {
  const build = CATCH_UP_FORMATS[format];
  if (!build) {
    throw new Error(`The snapshot is ${issues.length} issues behind the stream, so it must be caught up with `
      + `POST /api/stocks_update, but "${format}" is not a known format (known: ${Object.keys(CATCH_UP_FORMATS).join(', ')})`);
  }
  const { contentType, body } = build(issues);
  const path = `/api/stocks_update/${encodeURIComponent(app.series)}?date=${encodeURIComponent(app.date)}`;
  for (let attempt = 0; ; attempt++) {
    try {
      trace?.(`catch-up: POST ${path} (${issues.length} issues, ${body.length} bytes, format ${format})`);
      return await session.request(path, { method: 'POST', body, contentType, kind: 'bytes' });
    } catch (error) {
      if (error.reason === 'too-old') {
        throw new Error(`${session.name} says the snapshot is too old to catch up (x-error-reason: too-old); restart to load a fresh one`);
      }
      if (/session expired/.test(error.message) || attempt >= retries) {
        throw new Error(`Catch-up failed: ${error.message}${error.status ? ` (the server rejected format "${format}")` : ''}`);
      }
      trace?.(`catch-up attempt ${attempt + 1} failed: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, backoffMs(attempt)));
    }
  }
}

// The decoder is served to logged-in sessions only and changes with releases, so it is
// located through the app's own bundles rather than pinned. The page may name its bundles by
// absolute or relative path (e-shiten: `main-XXXX.js`). A bundle names the loader either as a
// literal path or, in both sites' builds of 2026-10-08, as a template with an exchange variant,
// `./assets/wasm${r}/fita.<hash>.js` (r is '-fukuoka' for the Fukuoka exchange); only the Tokyo
// build is used. Without a literal .wasm path, its name follows the loader's (Emscripten's rule).
async function decoderAssets(session) {
  const page = await session.get('/', 'text');
  const scripts = [...page.matchAll(/<script\b[^>]*\ssrc=["']([^"']+\.js)["']/g)]
    .map(m => new URL(m[1], session.origin + '/'))
    .filter(url => url.origin === session.origin)  // never send the session to another host
    .map(url => url.pathname);
  for (const script of scripts) {
    const source = await session.get(script, 'text');
    const js = source.match(/["'`](?:\.?\/)?assets\/wasm(?:\$\{[\w$.]+\})?\/(fita[\w.-]*)\.js["'`]/);
    if (!js) continue;
    const wasm = source.match(/["'`](?:\.?\/)?assets\/wasm\/(fita[\w.-]*)\.wasm["'`]/);
    const paths = [`/assets/wasm/${js[1]}.js`, `/assets/wasm/${wasm ? wasm[1] : js[1]}.wasm`];
    const assets = { 'fita.js': await session.get(paths[0], 'bytes'), 'fita.wasm': await session.get(paths[1], 'bytes') };
    return { assets, paths };
  }
  throw new Error(`${session.name} decoder not found in the app bundles; the site layout may have changed`);
}

// Where the stream is. The boot's ws_url is relative, and the browser resolves it against the
// API endpoint the frontend boot names (https://api.brisk.jp on e-shiten), not against the site
// that served the page. ws_url carries the session, so only a BRiSK host may receive it.
function streamUrl(wsUrl, apiEndpoint, origin) {
  const base = typeof apiEndpoint === 'string' && apiEndpoint.startsWith('https://') ? apiEndpoint : origin;
  const url = new URL(wsUrl, base.replace(/^https:/, 'wss:'));
  if (url.protocol !== 'wss:' || !(url.hostname === 'brisk.jp' || url.hostname.endsWith('.brisk.jp'))) {
    throw new Error(`Refusing to stream from ${url.protocol}//${url.host}: not a BRiSK host`);
  }
  return url;
}

// The master's record layout also depends on the protocol version, and 18000's is not independently
// confirmed. A wrong layout would read tick types as codes, limits as lot sizes and so on, so
// refuse a master that does not look like a market. (The 2021 demo's passes every test at 99.98%.)
// The decoder runs this as soon as it has read the master, before boot data is matched to it.
function checkMaster(master, protocolVersion) {
  const fraction = predicate => master.filter(predicate).length / (master.length || 1);
  const problems = [];
  if (master.length < 1000) problems.push(`only ${master.length} issues`);
  const rules = [
    // Four digits, five for some preferred shares, or since 2024 letters too (167A).
    ['security codes', 0.95, m => /^[0-9][0-9A-Z]{3,4}$/.test(m.code)],
    ['positive base prices', 0.98, m => m.base_price10 > 0],
    ['limits around the base price', 0.98, m => m.limit_up10 > m.base_price10 && m.base_price10 > m.limit_down10],
    ['lot sizes of 1 to 1,000,000', 0.98, m => m.lot_size >= 1 && m.lot_size <= 1e6],
    ['names', 0.95, m => m.name.length > 0],
  ];
  for (const [what, minimum, predicate] of rules) {
    const seen = fraction(predicate);
    if (seen < minimum) problems.push(`${what}: ${(100 * seen).toFixed(1)}% (need ${(100 * minimum).toFixed(0)}%)`);
  }
  const distinct = new Set(master.map(m => m.code)).size / (master.length || 1);
  if (distinct < 0.99) problems.push(`distinct codes: ${(100 * distinct).toFixed(1)}% (need 99%)`);
  if (problems.length) {
    throw new Error(`BRiSK stock master layout not recognised for protocol ${protocolVersion}: ${problems.join('; ')}`);
  }
}

function checkAbi(wasm) {
  const missing = REQUIRED.filter(name => typeof wasm[name] !== 'function');
  if (missing.length) throw new Error(`BRiSK decoder changed; missing ${missing.join(', ')}`);
}

async function live({ cookies, codes = [], emit, fetchImpl = fetch, WebSocketImpl = WebSocket, startTimeoutMs = 60000,
  protocolVersion, profile: overrides = {}, trace = null, catchUp = null, heartbeatMs = 7000,
  staleSeconds = 90, connectTimeoutMs = 15000, catchUpBackoffMs, site: siteId = 'sbi' }) {
  const where = site(siteId);
  const profile = resolveProfile(overrides);
  const session = new Session(cookies, fetchImpl, where);
  const began = performance.now();
  const frontend = await session.get('/api/frontend/boot');
  session.token = frontend.api_token;
  const app = await session.get('/api/app/boot');
  // The server says which decoder protocol it speaks (the vendor client passes it to the WASM and
  // falls back to 16000); an explicit option wins, then the boot response, then SBI's 18000.
  protocolVersion ??= app.flex_version ?? PROTOCOL_VERSION;
  const wsUrl = streamUrl(app.ws_url, frontend.api_endpoint, where.origin);
  trace?.(`boot: date ${app.date}, series ${app.series}, flex_version ${protocolVersion}, `
    + `${(app.base_prices || []).length} base prices, ${(app.exceptional_sq || []).length} exceptional special quotes, `
    + `ws_url ${redact(app.ws_url)} on ${wsUrl.host}`);
  const { assets, paths } = await decoderAssets(session);
  assets['master.dat'] = await session.get(`/api/master/${encodeURIComponent(app.master)}`, 'bytes');
  assets['snapshot.dat'] = await session.get(`/api/snapshot/${encodeURIComponent(app.snapshot)}`, 'bytes');

  // The WASM calls back into the host; these are the vendor client's six callbacks.
  let link = null, failure = null, marketFinished = false, settle;
  const ended = new Promise(resolve => { settle = resolve; });
  const fail = error => {
    failure = failure || error;
    link?.close();
    settle({ code: -1, reason: '' });
  };
  let monitor = null, timer = null;  // started with the link and always stopped, so a failure cannot leave the process hanging
  const stale = staleSeconds ? staleDataCheck({ date: app.date, thresholdUs: staleSeconds * 1e6 }) : null;
  const decoder = await Decoder.create(assets, { protocolVersion, check: checkAbi,
    checkMaster: master => checkMaster(master, protocolVersion), schedule: app.schedule_info ?? null,
    boot: { exceptional_sq: app.exceptional_sq, base_prices: app.base_prices }, callbacks: {
    send: bytes => { if (link) link.send(bytes); },
    heartbeat: ns => { monitor?.beat(); stale?.beat(ns); },
    authError: () => fail(new Error(`${where.name} reports another WebSocket session for this user (only one is allowed); close the other one`)),
    marketFinished: () => { marketFinished = true; },
  } });
  trace?.(`master: ${decoder.master.length} issues, ${decoder.layout.size}-byte records, agreeing with the master file`
    + (decoder.boot.skipped_exceptional_sq ? `; special quotes not sent for ${decoder.boot.skipped_exceptional_sq.join(',')} (not in the master)` : '')
    + (decoder.scheduled ? `; schedule applied (${decoder.ohlc} price bars)` : ''));
  const selected = new Set(codes);
  const master = decoder.master.filter(m => !selected.size || selected.has(m.code));
  const missing = codes.filter(code => !master.some(m => m.code === code));
  if (missing.length) throw new Error(`Codes not in the ${where.name} master: ${missing.join(',')}`);
  const ids = new Set(master.map(m => m.issue_id));
  const input_transport = { kind: where.transport, origin: where.origin + '/', ws_host: wsUrl.host, series: app.series,
    protocol_version: protocolVersion, boot_applied: decoder.boot,
    decoder: paths, decoder_sha256: crypto.createHash('sha256').update(assets['fita.wasm']).digest('hex'),
    setup_ms: performance.now() - began };

  const requestCatchUp = catchUp || (issues => fetchCatchUp(session, { app, issues, format: profile.catchUp ?? DEFAULT_CATCH_UP,
    backoffMs: catchUpBackoffMs, trace }));
  const wsSession = wsUrl.searchParams.get('session');
  // Only the Socket.IO fallback needs these; a plain WebSocket never asks for the market token.
  const context = async () => {
    let marketToken;
    try { marketToken = (await session.get('/api/app/market-token')).token; } catch (error) {
      throw new Error(`app/market-token failed: ${error.message}`);
    }
    return { marketToken, identity: frontend.identity, wsSession, tabId: crypto.randomUUID(), origin: where.origin };
  };

  let seq = 0, frames = 0, updates = 0, started = false, starting = false, work = Promise.resolve();
  const start = performance.now();

  const finishStart = caughtUp => {
    clearTimeout(timer);
    decoder.begin();
    const now = decoder.time();
    const { quotes, skipped } = decoder.quotes(master.map(m => m.issue_id));
    // The stock view layout is unverified for the broker build: refuse implausible values.
    if (!quotes.length || quotes.some(q => q.frame > q.max_frame || q.source_time_us > now)) {
      throw new Error(`${where.name} stock view layout not recognised (ohlcLength=${decoder.ohlc})`);
    }
    if (skipped.length) trace?.(`unreadable (skipped until they update): ${skipped.map(id => decoder.master[id].code).join(',')}`);
    started = true;
    const batch = { type: 'bootstrap', seq: seq++, source: where.source, trading_date: String(app.date).replaceAll('-', ''),
      input_transport: { ...input_transport, dialect: link.mode, engineio: link.version, caught_up_issues: caughtUp },
      source_timestamp_origin: 'brisk_decoder_unverified', exchange_delay_ms: null,
      source_time_us: now, market_issue_count: decoder.master.length, master, quotes,
      ...(skipped.length && { skipped_issues: skipped.sort((a, b) => a - b) }) };
    work = work.then(() => failure || emit(batch)).catch(fail);
  };

  // The snapshot is normally older than the stream's first frame. The vendor client feeds
  // frames to the WASM at once and fetches the missing range meanwhile; so do we.
  const begin = () => {
    const lagging = decoder.laggingIssues();
    if (!lagging.length) return finishStart(0);
    trace?.(`catch-up needed for ${lagging.length} issues`);
    work = work.then(async () => {
      if (failure) return;
      decoder.catchUp(await requestCatchUp(lagging), lagging);
      finishStart(lagging.length);
    }).catch(fail);
  };

  const handle = async (frame, received_unix_ms) => {
    const t0 = process.hrtime.bigint();
    decoder.feed(frame);
    decoder.applyBasePrice();
    const { quotes, skipped } = decoder.quotes(decoder.changed().filter(id => ids.has(id)));
    updates += quotes.length;
    await emit({ type: 'quotes', seq: seq++, source_time_us: decoder.time(), received_unix_ms,
      decode_ns: Number(process.hrtime.bigint() - t0), replay_lateness_ms: null, quotes,
      ...(skipped.length && { skipped_issues: skipped.sort((a, b) => a - b) }) });
  };

  const onFrame = frame => {
    const received_unix_ms = Date.now();
    frames++;
    if (started) {
      work = work.then(() => failure || handle(frame, received_unix_ms)).catch(fail);
      return;
    }
    try {
      decoder.feed(frame);
      decoder.applyBasePrice();
      if (!starting && decoder.initialFrames) { starting = true; begin(); }
    } catch (error) { fail(error); }
  };

  try {
    monitor = heartbeatMonitor({ intervalMs: heartbeatMs, checkEveryMs: Math.min(1000, heartbeatMs / 4), onFail: fail,
      staleCheck: stale && (() => stale.check(decoder.time())) });
    timer = setTimeout(() => fail(new Error(`${where.name} stream did not initialize within ${startTimeoutMs / 1000}s; `
      + 'no first frame numbers arrived (run with --trace-protocol to see what the server sent)')), startTimeoutMs);
    // Cookies go only to the host that set them (elsewhere the session in ws_url authenticates);
    // Origin is what a browser on the site sends.
    const headers = { origin: where.origin };
    if (wsUrl.origin === where.origin.replace(/^https:/, 'wss:')) headers.cookie = session.cookie;
    link = new Link({ WebSocketImpl, url: wsUrl, headers, profile, context, trace, connectTimeoutMs, onBinary: onFrame,
      onClose: closed => settle(closed), onError: error => { failure = failure || error; settle({ code: -1, reason: '' }); } });

    const closed = await ended;
    // A catch-up that finishes extends the chain (it queues the bootstrap), so wait until it stops growing.
    for (let pending = null; pending !== work;) { pending = work; await pending; }
    if (failure) throw failure;
    if ((closed.code !== 1000 && !marketFinished) || !started) {
      throw new Error(`${where.name} stream closed (${closed.code} ${closed.reason || ''})`.trim());
    }
    const summary = { type: 'end', seq, source_time_us: decoder.time(), frames, quote_updates: updates,
      replay_wall_ms: performance.now() - start };
    await emit(summary);
    return summary;
  } finally {
    clearTimeout(timer);
    monitor?.stop();
    link?.close();
  }
}

const USAGE = 'Usage: BRISK_SBI_COOKIES=... [BRISK_SBI_SITE=e-shiten] sbi.cjs [--codes 7203,6758] [--trace-protocol]';

function parseArgs(argv) {
  const options = { codes: [], trace: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--codes' && argv[i + 1] !== undefined && !argv[i + 1].startsWith('--') && !options.codesSet) {
      options.codes = argv[++i].split(','); options.codesSet = true;
    } else if (argv[i] === '--trace-protocol' && !options.trace) {
      options.trace = true;
    } else throw new Error(USAGE);
  }
  return options;
}

async function main(argv, env = process.env) {
  const { codes, trace } = parseArgs(argv);
  let profile = {};
  if (env.BRISK_SBI_PROFILE) {
    try { profile = JSON.parse(env.BRISK_SBI_PROFILE); } catch { throw new Error('BRISK_SBI_PROFILE is not valid JSON'); }
  }
  await live({ cookies: JSON.parse(env.BRISK_SBI_COOKIES || 'null'), codes, profile, site: env.BRISK_SBI_SITE || 'sbi',
    trace: trace ? line => process.stderr.write(`[sbi protocol] ${line}\n`) : null,
    emit: async record => {
      if (!process.stdout.write(JSON.stringify(record) + '\n')) await once(process.stdout, 'drain');
    } });
}

module.exports = { Session, SITES, site, decoderAssets, streamUrl, checkAbi, checkMaster, live, main, fetchCatchUp, CATCH_UP_FORMATS, DEFAULT_CATCH_UP,
  PROTOCOL_VERSION, REQUIRED };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message); process.exitCode = 1;
});
