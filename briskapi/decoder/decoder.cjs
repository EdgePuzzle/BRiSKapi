'use strict';
// Adapter for the pinned Next demo's public WASM ABI, not a guessed wire decoder.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { once } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');
const manifest = require('./assets.json');
const { verifyAsset, loadWebAssets } = require('./web.cjs');
const BUFFER = 4 * 1024 * 1024;

function u64(words, i) {
  const value = words[i] + 4294967296 * words[i + 1];
  if (!Number.isSafeInteger(value)) throw new Error('Unsafe 64-bit quantity/timestamp');
  return value;
}

// A master read with the wrong layout can hold any bits; NaN keeps them for the host's layout check.
function looseU64(words, i) {
  const value = words[i] + 4294967296 * words[i + 1];
  return Number.isSafeInteger(value) ? value : NaN;
}

function* frames(data) {
  let previous = -1;
  for (let offset = 0; offset < data.length;) {
    if (offset + 8 > data.length) throw new Error('Truncated mock frame header');
    const time = data.readUInt32LE(offset);
    const size = data.readUInt32LE(offset + 4);
    if (!size || size > BUFFER || offset + 8 + size > data.length) throw new Error('Invalid mock frame length');
    if (time < previous) throw new Error('Mock replay times regressed');
    previous = time;
    yield { time, data: data.subarray(offset + 8, offset + 8 + size) };
    offset += 8 + size;
  }
}

// QR history is needed by the vendor's rollback/last-price callbacks. Retain it
// for the finite recording; do not replace it with the trade-only upstream hook.
class QuoteHistory {
  constructor() { this.qrs = new Map(); }
  initialize(p) { this.qrs.set(p, []); }
  deinitialize(p) { this.qrs.delete(p); }
  clone(dst, src) { this.qrs.set(dst, this.qrs.get(src).slice()); }
  emplace(p, type, timestamp, price10, qty, lot, frame) { this.qrs.get(p).push({ price10, frame }); }
  popBack(p) { this.qrs.get(p).pop(); }
  size(p) { return this.qrs.get(p).length; }
  lastPrice10(p) { return this.qrs.get(p).at(-1)?.price10 || 0; }
}

function loadAssets(cache) {
  const assets = {};
  for (const name of Object.keys(manifest.assets)) {
    const data = fs.readFileSync(path.join(cache, name));
    assets[name] = verifyAsset(name, data);
  }
  return assets;
}

// Position of the stock view's fields, relative to a version's base. Protocol 16000 (the 2021
// demo, and the vendor client's own parse of it) puts a header word first. 18000 (SBI's current
// build, as an independent parser of it reads the same view) drops that word, so every index is
// one lower. The fields and their order are otherwise identical. Any other version is refused
// rather than read at a guessed offset.
const STOCK_VIEW_SHIFT = { 16000: 0, 18000: -1 };

function stockViewShift(protocolVersion) {
  if (!(protocolVersion in STOCK_VIEW_SHIFT)) {
    throw new Error(`Stock view layout for protocol ${protocolVersion} is not known (known: ${Object.keys(STOCK_VIEW_SHIFT).join(', ')})`);
  }
  return STOCK_VIEW_SHIFT[protocolVersion];
}

// `words` is the raw _getStockView buffer; `ohlc` the number of OHLC bars ahead of the quote fields.
function parseStockView(words, ohlc, shift) {
  const a = words.subarray(6 * ohlc + shift);
  return { frame: a[19], max_frame: a[20], source_time_us: u64(a, 17), last_price10: words[1 + shift],
    open_price10: words[2 + shift], bid_price10: a[7], ask_price10: a[8], indicative_price10: a[9],
    indicative_volume: u64(a, 10), indicative_side: a[12], closing_indicative_price10: a[13],
    closing_indicative_volume: u64(a, 14), quote_flag: a[21], quote_side: a[22],
    special_quote_time_us: u64(a, 23), indicative_open_price10: a[32], auction_reference_price10: a[35],
    volume: u64(a, 3) };
}

// The decoder's own master records (_getStockMaster), in issue order. 16000 (the demo) is the vendor
// client's layout. 18000 widens the code from a number to eight bytes of text, which must hold codes
// such as 167A, so every later field is four bytes on (`shift`): found by running SBI's decoder of
// March 2026 (from pybrisk's research) on the master file it served, where all 4,445 records agree
// with the file on code, prices, lot size and issue type. Any other version is refused.
const MASTER_LAYOUT = { 16000: { size: 144, shift: 0 }, 18000: { size: 148, shift: 4 } };

// The 2026 decoders take an eighth _initialize argument: the session schedule, which the
// vendor client builds with _createSchedule from the boot response's schedule_info. Without
// it they keep no price bars (_ohlcLength 0) and their stock views can't be read. The
// arguments are schedule_info's fields in the boot's own order; moving an open or close time by
// 30 minutes moves the bar count by 6 (66 five-minute bars for 9:00-11:30 and 12:30-15:30).
const SCHEDULE_FIELDS = ['morning_session_pre_open_time', 'morning_session_open_time', 'morning_session_close_time',
  'afternoon_session_pre_open_time', 'afternoon_session_open_time', 'afternoon_session_pre_close_time',
  'afternoon_session_close_time', 'sq_jump_interval'];

function scheduleArgs(schedule) {
  if (schedule === null || typeof schedule !== 'object') throw new Error('The boot response has no schedule_info, which this decoder needs');
  const missing = SCHEDULE_FIELDS.filter(key => !Number.isFinite(schedule[key]));
  if (missing.length) throw new Error(`The boot schedule_info has no usable ${missing.join(', ')}`);
  return SCHEDULE_FIELDS.map(key => schedule[key]);
}

function masterLayout(protocolVersion) {
  if (!(protocolVersion in MASTER_LAYOUT)) {
    throw new Error(`Master layout for protocol ${protocolVersion} is not known (known: ${Object.keys(MASTER_LAYOUT).join(', ')})`);
  }
  return MASTER_LAYOUT[protocolVersion];
}

// One record at byte `at` of `heap`. Past the code, field indices are the demo's.
// The name never runs past its record, however a misread length says.
function readMasterRecord(heap, at, issue_id, { size, shift }) {
  const a = new Uint32Array(heap.buffer, heap.byteOffset + at + shift, 36);
  const code = shift ? Buffer.from(heap.subarray(at, at + 8)).toString('latin1').replace(/\0[^]*$/, '') : String(a[0]);
  return { issue_id, code, tick_type: a[1], base_price10: a[2], limit_up10: a[3], limit_down10: a[4],
    lot_size: looseU64(a, 5), issue_type: a[7],
    name: Buffer.from(heap.subarray(at + shift + 68, at + Math.min(size, shift + 68 + a[16]))).toString('utf8') };
}

// The master file the decoder loads: fixed 180-byte records in issue order, after a preamble (444
// bytes in the 2021 demo, none in SBI's of 2026-03-11). A 16-byte binary header (a word of unknown
// use, then base price, upper and lower limit in tenths of a yen) and fixed-width text: issue type at
// 40, code at 50 (five wide), lot size right-aligned up to 139. pybrisk's research documents the
// header and the code; the rest agrees with the vendor decoder's own master on every issue of both
// files. Names and tick types are not read here: the decoder's differ (it keeps "Ｕ－ＮＥＸＴ" whole
// but drops a market prefix such as "Ｍ－", and SBI's tick types follow no single column).
const MASTER_RECORD = 180;

function parseMasterFile(data, count) {
  const start = data.length - count * MASTER_RECORD;
  if (!(count > 0) || start < 0) throw new Error(`A master file of ${data.length} bytes cannot hold ${count} issues`);
  const text = (r, from, to) => r.toString('latin1', from, to).trim();
  const integer = value => (/^\d+$/.test(value) ? Number(value) : NaN);
  return Array.from({ length: count }, (_, issue_id) => {
    const r = data.subarray(start + MASTER_RECORD * issue_id, start + MASTER_RECORD * (issue_id + 1));
    return { issue_id, code: text(r, 50, 55), base_price10: r.readUInt32LE(4), limit_up10: r.readUInt32LE(8),
      limit_down10: r.readUInt32LE(12), lot_size: integer(text(r, 124, 139)), issue_type: integer(text(r, 40, 44)) };
  });
}

// The decoder's master must agree with the file it loaded. A record layout that no longer fits a
// build shows here as a disagreement, and is refused rather than read.
const CHECKED = ['code', 'base_price10', 'limit_up10', 'limit_down10', 'lot_size', 'issue_type'];

function checkMasterFile(master, data) {
  const file = parseMasterFile(data, master.length);
  const wrong = master.filter((m, i) => CHECKED.some(key => m[key] !== file[i][key]));
  if (wrong.length) {
    const [m] = wrong, f = file[m.issue_id];
    const key = CHECKED.find(k => m[k] !== f[k]);
    throw new Error(`The decoder's master disagrees with its master file on ${wrong.length} of ${master.length} issues `
      + `(issue ${m.issue_id}: ${key} ${JSON.stringify(m[key])}, file ${JSON.stringify(f[key])}); its record layout may have changed`);
  }
}

class Decoder {
  // options.protocolVersion: 16000 for the Next demo (default), 18000 for SBI BRiSK.
  // options.callbacks: { send(Buffer), heartbeat(ns BigInt), frameNumbers(Array), basePrice(issueIndex),
  //   authError(), marketFinished() }, the vendor client's six WASM callbacks.
  // options.boot: { exceptional_sq, base_prices } from the app boot response, applied in the
  //   vendor client's order: after the master is loaded, before the snapshot.
  // options.checkMaster(master): throws if the master read does not look like a market, before
  //   anything else uses it. Without it, a lot size beyond 2^53 is an error. Either way the
  //   master is then checked against the master file.
  static async create(assets, options = {}) {
    // Isolate the legacy glue's globals and process exception handlers. No UI,
    // network, filesystem access or account state is needed inside this VM.
    const context = vm.createContext({
      module: { exports: {} }, exports: {}, qrm: new QuoteHistory(),
      console: { log() {}, warn() {}, error() {} },
      crypto: crypto.webcrypto, TextDecoder, TextEncoder, setTimeout, clearTimeout,
    });
    vm.runInContext(assets['fita.js'].toString('utf8'), context, { filename: 'brisk-demo-fita.js' });
    const { wasm } = await new Promise((resolve, reject) => {
      context.module.exports({ wasmBinary: assets['fita.wasm'],
        print() {}, printErr() {}, onAbort: reject,
      }).then(wasm => resolve({ wasm }));
    });
    options.check?.(wasm);
    return new Decoder(wasm, assets, options);
  }

  // options.schedule: the boot's schedule_info. Used only by decoders that take one (the 2026
  //   builds export _createSchedule; the 2021 demo's doesn't); null there is an error.
  constructor(w, assets, { protocolVersion = manifest.protocol_version, callbacks = {}, boot = {}, checkMaster, schedule } = {}) {
    this.w = w;
    this.layoutShift = stockViewShift(protocolVersion);
    this.layout = masterLayout(protocolVersion);
    this.basePriceQueue = new Set();
    this.authError = false;
    this.marketFinished = false;
    this.initialFrames = null;
    // A schedule is created before _initialize copies it in; the 2021 demo's build takes none.
    this.scheduled = typeof w._createSchedule === 'function' && schedule !== undefined;
    const scheduleArg = this.scheduled ? [w._createSchedule(...scheduleArgs(schedule))] : [];
    this.lastHeartbeat = null;
    // The signatures are the vendor client's own (an indirect call with another type traps).
    const add = (fn, sig) => w.addFunction(fn, sig);
    this.id = w._initialize(
      // send(id, ptr, len): bytes the client must write to the server. The WASM decides
      // when (the vendor client has no ping timer of its own), so a host must forward them.
      add((id, ptr, len) => callbacks.send?.(Buffer.from(w.HEAPU8.slice(ptr, ptr + len))), 'viii'),
      // updateNumber(id, ptr, count): the first frame number of every issue on the stream.
      add((id, p, count) => {
        this.initialFrames = Array.from(new Uint32Array(w.HEAPU8.buffer, p, count));
        callbacks.frameNumbers?.(this.initialFrames);
      }, 'viii'),
      // heartbeat(id, low, high): the server's clock, nanoseconds since the Unix epoch.
      add((id, low, high) => {
        this.lastHeartbeat = BigInt(low >>> 0) + (BigInt(high >>> 0) << 32n);
        callbacks.heartbeat?.(this.lastHeartbeat);
      }, 'viii'),
      // basePrice(id, issueIndex): the decoder changed an issue's base price or limits.
      add((id, issueIndex) => { this.basePriceQueue.add(issueIndex); callbacks.basePrice?.(issueIndex); }, 'vii'),
      add(() => { this.authError = true; callbacks.authError?.(); }, 'vii'),
      add(() => { this.marketFinished = true; callbacks.marketFinished?.(); }, 'vi'),
      protocolVersion, ...scheduleArg);
    this.buf = w._malloc(BUFFER);
    this.aux = w._malloc(64);
    this.push(assets['master.dat']);
    w._applyBasePriceQueue(this.id);
    const count = w._stockCount(this.id);
    if (count <= 0 || count * this.layout.size > BUFFER) throw new Error('Invalid master count');
    w._getStockMaster(this.id, this.buf, count * this.layout.size);
    this.master = Array.from({ length: count },
      (_, issue_id) => readMasterRecord(w.HEAPU8, this.buf + this.layout.size * issue_id, issue_id, this.layout));
    if (checkMaster) checkMaster(this.master);
    else if (this.master.some(m => Number.isNaN(m.lot_size))) throw new Error('Unsafe 64-bit quantity/timestamp');
    checkMasterFile(this.master, assets['master.dat']);
    this.boot = this.applyBoot(boot);
    this.unserialize(assets['snapshot.dat']);
    this.date = String(w._getDate(this.id));
  }

  // The vendor client pushes the boot response's exceptional special quotes and base-price
  // changes into the decoder right after the master, then re-reads the master's limits.
  applyBoot({ exceptional_sq = [], base_prices = [] } = {}) {
    const byCode = new Map(this.master.map(m => [m.code, m.issue_id]));
    const number = (value, what) => {
      if (!Number.isFinite(value)) throw new Error(`Boot ${what} is not a number: ${JSON.stringify(value)}`);
      return value;
    };
    // The decoder takes a special quote's issue as a number. How it takes a code such as 167A
    // (issued since 2024) is not known, so those are left out and listed rather than guessed.
    const skipped = [];
    for (const sq of exceptional_sq) {
      if (!/^\d+$/.test(String(sq.issue_code))) {
        skipped.push(String(sq.issue_code));
        continue;
      }
      this.w._pushSQ(this.id, Number(sq.issue_code), sq.buy_sell === 'B' ? 0 : 1, 10 * number(sq.jump_range, 'jump_range'),
        number(sq.jump_sec, 'jump_sec'), 10 * number(sq.quote_limit_down, 'quote_limit_down'),
        10 * number(sq.quote_limit_up, 'quote_limit_up'));
    }
    for (const change of base_prices) {
      const issue = byCode.get(String(change.issue_code));
      if (issue === undefined) throw new Error(`Boot base_prices names issue code ${change.issue_code}, which is not in the master`);
      this.w._pushBasePrice(this.id, issue, number(change.base_price10, 'base_price10'),
        number(change.limit_down10, 'limit_down10'), number(change.limit_up10, 'limit_up10'));
      this.basePriceQueue.add(issue);
    }
    return { exceptional_sq: exceptional_sq.length - skipped.length, base_prices: base_prices.length, applied: this.applyBasePrice(),
      ...(skipped.length && { skipped_exceptional_sq: skipped }) };
  }

  // Re-read the master's price limits for issues whose base price changed (the vendor client
  // does this after boot and after every frame); cheap when nothing is queued.
  applyBasePrice() {
    if (!this.basePriceQueue.size) return 0;
    const { size } = this.layout;
    this.w._getStockMaster(this.id, this.buf, this.master.length * size);
    let applied = 0;
    for (const issue of this.basePriceQueue) {
      const m = this.master[issue];
      if (!m) throw new Error(`Base price change for unknown issue index ${issue}`);
      const { base_price10, limit_up10, limit_down10 } = readMasterRecord(this.w.HEAPU8, this.buf + size * issue, issue, this.layout);
      Object.assign(m, { base_price10, limit_up10, limit_down10 });
      applied++;
    }
    this.basePriceQueue.clear();
    return applied;
  }

  push(data) {
    for (let offset = 0; offset < data.length; offset += BUFFER) {
      const part = data.subarray(offset, offset + BUFFER);
      this.w.HEAPU8.set(part, this.buf);
      this.w._push(this.id, this.buf, part.length);
    }
  }

  unserialize(data) {
    let start = 0, size = 0, first = 0, count = 0;
    const flush = () => {
      this.w.HEAPU8.set(data.subarray(start, start + size), this.buf);
      this.w._unserialize(this.id, this.buf, size, first, first + count);
      start += size; first += count; size = 0; count = 0;
    };
    for (let offset = 0; offset < data.length;) {
      if (offset + 4 > data.length) throw new Error('Truncated snapshot header');
      const length = data.readUInt32LE(offset);
      if (length < 4 || length > BUFFER || offset + length > data.length) throw new Error('Invalid snapshot record');
      if (size + length > BUFFER) flush();
      size += length; count++; offset += length;
    }
    if (size) flush();
  }

  // Bars of OHLC history ahead of the quote fields in a stock view. SBI's build
  // reports it once its stream is initialized; the demo's comes from the manifest.
  get ohlc() {
    this._ohlc ??= typeof this.w._ohlcLength === 'function' ? this.w._ohlcLength(this.id) : manifest.ohlc_length;
    return this._ohlc;
  }

  get viewWords() {
    return 37 + 6 * this.ohlc;
  }

  time() {
    this.w._getTime(this.id, this.aux);
    return u64(new Uint32Array(this.w.HEAPU8.buffer, this.aux, 2), 0);
  }

  feed(data) {
    if (data.length > BUFFER) throw new Error('Frame exceeds decoder buffer');
    this.w.HEAPU8.set(data, this.buf);
    this.w._pushWs(this.id, this.buf, data.length, this.aux, this.aux + 8);
    this.w._applyBasePriceQueue(this.id);
    if (this.authError) throw new Error('Decoder rejected session');
  }

  // Feed one frame; true once the decoder has reported its initial frame numbers
  // and quote tracing has started (the demo's first frame always does this).
  start(frame) {
    this.feed(frame);
    if (!this.initialFrames) return false;
    if (this.laggingIssues().length) throw new Error('Snapshot needs unavailable catch-up data');
    this.begin();
    return true;
  }

  // Frame number of every issue as the decoder holds it now (the snapshot's, before catch-up).
  frameNumbers() {
    this.w._getFrameNumbers(this.id, this.buf, this.master.length);
    return Array.from(new Uint32Array(this.w.HEAPU8.buffer, this.buf, this.master.length));
  }

  // Issues whose snapshot is older than the stream's first frame: the ranges a live
  // client must fetch before the state is consistent. Empty for the demo's cut recording.
  laggingIssues() {
    if (!this.initialFrames) throw new Error('Frame numbers not received yet');
    if (this.initialFrames.length !== this.master.length) throw new Error('Missing frame-number bootstrap');
    const current = this.frameNumbers();
    const lagging = [];
    this.initialFrames.forEach((to, issue_id) => { if (current[issue_id] < to) lagging.push({ issue_id, from: current[issue_id], to }); });
    return lagging;
  }

  // Mark the API data complete and start tracing quote changes.
  begin() {
    this.w._apiRecieved(this.id);
    this.trace = this.w._addTraceUpdate(this.id);
  }

  changed() {
    const count = this.w._getTrace(this.id, this.trace, this.buf, 8192);
    if (count < 0 || count > 8192) throw new Error('Decoder trace overflow');
    const ids = Array.from(new Uint16Array(this.w.HEAPU8.buffer, this.buf, count));
    this.w._clearTrace(this.id, this.trace);
    if (ids.some(id => id >= this.master.length)) throw new Error('Invalid trace issue ID');
    return ids;
  }

  quote(issue_id) {
    const m = this.master[issue_id];
    if (!m) throw new Error(`Unknown issue ID: ${issue_id}`);
    if (!this.w._getStockView(this.id, issue_id, this.buf)) throw new Error(`Stock view unavailable: ${m.code}`);
    const words = new Uint32Array(this.w.HEAPU8.buffer, this.buf, this.viewWords);
    const quote = { issue_id, code: m.code, ...parseStockView(words, this.ohlc, this.layoutShift) };
    // SBI's build has no portfolio export, so its quotes carry no issue_status.
    if (this.w._getPortfolio) {
      if (!this.w._getPortfolio(this.id, Number(m.code), this.buf)) throw new Error(`Portfolio unavailable: ${m.code}`);
      quote.issue_status = new Uint32Array(this.w.HEAPU8.buffer, this.buf, 22 + this.ohlc)[this.ohlc + 15];
    }
    // Request one price row solely to obtain the market/over/under rows in the
    // documented ABI. Row 0 is market orders, not the top-of-book depth.
    const price = quote.indicative_price10 || quote.bid_price10 || quote.ask_price10 || m.base_price10;
    const index = this.w._fitItaViewRowPrice10(this.id, issue_id, price, 1);
    if (!this.w._getItaRows(this.id, issue_id, index, 1, this.buf + 612,
      this.buf, this.buf + 204, this.buf + 408, 0)) throw new Error(`Book unavailable: ${m.code}`);
    const row = new Uint32Array(this.w.HEAPU8.buffer, this.buf, 51);
    quote.market_buy_quantity = u64(row, 5);
    quote.market_sell_quantity = u64(row, 28);
    quote.closing_market_buy_quantity = u64(row, 16);
    quote.closing_market_sell_quantity = u64(row, 39);
    return quote;
  }
}

async function replay({ cache, web = false, codes = [], speed = 1, limitFrames = Infinity, emit }) {
  if (!Number.isFinite(speed) || speed < 0) throw new Error('Speed must be >= 0 (0 means unpaced)');
  if (!(limitFrames > 0)) throw new Error('Frame limit must be positive');
  if (Boolean(cache) === Boolean(web)) throw new Error('Select exactly one of --cache DIR or --web');
  const { assets, input_transport } = web ? await loadWebAssets()
    : { assets: loadAssets(cache), input_transport: { kind: 'local_cache' } };
  const decoder = await Decoder.create(assets);
  if (decoder.date !== manifest.trading_date.replaceAll('-', '')) throw new Error('Unexpected mock trading date');
  const selected = new Set(codes);
  const master = decoder.master.filter(m => !selected.size || selected.has(m.code));
  const missing = codes.filter(code => !master.some(m => m.code === code));
  if (missing.length) throw new Error(`Codes missing from 2021 mock: ${missing.join(',')}`);
  const ids = new Set(master.map(m => m.issue_id));
  const iter = frames(assets['ws.dat']);
  const first = iter.next().value;
  if (!first) throw new Error('Empty mock recording');
  if (!decoder.start(first.data)) throw new Error('Missing frame-number bootstrap');
  let seq = 0;
  await emit({ type: 'bootstrap', seq: seq++, source: 'historical_mock', trading_date: decoder.date, input_transport,
    source_timestamp_origin: 'brisk_decoder_unverified', exchange_delay_ms: null,
    source_time_us: decoder.time(), market_issue_count: decoder.master.length, master,
    quotes: master.map(m => decoder.quote(m.issue_id)) });
  const start = performance.now();
  let frameCount = 1, updateCount = 0;
  for (const frame of iter) {
    if (frameCount >= limitFrames) break;
    if (speed) {
      const wait = (frame.time - first.time) / speed - (performance.now() - start);
      if (wait > 0) await delay(wait);
    }
    const received_unix_ms = Date.now();
    // Compare local monotonic elapsed time with the replay schedule. The
    // recording clock is historical and cannot measure exchange delay.
    const replay_lateness_ms = speed ? Math.max(0,
      performance.now() - start - (frame.time - first.time) / speed) : null;
    const began = process.hrtime.bigint();
    decoder.feed(frame.data);
    const quotes = decoder.changed().filter(id => ids.has(id)).map(id => decoder.quote(id));
    const decode_ns = Number(process.hrtime.bigint() - began);
    // Include empty batches: sequence continuity and feed health differ from a
    // quiet security's last update. No fixed polling interval or dropped deltas.
    await emit({ type: 'quotes', seq: seq++, source_time_us: decoder.time(),
      received_unix_ms, decode_ns, replay_lateness_ms, quotes });
    updateCount += quotes.length; frameCount++;
  }
  const summary = { type: 'end', seq, source_time_us: decoder.time(), frames: frameCount,
    quote_updates: updateCount, replay_wall_ms: performance.now() - start };
  await emit(summary);
  return summary;
}

async function main(argv) {
  const args = {};
  for (let i = 0; i < argv.length;) {
    if (argv[i] === '--web') {
      if (args['--web']) throw new Error('Duplicate --web');
      args['--web'] = true; i++; continue;
    }
    if (!['--cache', '--codes', '--speed', '--limit-frames'].includes(argv[i]) || argv[i + 1] === undefined || argv[i + 1].startsWith('--')) {
      throw new Error('Usage: decoder.cjs (--cache DIR | --web) [--codes 7203,6758] [--speed 1] [--limit-frames N]');
    }
    if (args[argv[i]] !== undefined) throw new Error(`Duplicate ${argv[i]}`);
    args[argv[i]] = argv[i + 1]; i += 2;
  }
  if (Boolean(args['--cache']) === Boolean(args['--web'])) throw new Error('Select exactly one of --cache DIR or --web');
  await replay({ cache: args['--cache'], web: args['--web'] || false, codes: args['--codes']?.split(',') || [],
    speed: Number(args['--speed'] ?? 1), limitFrames: args['--limit-frames'] ? Number(args['--limit-frames']) : Infinity,
    emit: async record => {
      if (!process.stdout.write(JSON.stringify(record) + '\n')) await once(process.stdout, 'drain');
    } });
}

module.exports = { Decoder, QuoteHistory, frames, u64, looseU64, readMasterRecord, masterLayout, MASTER_LAYOUT, SCHEDULE_FIELDS, scheduleArgs,
  parseMasterFile, checkMasterFile, loadAssets, replay, main, parseStockView, stockViewShift, STOCK_VIEW_SHIFT };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message); process.exitCode = 1;
});
