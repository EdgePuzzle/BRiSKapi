'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { Decoder, QuoteHistory, frames, u64, looseU64, parseMasterFile, checkMasterFile, loadAssets, replay, main, parseStockView, stockViewShift } = require('../../briskapi/decoder/decoder.cjs');
const cache = process.env.BRISK_MOCK_CACHE;

function frame(time, bytes = Buffer.from([1])) {
  const data = Buffer.alloc(8 + bytes.length);
  data.writeUInt32LE(time, 0); data.writeUInt32LE(bytes.length, 4); bytes.copy(data, 8);
  return data;
}

test('record container validates lengths, truncation and time order', () => {
  assert.equal([...frames(Buffer.concat([frame(9), frame(10)]))].length, 2);
  for (const bad of [Buffer.alloc(7), frame(9).subarray(0, 8), Buffer.alloc(8),
    Buffer.concat([frame(9), frame(8)])]) assert.throws(() => [...frames(bad)]);
  assert.equal(u64([0xffffffff, 1], 0), 8589934591);
  assert.throws(() => u64([0xffffffff, 0xffffffff], 0), /Unsafe/);
  assert.equal(looseU64([100, 0], 0), 100);
  assert.ok(Number.isNaN(looseU64([0xffffffff, 0xffffffff], 0)));
});

test('QR rollback and clone keep independent last prices', () => {
  const q = new QuoteHistory(); q.initialize(1);
  assert.equal(q.lastPrice10(1), 0);
  q.emplace(1, 0, 99, 700, 10, 100, 1); q.clone(2, 1);
  q.emplace(2, 0, 100, 710, 10, 100, 2);
  assert.equal(q.lastPrice10(1), 700); assert.equal(q.lastPrice10(2), 710);
  q.popBack(2); assert.equal(q.size(2), 1); q.deinitialize(2);
  assert.equal(q.qrs.has(2), false);
});

test('rejects unpinned cache and invalid CLI settings', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'brisk-hash-'));
  try { fs.writeFileSync(path.join(dir, 'fita.js'), 'bad'); assert.throws(() => loadAssets(dir), /hash mismatch/); }
  finally { fs.rmSync(dir, { recursive: true }); }
  await assert.rejects(main([]), /cache/);
  await assert.rejects(main(['--unknown', 'x']), /Usage/);
  await assert.rejects(main(['--cache']), /Usage/);
  await assert.rejects(replay({ speed: -1 }), /Speed/);
  await assert.rejects(replay({ speed: Infinity }), /Speed/);
  await assert.rejects(replay({ speed: 0, limitFrames: 0 }), /limit/);
});

test('decoder rejects malformed snapshots, gaps and overflows', () => {
  const d = Object.create(Decoder.prototype);
  d.w = { HEAPU8: new Uint8Array(4 * 1024 * 1024), _push() {}, _unserialize() {},
    _pushWs() {}, _applyBasePriceQueue() {}, _getTrace: () => -1, _clearTrace() {},
    _getStockView: () => 0 };
  d.buf = 0; d.aux = 8; d.master = [{ code: '7203' }];
  d.push(Buffer.alloc(4 * 1024 * 1024 + 1));
  for (const bad of [Buffer.alloc(3), Buffer.alloc(4), Buffer.from([9, 0, 0, 0])]) {
    assert.throws(() => d.unserialize(bad), /snapshot/);
  }
  assert.throws(() => d.feed(Buffer.alloc(4 * 1024 * 1024 + 1)), /buffer/);
  d.authError = true; assert.throws(() => d.feed(Buffer.alloc(1)), /session/); d.authError = false;
  assert.equal(d.start(Buffer.alloc(1)), false);  // waits for frame numbers
  d.initialFrames = [1, 2];
  assert.throws(() => d.start(Buffer.alloc(1)), /bootstrap/);
  d.initialFrames = [1]; d.w._getFrameNumbers = () => {};
  assert.throws(() => d.start(Buffer.alloc(1)), /catch-up/);
  assert.throws(() => d.changed(), /overflow/);
  d.w._getTrace = () => 1; d.w.HEAPU8[0] = 2;
  assert.throws(() => d.changed(), /issue ID/);
  assert.throws(() => d.quote(1), /Unknown/);
  assert.throws(() => d.quote(0), /unavailable/);
});

test('public mock pre-open snapshot and complete opening replay', { skip: !cache }, async () => {
  let bootstrap, previousSeq = -1, previousTime = 0, count = 0, sawOpeningTrade = false;
  const stats = await replay({ cache, speed: 0, emit: batch => {
    assert.equal(batch.seq, previousSeq + 1); previousSeq = batch.seq;
    assert.ok(batch.source_time_us >= previousTime); previousTime = batch.source_time_us;
    if (batch.type === 'bootstrap') {
      bootstrap = batch;
      assert.equal(batch.market_issue_count, 4131); assert.equal(batch.quotes.length, 4131);
      assert.equal(batch.source_time_us, 32399999955);
      assert.equal(batch.source_timestamp_origin, 'brisk_decoder_unverified');
      assert.equal(batch.exchange_delay_ms, null);
      const q = batch.quotes.find(q => q.code === '7203');
      assert.equal(q.indicative_price10, 101500); assert.equal(q.indicative_volume, 304700);
      assert.equal(q.market_buy_quantity, 201300); assert.equal(q.market_sell_quantity, 105700);
      assert.equal(q.volume, 0); assert.equal(q.last_price10, 0);
      assert.equal(batch.quotes.filter(q => q.indicative_price10 > 0).length, 3891);
    }
    if (batch.type === 'quotes') {
      assert.equal(batch.replay_lateness_ms, null);
      count += batch.quotes.length;
      if (batch.quotes.some(q => q.code === '7203' && q.open_price10 > 0 && q.volume > 0)) sawOpeningTrade = true;
    }
  } });
  assert.equal(bootstrap.trading_date, '20210927');
  assert.equal(stats.frames, 18001); assert.equal(count, 731251);
  assert.equal(stats.source_time_us, 32579999896); assert.ok(sawOpeningTrade);
});

test('filtering, replay pacing and partial-run termination', { skip: !cache }, async () => {
  const batches = [];
  await replay({ cache, codes: ['7203', '5659'], speed: 10, limitFrames: 10, emit: b => batches.push(b) });
  assert.equal(batches[0].quotes.length, 2);
  assert.ok(batches.every(b => !b.quotes || b.quotes.every(q => ['7203', '5659'].includes(q.code))));
  assert.equal(batches.at(-1).frames, 10);
  assert.ok(batches.filter(b => b.type === 'quotes').every(b => b.replay_lateness_ms >= 0));
  await assert.rejects(replay({ cache, codes: ['130A'], speed: 0, emit() {} }), /missing from 2021/);
});

test('CLI emits JSON only and reports failures', { skip: !cache }, () => {
  const command = path.join(__dirname, '../../briskapi/decoder/decoder.cjs');
  const run = spawnSync(process.execPath, [command, '--cache', cache, '--codes', '7203', '--speed', '0', '--limit-frames', '2'], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  const records = run.stdout.trim().split('\n').map(JSON.parse);
  assert.equal(records.length, 3); assert.equal(records.at(-1).type, 'end');
  const bad = spawnSync(process.execPath, [command], { encoding: 'utf8' });
  assert.equal(bad.status, 1); assert.match(bad.stderr, /cache/);
});

// ---------------------------------------------------------------------------------------
// Stock view layout per protocol version. The two position tables below are written out
// literally, from two independent parsers, so they cross-check the shift used by the code:
//   16000: the vendor client's own getStockView (public Next demo bundle), n = 6 * ohlcLength
//   18000: an independent parser of SBI's current build, which reads the same
//          fields one word lower (the header word is gone)
const FIELDS_16000 = n => ({ last_price10: [1], open_price10: [2], volume: [n + 3, n + 4], bid_price10: [n + 7],
  ask_price10: [n + 8], indicative_price10: [n + 9], indicative_volume: [n + 10, n + 11], indicative_side: [n + 12],
  closing_indicative_price10: [n + 13], closing_indicative_volume: [n + 14, n + 15], source_time_us: [n + 17, n + 18],
  frame: [n + 19], max_frame: [n + 20], quote_flag: [n + 21], quote_side: [n + 22],
  special_quote_time_us: [n + 23, n + 24], indicative_open_price10: [n + 32], auction_reference_price10: [n + 35] });
const FIELDS_18000 = n => ({ last_price10: [0], open_price10: [1], volume: [n + 2, n + 3], bid_price10: [n + 6],
  ask_price10: [n + 7], indicative_price10: [n + 8], indicative_volume: [n + 9, n + 10], indicative_side: [n + 11],
  closing_indicative_price10: [n + 12], closing_indicative_volume: [n + 13, n + 14], source_time_us: [n + 16, n + 17],
  frame: [n + 18], max_frame: [n + 19], quote_flag: [n + 20], quote_side: [n + 21],
  special_quote_time_us: [n + 22, n + 23], indicative_open_price10: [n + 31], auction_reference_price10: [n + 34] });

function readWith(fields, words) {
  return Object.fromEntries(Object.entries(fields).map(([name, at]) =>
    [name, at.length === 2 ? words[at[0]] + 4294967296 * words[at[1]] : words[at[0]]]));
}

test('stock view layouts: both protocol versions read the same fields at their own positions', () => {
  for (const ohlc of [60, 5]) {
    const n = 6 * ohlc;
    for (const [version, table] of [[16000, FIELDS_16000(n)], [18000, FIELDS_18000(n)]]) {
      const words = new Uint32Array(37 + 6 * ohlc);
      let seed = 1000;
      const expected = {};
      for (const [name, at] of Object.entries(table)) {
        const value = at.length === 2 ? 5 * 4294967296 + (seed += 7) : (seed += 7);
        if (at.length === 2) { words[at[0]] = value % 4294967296; words[at[1]] = Math.floor(value / 4294967296); } else words[at[0]] = value;
        expected[name] = value;
      }
      assert.deepEqual(parseStockView(words, ohlc, stockViewShift(version)), expected, `protocol ${version}, ohlc ${ohlc}`);
    }
  }
  assert.throws(() => stockViewShift(17000), /layout for protocol 17000 is not known \(known: 16000, 18000\)/);
  assert.throws(() => stockViewShift(undefined), /is not known/);
});

test('the layout shift is the only difference between the two versions', () => {
  const n = 6 * 60;
  for (const [name, at16] of Object.entries(FIELDS_16000(n))) {
    assert.deepEqual(FIELDS_18000(n)[name], at16.map(i => i - 1), name);
  }
});

test('an unknown protocol version is refused before any frame is read', { skip: !cache }, async () => {
  await assert.rejects(Decoder.create(loadAssets(cache), { protocolVersion: 17000 }), /layout for protocol 17000 is not known/);
});

test('our parse equals the vendor client\'s own indices on real demo data', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const it = frames(assets['ws.dat']);
  decoder.start(it.next().value.data);
  let checked = 0;
  const check = () => {
    const n = 6 * decoder.ohlc;
    for (let issue = 0; issue < decoder.master.length; issue += 17) {
      if (!decoder.w._getStockView(decoder.id, issue, decoder.buf)) continue;
      const words = new Uint32Array(decoder.w.HEAPU8.buffer, decoder.buf, decoder.viewWords).slice();
      const { issue_id, code, ...ours } = decoder.quote(issue);
      const { ...expected } = readWith(FIELDS_16000(n), words);
      const compared = Object.fromEntries(Object.keys(expected).map(key => [key, ours[key]]));
      assert.deepEqual(compared, expected, `issue ${issue}`);
      checked++;
    }
  };
  check();
  for (let i = 0; i < 1500; i++) decoder.feed(it.next().value.data);
  check();
  assert.ok(checked > 400, `checked ${checked} views`);
});

// ---------------------------------------------------------------------------------------
// The six callbacks and the heartbeat/ping loop, against the real decoder.

function pingFrame(ns) {
  const frame = Buffer.alloc(9);
  frame[0] = 0xf0;
  frame.writeBigUInt64LE(ns, 1);
  return frame;
}

test('the decoder turns a ping echo into a heartbeat and answers with its own ping', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const beats = [], sends = [];
  const decoder = await Decoder.create(assets, { callbacks: { heartbeat: ns => beats.push(ns), send: b => sends.push(b) } });
  const it = frames(assets['ws.dat']);
  decoder.start(it.next().value.data);
  for (let i = 0; i < 300; i++) decoder.feed(it.next().value.data);
  decoder.feed(Buffer.from([0x38, 0x04, 0x00, 1, 2, 3]));                  // a server sync frame
  assert.deepEqual([beats.length, sends.length], [0, 0], 'market and sync frames neither beat nor ping');
  const clock = 1_791_475_200_123_456_789n;
  decoder.feed(pingFrame(clock));
  assert.deepEqual(beats, [clock]);
  assert.equal(decoder.lastHeartbeat, clock);
  assert.equal(sends.length, 1);
  assert.equal(sends[0].length, 9);
  assert.equal(sends[0][0], 0xf0);
  // The ping carries the server's clock as the decoder estimates it (the echo's time plus the
  // time elapsed since), not the local clock: the fed echo is hours away from Date.now().
  const sentClock = sends[0].readBigUInt64LE(1);
  assert.ok(Math.abs(Number(sentClock - clock) / 1e6) < 5000, 'the ping carries the server clock in nanoseconds');
  decoder.feed(pingFrame(clock + 1n));
  assert.equal(sends.length, 2, 'every echo is answered, so the loop sustains itself once a ping arrives');
});

test('all six WASM callbacks reach the host with the vendor\'s signatures', { skip: !cache }, async () => {
  const got = {};
  let wasm, pointers;
  const decoder = await Decoder.create(loadAssets(cache), {
    check: w => { wasm = w; const init = w._initialize; w._initialize = (...a) => { pointers = a.slice(0, 6); return init(...a); }; },
    callbacks: { send: b => got.send = b, heartbeat: n => got.heartbeat = n, basePrice: i => got.basePrice = i,
      authError: () => got.authError = true, marketFinished: () => got.marketFinished = true },
  });
  const ptr = wasm._malloc(16);
  wasm.HEAPU8.set([0xf0, 1, 2, 3], ptr);
  wasm.dynCall_viii(pointers[0], decoder.id, ptr, 4);
  wasm.dynCall_viii(pointers[2], decoder.id, -1, 5);   // a negative i32 half is unsigned on the wire
  wasm.dynCall_vii(pointers[3], decoder.id, 1424);
  wasm.dynCall_vii(pointers[4], decoder.id, 1);
  wasm.dynCall_vi(pointers[5], decoder.id);
  assert.equal(got.send.toString('hex'), 'f0010203');
  assert.equal(got.heartbeat, 0xFFFFFFFFn + (5n << 32n));
  assert.equal(got.basePrice, 1424);
  assert.ok(got.authError && got.marketFinished && decoder.authError && decoder.marketFinished);
  assert.ok(decoder.basePriceQueue.has(1424), 'a decoder-reported base price change is queued for the master re-read');
});

// ---------------------------------------------------------------------------------------
// Boot data (base prices, exceptional special quotes), applied as the vendor client does.

test('boot base prices change the master limits; counts are reported', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const plain = await Decoder.create(assets);
  const toyota = plain.master.find(m => m.code === '7203');
  assert.deepEqual(plain.boot, { exceptional_sq: 0, base_prices: 0, applied: 0 });
  const boot = {
    base_prices: [{ issue_code: '7203', base_price10: 123450, limit_down10: 93450, limit_up10: 153450 }],
    exceptional_sq: [{ buy_sell: 'B', jump_range: 10, jump_sec: 180, issue_code: '7203', quote_limit_down: 9900, quote_limit_up: 10400 }],
  };
  const decoder = await Decoder.create(assets, { boot });
  const changed = decoder.master.find(m => m.code === '7203');
  assert.deepEqual(decoder.boot, { exceptional_sq: 1, base_prices: 1, applied: 1 });
  assert.equal(changed.limit_up10, 153450);
  assert.equal(changed.limit_down10, 93450);
  assert.notEqual(changed.limit_up10, toyota.limit_up10);
  const others = decoder.master.filter(m => m.code !== '7203').every((m, i) => m.limit_up10 === plain.master.filter(p => p.code !== '7203')[i].limit_up10);
  assert.ok(others, 'only the named issue changed');
  assert.equal(decoder.applyBasePrice(), 0, 'nothing is queued afterwards');
});

test('malformed or unknown boot data fails loudly; a special quote for a lettered code is listed, not sent',
  { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const base = { issue_code: '7203', base_price10: 1, limit_down10: 1, limit_up10: 2 };
  await assert.rejects(Decoder.create(assets, { boot: { base_prices: [{ ...base, issue_code: '0000' }] } }),
    /base_prices names issue code 0000, which is not in the master/);
  await assert.rejects(Decoder.create(assets, { boot: { base_prices: [{ ...base, limit_up10: undefined }] } }),
    /Boot limit_up10 is not a number/);
  // How the decoder takes 130A is not known; failing would stop every session whose boot names one.
  const decoder = await Decoder.create(assets, { boot: { exceptional_sq: [{ buy_sell: 'S', jump_range: 1, jump_sec: 1,
    issue_code: '130A', quote_limit_down: 1, quote_limit_up: 2 }] } });
  assert.deepEqual(decoder.boot, { exceptional_sq: 0, base_prices: 0, applied: 0, skipped_exceptional_sq: ['130A'] });
});

test('a host\'s master check runs before boot data is matched to the master', { skip: !cache }, async () => {
  // A misread master would otherwise fail on its first boot base price, hiding the real problem.
  let checked = 0;
  const boot = { base_prices: [{ issue_code: '0000', base_price10: 1000, limit_down10: 500, limit_up10: 1500 }] };
  await assert.rejects(Decoder.create(loadAssets(cache), { boot, checkMaster: master => {
    checked = master.length;
    throw new Error('stock master layout not recognised');
  } }), /layout not recognised/);
  assert.ok(checked > 1000);
  await assert.rejects(Decoder.create(loadAssets(cache), { boot }), /issue code 0000, which is not in the master/);
});

// A master file record as the demo's and SBI's files hold them (self-authored values).
function masterRecord({ code, base, up, down, type = '0111', lot }) {
  const r = Buffer.alloc(180, 0x20);
  r.writeUInt32LE(0x1e000018, 0); r.writeUInt32LE(base, 4); r.writeUInt32LE(up, 8); r.writeUInt32LE(down, 12);
  r.write(type, 40, 'latin1'); r.write(code, 50, 'latin1');
  r.write(String(lot).padStart(15), 124, 'latin1');
  return r;
}

// A record as an 18000 decoder writes it: the code as eight bytes of text, then the demo's fields four bytes on.
function decoderRecord({ code, tick = 1, base, up, down, lot, type = 111, name = '' }) {
  const r = Buffer.alloc(148);
  r.write(code, 0, 'latin1'); r.writeUInt32LE(tick, 8);
  r.writeUInt32LE(base, 12); r.writeUInt32LE(up, 16); r.writeUInt32LE(down, 20);
  r.writeBigUInt64LE(BigInt(lot), 24); r.writeUInt32LE(type, 32);
  const text = Buffer.from(name, 'utf8'); r.writeUInt32LE(text.length, 68); text.copy(r, 72);
  return r;
}

test('master file records: header prices and fixed-width text', () => {
  const records = [
    masterRecord({ code: '167A', base: 1700, up: 2200, down: 1200, lot: 100 }),
    masterRecord({ code: '25935', base: 2350, up: 3050, down: 1650, type: '0115', lot: 1 }),
  ];
  const expected = [
    { issue_id: 0, code: '167A', base_price10: 1700, limit_up10: 2200, limit_down10: 1200, lot_size: 100, issue_type: 111 },
    { issue_id: 1, code: '25935', base_price10: 2350, limit_up10: 3050, limit_down10: 1650, lot_size: 1, issue_type: 115 },
  ];
  assert.deepEqual(parseMasterFile(Buffer.concat(records), 2), expected);
  // Records are counted from the end, so a preamble (the demo's file has one) is skipped.
  assert.deepEqual(parseMasterFile(Buffer.concat([Buffer.alloc(444, 7), ...records]), 2), expected);
  assert.ok(Number.isNaN(parseMasterFile(masterRecord({ code: '1301', base: 1, up: 2, down: 0, lot: 'x' }), 1)[0].lot_size));
  assert.throws(() => parseMasterFile(Buffer.concat(records), 3), /cannot hold 3 issues/);
  assert.throws(() => parseMasterFile(Buffer.concat(records), 0), /cannot hold 0 issues/);
});

test('the demo decoder\'s master agrees with its master file on every issue, and a disagreement is refused',
  { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const { master } = await Decoder.create(assets);
  assert.equal(master.length, 4131);
  assert.doesNotThrow(() => checkMasterFile(master, assets['master.dat']));
  const file = Buffer.from(assets['master.dat']);
  const at = file.length - (master.length - 5) * 180;   // issue 5's record
  file.writeUInt32LE(file.readUInt32LE(at + 4) + 10, at + 4);
  assert.throws(() => checkMasterFile(master, file),
    /disagrees with its master file on 1 of 4131 issues \(issue 5: base_price10 \d+, file \d+\); its record layout may have changed/);
});

test('an 18000 master: text codes, the decoder\'s own names and tick types, limits re-read after a change', () => {
  // Just enough of a decoder to load a master: its records are the 18000 layout, at the buffer it is given.
  const issues = [
    { code: '1301', base: 52500, up: 62500, down: 42500, lot: 100, name: '極洋' },
    { code: '167A', tick: 3, base: 32300, up: 39300, down: 25300, lot: 100, name: 'リョーサン菱洋' },
    { code: '9418', base: 18140, up: 22140, down: 14140, lot: 100, name: 'Ｕ－ＮＥＸＴ' },
  ];
  const heap = new Uint8Array(4 * 1024 * 1024);
  const fake = { addFunction: () => 0, _initialize: () => 1, _malloc: () => 0, HEAPU8: heap, _push() {},
    _applyBasePriceQueue() {}, _stockCount: () => issues.length, _getDate: () => 20260311,
    _getStockMaster: (id, buf, size) => { assert.equal(size, issues.length * 148); heap.set(Buffer.concat(issues.map(decoderRecord)), buf); } };
  const file = Buffer.concat(issues.map(i => masterRecord(i)));
  const assets = { 'master.dat': file, 'snapshot.dat': Buffer.alloc(0) };
  const decoder = new Decoder(fake, assets, { protocolVersion: 18000 });
  assert.deepEqual(decoder.master[1], { issue_id: 1, code: '167A', tick_type: 3, base_price10: 32300, limit_up10: 39300,
    limit_down10: 25300, lot_size: 100, issue_type: 111, name: 'リョーサン菱洋' });
  assert.equal(decoder.master[2].name, 'Ｕ－ＮＥＸＴ', 'a name that starts like a market prefix is the decoder\'s, whole');
  issues[2].up += 1000; issues[2].down += 1000;
  decoder.basePriceQueue.add(2);
  assert.equal(decoder.applyBasePrice(), 1);
  assert.deepEqual([decoder.master[2].limit_down10, decoder.master[2].limit_up10], [15140, 23140]);
  // A file that does not line up with the decoder's issues is refused, not paired with the wrong codes.
  issues[2].up -= 1000; issues[2].down -= 1000;
  assert.throws(() => new Decoder(fake, { ...assets, 'master.dat': Buffer.concat([file.subarray(180), file.subarray(0, 180)]) },
    { protocolVersion: 18000 }), /disagrees with its master file on 3 of 3 issues \(issue 0: code "1301", file "167A"\)/);
  assert.throws(() => new Decoder(fake, assets, { protocolVersion: 17000 }), /layout for protocol 17000 is not known/);
});
