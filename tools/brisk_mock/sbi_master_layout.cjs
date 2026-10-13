'use strict';
// Runs briskapi's Decoder on a cached copy of SBI's 18000 decoder and master (sbi_master_layout.py
// fetches them), with no snapshot. Creating it reads the master at MASTER_LAYOUT[18000] and checks every
// record against the master file; then a pushed limit change is read back from the decoder's records.
// Then the session schedule and catch-up from the same capture: started with the boot's schedule_info,
// the decoder keeps its price bars, takes the catch-up reply and returns a readable view for every
// caught-up issue; briskapi's catch-up request for those issues is the one the vendor client sent.
// The same start without the schedule is reported for comparison (no price bars, no readable view).
// Then the recorded stream: it starts partway into its connection, without the opening message that
// gives each issue's update number, so one is made in the demo's layout (0x88, a 16-bit issue count, a
// 32-bit number per issue: here all 0). As recorded, the host must stop right after the first of its
// 15-byte frames, which leave the decoder unable to find another message, and name it. Without them,
// every message must be accepted and many issues updated (values are relative to the missing snapshot),
// and every server ping answered with itself, as the browser's were.
// Then boot special quotes, a lettered code among them: each must land on its own issue and no other.
// With the demo's cache (download_mock.py) it also loads the 2021 demo's master and snapshot into this
// 2026 decoder: a snapshot's price bars must match the schedule's, so the demo's (TSE closed at 15:00
// then: 60 bars) loads only under 2021's hours, and every quote must equal the demo decoder's own.
// Prints one JSON report and exits 1 on any failure.
const fs = require('node:fs');
const zlib = require('node:zlib');
const path = require('node:path');
const assert = require('node:assert');
const { Decoder, parseStockView } = require('../../briskapi/decoder/decoder.cjs');
const { checkAbi, checkMaster, CATCH_UP_FORMATS, DEFAULT_CATCH_UP } = require('../../briskapi/decoder/sbi.cjs');

// Start a decoder, push the catch-up reply and read back what it did to the requested issues.
async function catchUp(assets, sent, reply, schedule) {
  const decoder = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi,
    checkMaster: master => checkMaster(master, 18000), schedule });
  const before = decoder.frameNumbers();
  // With no snapshot the issues start below the request's `from`; each must advance by its count.
  const lagging = sent.issue_ids.map((issue_id, k) => ({ issue_id, from: before[issue_id],
    to: before[issue_id] + sent.update_numbers_to[k] - sent.update_numbers_from[k] }));
  let exact = true, twice = null;
  try { decoder.catchUp(reply, lagging); } catch { exact = false; }
  const after = decoder.frameNumbers();
  const ids = sent.issue_ids;
  const advanced = ids.filter((id, k) => after[id] - before[id] === sent.update_numbers_to[k] - sent.update_numbers_from[k]).length;
  // The decoder takes the same reply again without complaint; the host's check must not.
  try {
    const again = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi, schedule });
    again.push(reply);
    again.catchUp(reply, lagging);
  } catch (e) { twice = e.message; }
  // The stock view is what the schedule shapes (its price bars come first): it must read back the
  // decoder's own frame number. With no snapshot under the catch-up, some issues' updates take their
  // order quantities below zero, and reading them traps the decoder (a WebAssembly RuntimeError); those
  // are counted apart from views that read back wrong. A full quote also reads order-book rows, which
  // trap far more often for the same reason, so quotes are reported, not required.
  let views = 0, traps = 0, mismatches = 0, quotes = 0, viewError = null, quoteError = null;
  for (const id of ids) {
    try {
      if (!decoder.w._getStockView(decoder.id, id, decoder.buf)) throw new Error('no stock view');
      const view = parseStockView(new Uint32Array(decoder.w.HEAPU8.buffer, decoder.buf, decoder.viewWords), decoder.ohlc, decoder.layoutShift);
      if (view.frame !== after[id] || view.frame > view.max_frame) throw new Error(`frame ${view.frame}, decoder ${after[id]}`);
      views++;
    } catch (e) {
      if (e?.constructor?.name === 'RuntimeError') traps++; else mismatches++;
      viewError = viewError || e.message;
    }
  }
  // Without a snapshot the books are garbage, and the decoder can take seconds to read one, so quotes
  // here leave the ten-level book out (the market-order row is still read).
  decoder.book = () => ({});
  for (const id of ids) {
    try { decoder.quote(id); quotes++; } catch (e) { quoteError = quoteError || e.message; }
  }
  return { price_bars: decoder.ohlc, issues: ids.length, catch_up_exact: exact, ...(twice ? { pushed_twice: twice } : {}),
    advanced_by_the_requested_updates: advanced, views_with_the_decoders_frame: views,
    views_read_wrong: mismatches, decoder_traps: traps, full_quotes: quotes, ...(viewError ? { first_view_error: viewError } : {}), ...(quoteError ? { first_quote_error: quoteError } : {}) };
}

// The opening frame-number message in the demo's layout, which the 2026 builds also accept.
function openingMessage(issues) {
  const message = Buffer.alloc(3 + 4 * issues);
  message[0] = 0x88;
  message.writeUInt16LE(issues, 1);
  return message;
}

// A 15-byte frame the recording holds every 40 s. The decoder can't complete a message from it,
// misreads what follows and decodes nothing more; the browser's decoder kept going, so it may not
// be what the browser was given.
const STALL = '99eb2c6a07adca97a179aaed6de6ad';

// The recorded stream behind a made-up opening message, as recorded or without the frames above.
async function stream(assets, schedule, file, { withoutStalls = false } = {}) {
  const messages = zlib.gunzipSync(file).toString('utf8').trim().split('\n').map(line => JSON.parse(line))
    .filter(event => event.type === 'wire').map(event => Buffer.from(event.payload_base64, 'base64'))
    .filter(message => !withoutStalls || message.toString('hex') !== STALL);
  const pongs = [];
  let numbers = null, heartbeats = 0;
  const decoder = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi, schedule, callbacks: {
    frameNumbers: n => { numbers = n.length; }, heartbeat: () => heartbeats++, send: bytes => pongs.push(bytes.toString('hex')) } });
  decoder.feed(openingMessage(decoder.master.length));
  const opening = numbers === decoder.master.length && decoder.laggingIssues().length === 0;
  decoder.begin();
  const updated = new Set();
  let accepted = 0, changes = 0, error = null;
  for (const message of messages) {
    try { decoder.feed(message); accepted++; } catch (e) { error = e.message; break; }
    const ids = decoder.changed();
    changes += ids.length;
    ids.forEach(id => updated.add(id));
  }
  // Server pings are the heartbeats (0xF0, 0xF2, 0xF4, 0xF6 and an 8-byte clock); each reply must be the ping it answers.
  const pings = messages.filter(m => (m[0] & 0xf9) === 0xf0).map(m => m.toString('hex'));
  const echoed = pongs.filter((pong, k) => pong === pings[k]).length;
  return { messages: messages.length, opening_accepted: opening, accepted, ...(error ? { stopped: error } : {}), quote_changes: changes,
    issues_updated: updated.size, server_pings: pings.length, replies: pongs.length, replies_echoing_their_ping: echoed, heartbeats };
}

// Boot special quotes, each with its own values: one with a lettered code, one at issue 0, a 5-digit
// code and Toyota. This build keeps a special quote as five words behind a pointer at +664 of the stock
// (its own layout, read here only to check placement); every one must sit on its issue and nowhere else.
async function bootSpecialQuotes(assets, schedule) {
  const codes = ['167A', '1301', '25935', '7203'];
  const exceptional_sq = codes.map((issue_code, k) => ({ buy_sell: k % 2 ? 'S' : 'B', jump_range: 10 + k, jump_sec: 180,
    issue_code, quote_limit_down: 900 + k, quote_limit_up: 1100 + k }));
  const decoder = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi, schedule, boot: { exceptional_sq } });
  let placed = 0, elsewhere = 0;
  decoder.master.forEach((m, issue) => {
    const pointer = decoder.w._getStockPointer(decoder.id, issue);
    const u32 = new Uint32Array(decoder.w.HEAPU8.buffer);
    const record = u32[(pointer + 664) >> 2];
    const k = codes.indexOf(m.code);
    if (k < 0) { if (record) elsewhere++; return; }
    const sq = exceptional_sq[k];
    const expected = [sq.buy_sell === 'B' ? 0 : 1, 10 * sq.jump_range, sq.jump_sec, 10 * sq.quote_limit_down, 10 * sq.quote_limit_up];
    if (record && expected.every((v, i) => u32[(record >> 2) + i] === v)) placed++;
  });
  return { sent: decoder.boot.exceptional_sq, on_their_issue: placed, on_other_issues: elsewhere };
}

// TSE's hours on the demo's trading date, 2021-09-27 (microseconds after midnight); there was no
// pre-close period then, so it is set to the close. 9:00-11:30 and 12:30-15:00 give 60 five-minute bars.
const H = 3600e6, MIN = 60e6;
const DEMO_SCHEDULE = { morning_session_pre_open_time: 8 * H, morning_session_open_time: 9 * H,
  morning_session_close_time: 11 * H + 30 * MIN, afternoon_session_pre_open_time: 12 * H + 5 * MIN,
  afternoon_session_open_time: 12 * H + 30 * MIN, afternoon_session_pre_close_time: 15 * H,
  afternoon_session_close_time: 15 * H, sq_jump_interval: 180 };

// The demo's snapshot in this 2026 decoder under 2021's hours, quote for quote against the demo
// decoder; and what the same snapshot gets under 2026's hours and with no schedule.
async function demoSnapshot(decoderAssets, demoCache, schedule2026) {
  const read = name => fs.readFileSync(path.join(demoCache, name));
  const demo = { 'master.dat': read('master.dat'), 'snapshot.dat': read('snapshot.dat') };
  const reference = await Decoder.create({ 'fita.js': read('fita.js'), 'fita.wasm': read('fita.wasm'), ...demo });
  const attempt = async schedule => {
    try {
      return { decoder: await Decoder.create({ ...decoderAssets, ...demo }, { protocolVersion: 18000, check: checkAbi, schedule }) };
    } catch (e) { return { error: e.message.replace(/ \(File:.*$/, '') }; }
  };
  const { decoder, error } = await attempt(DEMO_SCHEDULE);
  const out = { issues: reference.master.length };
  if (decoder) {
    let same = 0, first = null;
    for (let i = 0; i < reference.master.length; i++) {
      // Every field, the issue status (the demo's from _getPortfolio, this build's from its view) and
      // both sides of the book included.
      const a = reference.quote(i), b = decoder.quote(i);
      const differs = Object.keys(a).filter(k => JSON.stringify(a[k]) !== JSON.stringify(b[k]));
      if (!differs.length && reference.master[i].code === decoder.master[i].code) same++; else first = first || `${a.code}: ${differs.join(', ')}`;
    }
    Object.assign(out, { with_2021_hours: 'loaded', price_bars: decoder.ohlc, quotes_equal_to_the_demo_decoder: same,
      ...(first ? { first_difference: first } : {}) });
  } else {
    out.with_2021_hours = error;
  }
  out.with_2026_hours = (await attempt(schedule2026)).error || 'loaded';
  out.without_schedule = (await attempt(undefined)).error || 'loaded';
  return out;
}

async function main(cache, demoCache) {
  const read = name => fs.readFileSync(path.join(cache, name));
  const assets = { 'fita.js': read('fita.js'), 'fita.wasm': read('fita.wasm'), 'master.dat': read('master.dat'),
    'snapshot.dat': Buffer.alloc(0) };
  const decoder = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi,
    checkMaster: master => checkMaster(master, 18000) });
  const tickTypes = {};
  for (const m of decoder.master) tickTypes[m.tick_type] = (tickTypes[m.tick_type] || 0) + 1;
  const toyota = decoder.master.find(m => m.code === '7203');
  const limits = [toyota.limit_down10 + 1000, toyota.limit_up10 + 1000];
  decoder.w._pushBasePrice(decoder.id, toyota.issue_id, toyota.base_price10, ...limits);
  decoder.basePriceQueue.add(toyota.issue_id);
  decoder.applyBasePrice();
  const report = { issues: decoder.master.length, record_bytes: decoder.layout.size, agrees_with_master_file: true,
    tick_types: tickTypes, limits_read_back: toyota.limit_down10 === limits[0] && toyota.limit_up10 === limits[1],
    samples: ['1301', '167A', '25935', '9418'].map(code => decoder.master.find(m => m.code === code)) };
  const sent = JSON.parse(read('stocks_update_request.json'));
  const schedule = JSON.parse(read('schedule_info.json'));
  const reply = read('stocks_update.dat');
  const lagging = sent.issue_ids.map((issue_id, k) => ({ issue_id, from: sent.update_numbers_from[k], to: sent.update_numbers_to[k] }));
  let requestMatches = true;
  try { assert.deepStrictEqual(JSON.parse(CATCH_UP_FORMATS[DEFAULT_CATCH_UP](lagging).body), sent); } catch { requestMatches = false; }
  report.catch_up = { request_matches_the_vendor_client: requestMatches, with_schedule: await catchUp(assets, sent, reply, schedule),
    without_schedule: await catchUp(assets, sent, reply, undefined) };
  report.stream = { as_recorded: await stream(assets, schedule, read('stream.jsonl.gz')),
    without_the_15_byte_frames: await stream(assets, schedule, read('stream.jsonl.gz'), { withoutStalls: true }) };
  report.boot_special_quotes = await bootSpecialQuotes(assets, schedule);
  if (demoCache) report.demo_snapshot = await demoSnapshot(assets, demoCache, schedule);
  console.log(JSON.stringify(report, null, 2));
  const sq = report.boot_special_quotes;
  const sqOk = sq.sent === 4 && sq.on_their_issue === 4 && sq.on_other_issues === 0;
  const demoOk = !demoCache || report.demo_snapshot.quotes_equal_to_the_demo_decoder === report.demo_snapshot.issues;
  // As recorded, the host must stop right after the first 15-byte frame and name it; without those
  // frames every message is taken, and every ping is answered with itself.
  const { as_recorded: r, without_the_15_byte_frames: s } = report.stream;
  const streamOk = r.opening_accepted && r.stopped?.includes(`after a 15-byte frame starting ${STALL}`) && r.accepted < 4000
    && s.opening_accepted && s.accepted === s.messages && s.issues_updated >= 3000 && s.heartbeats === s.server_pings
    && s.server_pings > 0 && s.replies === s.server_pings && s.replies_echoing_their_ping === s.replies;
  const scheduled = report.catch_up.with_schedule;
  if (!report.limits_read_back || !requestMatches || !sqOk || !demoOk || !streamOk || scheduled.price_bars !== 66
    || !scheduled.catch_up_exact || !/Catch-up took issue \d+ to frame \d+, but the stream starts at/.test(scheduled.pushed_twice)
    || scheduled.advanced_by_the_requested_updates !== scheduled.issues || scheduled.views_read_wrong !== 0
    || scheduled.views_with_the_decoders_frame < 0.99 * scheduled.issues) {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main(process.argv[2], process.argv[3]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
