'use strict';
// Runs briskapi's Decoder on a cached copy of SBI's 18000 decoder and master (sbi_master_layout.py
// fetches them), with no snapshot. Creating it reads the master at MASTER_LAYOUT[18000] and checks every
// record against the master file; then a pushed limit change is read back from the decoder's records.
// Then the session schedule and catch-up from the same capture: started with the boot's schedule_info,
// the decoder keeps its price bars, takes the catch-up reply and returns a readable view for every
// caught-up issue; briskapi's catch-up request for those issues is the one the vendor client sent.
// The same start without the schedule is reported for comparison (no price bars, no readable view).
// Prints one JSON report and exits 1 on any failure.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert');
const { Decoder, parseStockView } = require('../../briskapi/decoder/decoder.cjs');
const { checkAbi, checkMaster, CATCH_UP_FORMATS, DEFAULT_CATCH_UP } = require('../../briskapi/decoder/sbi.cjs');

// Start a decoder, push the catch-up reply and read back what it did to the requested issues.
async function catchUp(assets, sent, reply, schedule) {
  const decoder = await Decoder.create(assets, { protocolVersion: 18000, check: checkAbi,
    checkMaster: master => checkMaster(master, 18000), schedule });
  const before = decoder.frameNumbers();
  decoder.push(reply);
  const after = decoder.frameNumbers();
  const ids = sent.issue_ids;
  const advanced = ids.filter((id, k) => after[id] - before[id] === sent.update_numbers_to[k] - sent.update_numbers_from[k]).length;
  // The stock view is what the schedule shapes (its price bars come first): it must read back the
  // decoder's own frame number. With no snapshot under the catch-up, the busiest issues' updates have no
  // starting state, and the decoder's own arithmetic can trap on them (a WebAssembly RuntimeError); those
  // are counted apart from views that read back wrong. A full quote also reads order-book rows, which
  // are incomplete for the same reason, so quotes are reported, not required. Views come first: a trap
  // may leave the decoder unusable.
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
  for (const id of ids) {
    try { decoder.quote(id); quotes++; } catch (e) { quoteError = quoteError || e.message; }
  }
  return { price_bars: decoder.ohlc, issues: ids.length, advanced_by_the_requested_updates: advanced, views_with_the_decoders_frame: views,
    views_read_wrong: mismatches, decoder_traps: traps, full_quotes: quotes, ...(viewError ? { first_view_error: viewError } : {}), ...(quoteError ? { first_quote_error: quoteError } : {}) };
}

async function main(cache) {
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
  console.log(JSON.stringify(report, null, 2));
  const scheduled = report.catch_up.with_schedule;
  if (!report.limits_read_back || !requestMatches || scheduled.price_bars !== 66
    || scheduled.advanced_by_the_requested_updates !== scheduled.issues || scheduled.views_read_wrong !== 0
    || scheduled.views_with_the_decoders_frame < 0.99 * scheduled.issues) {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main(process.argv[2]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
