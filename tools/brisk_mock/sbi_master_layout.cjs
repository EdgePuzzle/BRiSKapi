'use strict';
// Runs briskapi's Decoder on a cached copy of SBI's 18000 decoder and master (sbi_master_layout.py
// fetches them), with no snapshot. Creating it reads the master at MASTER_LAYOUT[18000] and checks every
// record against the master file; then a pushed limit change is read back from the decoder's records.
// Prints one JSON report and exits 1 on any failure.
const fs = require('node:fs');
const path = require('node:path');
const { Decoder } = require('../../briskapi/decoder/decoder.cjs');
const { checkAbi, checkMaster } = require('../../briskapi/decoder/sbi.cjs');

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
  console.log(JSON.stringify(report, null, 2));
  if (!report.limits_read_back) process.exitCode = 1;
}

if (require.main === module) {
  main(process.argv[2]).catch(error => { console.error(error.message); process.exitCode = 1; });
}
