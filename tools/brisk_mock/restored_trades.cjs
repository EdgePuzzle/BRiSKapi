'use strict';
// A preload for reconnect_memory.cjs, into the host and every worker thread (NODE_OPTIONS="--require ..."): the first time a
// decoder's history is taken after its snapshot load, RESTORED more trades are "restored" through emplace(), as a snapshot
// taken during a live session restores the day so far, so a reconnect has the memory profile of a real one in the main
// decoder and in every worker. The demo's snapshot restores none.
const path = require('node:path');
const { QuoteHistory } = require(path.join(__dirname, '../../briskapi/decoder/decoder.cjs'));
const restored = Number(process.env.RESTORED || 0);
const take = QuoteHistory.prototype.take;
QuoteHistory.prototype.take = function () {
  if (restored && !this.injected && this.qrs.size > 1000) {
    this.injected = true;
    const stocks = [...this.qrs.keys()];
    for (let i = 0; i < restored; i++) this.emplace(stocks[i % stocks.length], 1, 32400000000 + i, 10000, 1, 100, i);
  }
  return take.call(this);
};
