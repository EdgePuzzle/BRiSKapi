'use strict';
// The BRiSK link over a real local WebSocket (the unit tests use fake sockets, which accept anything).
// The global WebSocket differs between runtimes in what it accepts: Bun refuses a bare string for
// `protocols` ("Value is not a sequence"), Node takes either. Nothing here touches a network beyond 127.0.0.1.
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const http = require('node:http');
const { Link, resolveProfile } = require('../../briskapi/decoder/engineio.cjs');

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// One frame from the server (unmasked): opcode 2 is binary, 8 is close.
function frame(opcode, payload) {
  const n = payload.length;
  const head = n < 126 ? Buffer.from([0x80 | opcode, n])
    : n < 65536 ? Buffer.from([0x80 | opcode, 126, n >> 8, n & 255])
      : Buffer.concat([Buffer.from([0x80 | opcode, 127]), (b => (b.writeBigUInt64BE(BigInt(n)), b))(Buffer.alloc(8))]);
  return Buffer.concat([head, payload]);
}
const closeFrame = (code, reason = '') => frame(8, Buffer.concat([Buffer.from([code >> 8, code & 255]), Buffer.from(reason)]));

// A server that records what the client asked for and, on /refuse, answers the upgrade with a 502 as the vendor's did.
async function serve() {
  const upgrades = [];
  const sockets = new Set();
  const server = http.createServer((request, response) => response.writeHead(404).end());
  server.on('upgrade', (request, socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    const offered = (request.headers['sec-websocket-protocol'] || '').split(',').map(name => name.trim()).filter(Boolean);
    upgrades.push({ path: request.url, offered, cookie: request.headers.cookie, origin: request.headers.origin });
    if (request.url === '/refuse') return socket.end('HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
    const accept = crypto.createHash('sha1').update(request.headers['sec-websocket-key'] + GUID).digest('base64');
    socket.write(['HTTP/1.1 101 Switching Protocols', 'Upgrade: websocket', 'Connection: Upgrade', `Sec-WebSocket-Accept: ${accept}`,
      ...(offered.includes('flex') ? ['Sec-WebSocket-Protocol: flex'] : []), '', ''].join('\r\n'));
    socket.write(Buffer.concat([frame(2, Buffer.from([1, 2, 3])), frame(2, Buffer.alloc(70000, 7)), closeFrame(1000, 'done')]));
    // The closing handshake: the server ends the connection once the client has answered its Close.
    socket.once('data', () => socket.end());
    setTimeout(() => socket.end(), 2000).unref();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return { port: server.address().port, upgrades, async stop() { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); } };
}

function link(port, path, options = {}) {
  const seen = { binary: [], closes: [], errors: [] };
  const done = new Promise(resolve => {
    const instance = new Link({
      WebSocketImpl: WebSocket,   // live()'s default: the runtime's own
      url: new URL(`ws://127.0.0.1:${port}${path}`), headers: { origin: 'https://sbi.brisk.jp', cookie: 'session_x=v' },
      protocols: 'flex', name: 'Test BRiSK', profile: resolveProfile(), context: async () => ({}), connectTimeoutMs: 5000,
      onBinary: data => seen.binary.push(Buffer.from(data)),
      onClose: close => { seen.closes.push(close); resolve(); },
      onError: error => { seen.errors.push(error); resolve(); },
      ...options,
    });
    seen.link = instance;
  });
  // A link that never reports is a failure to see, not a hang.
  const limit = new Promise((resolve, reject) => setTimeout(() => reject(new Error('the link neither closed nor failed in 8 s')), 8000).unref());
  return { seen, done: Promise.race([done, limit]) };
}

test('over a real socket the link sends its headers and subprotocol and passes binary frames of any size on', { timeout: 15000 }, async () => {
  const server = await serve();
  try {
    const { seen, done } = link(server.port, '/live');
    await done;
    assert.deepEqual(seen.errors, [], seen.errors.map(error => error.message).join('\n'));
    assert.deepEqual(server.upgrades, [{ path: '/live', offered: ['flex'], cookie: 'session_x=v', origin: 'https://sbi.brisk.jp' }]);
    assert.equal(seen.link.socket.protocol, 'flex', 'the server agreed to the subprotocol');
    assert.equal(seen.binary.length, 2);
    assert.deepEqual([...seen.binary[0]], [1, 2, 3]);
    assert.equal(seen.binary[1].length, 70000, 'a frame past 64 KiB (the 8-byte length form) arrives whole');
    assert.ok(seen.binary[1].every(byte => byte === 7));
    assert.deepEqual(seen.closes, [{ code: 1000, reason: 'done' }]);
  } finally { await server.stop(); }
});

test('over a real socket a subprotocol given as a list is asked for the same way', { timeout: 15000 }, async () => {
  const server = await serve();
  try {
    const { seen, done } = link(server.port, '/live', { protocols: ['flex'] });
    await done;
    assert.deepEqual(seen.errors, []);
    assert.deepEqual(server.upgrades.map(upgrade => upgrade.offered), [['flex']]);
    assert.equal(seen.binary.length, 2);
  } finally { await server.stop(); }
});

test('over a real socket an upgrade the server refuses is one error that names the site, and no frames', { timeout: 15000 }, async () => {
  const server = await serve();
  try {
    const { seen, done } = link(server.port, '/refuse');
    await done;
    assert.equal(seen.errors.length, 1);
    assert.match(seen.errors[0].message, /^Test BRiSK WebSocket error: /);
    assert.deepEqual(seen.closes, []);
    assert.deepEqual(seen.binary, []);
  } finally { await server.stop(); }
});
