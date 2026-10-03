import { afterEach, describe, expect, it, vi } from 'vitest';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { ArcClient, ArcConnectionError, ArcRequestError } from './client';
import { ClientFrameSchema, ServerFrameSchema } from './gen/wire_pb';

class FakeSocket {
  static OPEN = 1; static CLOSED = 3;
  readyState = 0; binaryType = '';
  onopen?: () => void; onclose?: () => void; onerror?: () => void; onmessage?: (event: { data: ArrayBuffer }) => void;
  sent: Uint8Array[] = [];
  send(data: ArrayBuffer | Uint8Array) { this.sent.push(new Uint8Array(data as ArrayBuffer)); }
  open() { this.readyState = FakeSocket.OPEN; this.onopen?.(); }
  close() { this.readyState = FakeSocket.CLOSED; this.onclose?.(); }
  receive(frame: object) { this.onmessage?.({ data: Uint8Array.from(toBinary(ServerFrameSchema, create(ServerFrameSchema, frame as never))).buffer }); }
}
let sockets: FakeSocket[] = [];
const factory = () => { const socket = new FakeSocket(); sockets.push(socket); return socket as unknown as WebSocket; };
afterEach(() => { sockets = []; vi.useRealTimers(); });

describe('ArcClient protobuf transport', () => {
  it('encodes creation, forks, model menus, and thinking through correlated read requests', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const models = client.listModels();
    await Promise.resolve(); sockets[0].open(); await Promise.resolve();
    let request = fromBinary(ClientFrameSchema, sockets[0].sent.at(-1)!);
    expect(request.msg.case).toBe('listModels');
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'modelList', value: { choices: [{ name: 'preset', role: 1 }] } } });
    await expect(models).resolves.toMatchObject([{ name: 'preset' }]);
    const creation = client.createSession('arc', 'preset');
    await Promise.resolve();
    request = fromBinary(ClientFrameSchema, sockets[0].sent.at(-1)!);
    expect(request.msg).toMatchObject({ case: 'createSession', value: { role: 1, project: 'arc', choice: 'preset' } });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'messageAccepted', value: { sessionId: 'new' } } });
    await expect(creation).resolves.toMatchObject({ sessionId: 'new' });
    const fork = client.forkSession('new', 9007199254740993n, 'other');
    await Promise.resolve();
    request = fromBinary(ClientFrameSchema, sockets[0].sent.at(-1)!);
    expect(request.msg).toMatchObject({ case: 'forkSession', value: { sessionId: 'new', forkPoint: 9007199254740993n, choice: 'other' } });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'messageAccepted', value: { sessionId: 'fork' } } });
    await expect(fork).resolves.toMatchObject({ sessionId: 'fork' });
    const thinking = client.setSessionThinking('fork', 'high');
    await Promise.resolve();
    request = fromBinary(ClientFrameSchema, sockets[0].sent.at(-1)!);
    expect(request.msg).toMatchObject({ case: 'setSessionThinking', value: { sessionId: 'fork', thinking: 'high' } });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'sessionStatus', value: { sessionId: 'fork', effectiveThinking: 'high' } } });
    await expect(thinking).resolves.toMatchObject({ effectiveThinking: 'high' });
    client.close();
  });

  it('correlates binary requests and returns decoded results', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const promise = client.listSessions(); await Promise.resolve(); sockets[0].open(); await Promise.resolve();
    const request = fromBinary(ClientFrameSchema, sockets[0].sent[0]);
    expect(request.requestId).toBeTypeOf('bigint');
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'sessionList', value: { sessions: [{ id: 's1', title: 'Hi' }] } } });
    await expect(promise).resolves.toMatchObject([{ id: 's1', title: 'Hi' }]);
    client.close();
  });

  it('streams a send on its own socket through acknowledgement to terminal frame', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const onFrame = vi.fn();
    const result = client.sendMessage(undefined, 'hello', onFrame);
    expect(sockets).toHaveLength(1);
    sockets[0].open(); await Promise.resolve();
    const request = fromBinary(ClientFrameSchema, sockets[0].sent[0]);
    expect(request.msg.case).toBe('sendMessage');
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'messageAccepted', value: { sessionId: 's1' } } });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'delta', value: { sessionId: 's1', text: 'Hi' } } });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'streamEnd', value: { sessionId: 's1' } } });
    await expect(result).resolves.toMatchObject({ sessionId: 's1' });
    expect(onFrame).toHaveBeenCalledTimes(3);
  });

  it('expires accepted streams after 15 minutes idle, resetting on deltas without a total timeout', async () => {
    vi.useFakeTimers();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const result = client.sendMessage('s1', 'long task', () => {});
    sockets[0].open();
    await Promise.resolve();
    const request = fromBinary(ClientFrameSchema, sockets[0].sent[0]);
    const receiveDelta = (text: string) => sockets[0].receive({
      requestId: request.requestId,
      msg: { case: 'delta', value: { sessionId: 's1', text } },
    });
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'messageAccepted', value: { sessionId: 's1' } } });
    await vi.advanceTimersByTimeAsync(14 * 60_000);
    receiveDelta('still working');
    await vi.advanceTimersByTimeAsync(14 * 60_000);
    receiveDelta('still working');
    expect(sockets[0].readyState).toBe(FakeSocket.OPEN);
    await vi.advanceTimersByTimeAsync(15 * 60_000 - 1);
    expect(sockets[0].readyState).toBe(FakeSocket.OPEN);
    const rejected = expect(result).rejects.toMatchObject({
      message: 'Reply observation idle; refresh history',
      accepted: true,
    });
    await vi.advanceTimersByTimeAsync(1);
    await rejected;
    expect(sockets[0].readyState).toBe(FakeSocket.CLOSED);
  });

  it('multiplexes notifications independently of reads', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const callback = vi.fn(); const subscribed = client.subscribe(callback);
    await Promise.resolve(); sockets[0].open(); await Promise.resolve(); const request = fromBinary(ClientFrameSchema, sockets[0].sent[0]); await subscribed;
    sockets[0].receive({ requestId: request.requestId, msg: { case: 'notification', value: { event: { case: 'sessionAppended', value: { sessionId: 's1' } } } } });
    expect(callback).toHaveBeenCalledOnce(); client.close();
  });

  it('rejects server failures distinctly and marks disconnected sends uncertain', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const read = client.listJobs(); await Promise.resolve(); sockets[0].open(); await Promise.resolve(); const readFrame = fromBinary(ClientFrameSchema, sockets[0].sent[0]);
    sockets[0].receive({ requestId: readFrame.requestId, msg: { case: 'error', value: { code: 'oops', msg: 'no' } } });
    await expect(read).rejects.toBeInstanceOf(ArcRequestError);
    const send = client.sendMessage('s1', 'hello', () => {}); sockets[1].open(); sockets[1].close();
    await expect(send).rejects.toMatchObject({ accepted: 'unknown' });
  });

  it('disconnects and clears reads after malformed frames', async () => {
    const onDisconnect = vi.fn(); const client = new ArcClient('wss://arc/arc', { socketFactory: factory, onDisconnect });
    const pending = client.listSessions(); await Promise.resolve(); sockets[0].open(); await Promise.resolve(); sockets[0].onmessage?.({ data: new Uint8Array([255]).buffer });
    await expect(pending).rejects.toBeInstanceOf(ArcConnectionError);
    expect(onDisconnect).toHaveBeenCalledOnce();
    client.close();
  });

  it('shares a connecting socket and correlates concurrent reads out of order', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const sessions = client.listSessions();
    const jobs = client.listJobs();
    await Promise.resolve();
    expect(sockets).toHaveLength(1);
    sockets[0].open();
    await Promise.resolve();
    const requests = sockets[0].sent.map((bytes) => fromBinary(ClientFrameSchema, bytes));
    expect(requests).toHaveLength(2);
    sockets[0].receive({ requestId: requests[1].requestId, msg: { case: 'jobList', value: { jobs: [{ sessionId: 'job' }] } } });
    sockets[0].receive({ requestId: requests[0].requestId, msg: { case: 'sessionList', value: { sessions: [{ id: 'session' }] } } });
    await expect(jobs).resolves.toMatchObject([{ sessionId: 'job' }]);
    await expect(sessions).resolves.toMatchObject([{ id: 'session' }]);
    client.close();
  });

  it('close settles an in-progress connect without reporting a disconnect', async () => {
    const onDisconnect = vi.fn();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory, onDisconnect });
    const connecting = client.connect();
    client.close();
    await expect(connecting).rejects.toBeInstanceOf(ArcConnectionError);
    expect(onDisconnect).not.toHaveBeenCalled();
  });

  it('deliberate close of an open connection does not report disconnect', async () => {
    const onDisconnect = vi.fn();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory, onDisconnect });
    const connecting = client.connect();
    sockets[0].open();
    await connecting;
    client.close();
    expect(onDisconnect).not.toHaveBeenCalled();
  });

  it('times out a connection handshake and reports transport failure', async () => {
    vi.useFakeTimers();
    const onDisconnect = vi.fn();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory, onDisconnect });
    const connecting = client.connect();
    const rejected = expect(connecting).rejects.toBeInstanceOf(ArcConnectionError);
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(onDisconnect).toHaveBeenCalledOnce();
  });

  it('close stops every send socket and preserves acceptance certainty', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const acceptedSend = client.sendMessage('s1', 'accepted', () => {});
    const unknownSend = client.sendMessage('s2', 'unknown', () => {});
    sockets[0].open(); sockets[1].open();
    await Promise.resolve();
    const acceptedRequest = fromBinary(ClientFrameSchema, sockets[0].sent[0]);
    sockets[0].receive({ requestId: acceptedRequest.requestId, msg: { case: 'messageAccepted', value: { sessionId: 's1' } } });
    client.close();
    await expect(acceptedSend).rejects.toMatchObject({ accepted: true });
    await expect(unknownSend).rejects.toMatchObject({ accepted: 'unknown' });
    expect(sockets[0].readyState).toBe(FakeSocket.CLOSED);
    expect(sockets[1].readyState).toBe(FakeSocket.CLOSED);
  });

  it('main connection loss closes send observation and ignores late send events', async () => {
    const onFrame = vi.fn();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const connected = client.connect();
    sockets[0].open();
    await connected;
    const send = client.sendMessage('s1', 'hello', onFrame);
    sockets[1].open();
    const request = fromBinary(ClientFrameSchema, sockets[1].sent[0]);
    sockets[0].close();
    await expect(send).rejects.toMatchObject({ accepted: 'unknown' });
    expect(sockets[1].readyState).toBe(FakeSocket.CLOSED);
    sockets[1].receive({ requestId: request.requestId, msg: { case: 'messageAccepted', value: { sessionId: 's1' } } });
    expect(onFrame).not.toHaveBeenCalled();
    client.close();
  });

  it('rejects text frames and disconnects the main connection', async () => {
    const onDisconnect = vi.fn();
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory, onDisconnect });
    const read = client.listJobs(); await Promise.resolve(); sockets[0].open(); await Promise.resolve();
    sockets[0].onmessage?.({ data: 'not binary' as unknown as ArrayBuffer });
    await expect(read).rejects.toBeInstanceOf(ArcConnectionError);
    expect(client.isConnected).toBe(false);
    expect(onDisconnect).toHaveBeenCalledOnce();
  });

  it('ignores stale socket close events after a replacement connection', async () => {
    const client = new ArcClient('wss://arc/arc', { socketFactory: factory });
    const first = client.connect(); sockets[0].open(); await first;
    sockets[0].close();
    const second = client.connect(); sockets[1].open(); await second;
    sockets[0].onclose?.();
    expect(client.isConnected).toBe(true);
    client.close();
  });
});
