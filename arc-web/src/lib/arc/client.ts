import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { ClientFrameSchema, ServerFrameSchema, type ClientFrame, type ServerFrame, type SessionInfo, type SessionHistory, type JobInfo, type Notification, type StreamEnd, type SessionList, type JobList, type ProjectInfo, type ProjectList, type ModelChoice, type ModelList, type MessageAccepted, type SessionStatus } from './gen/wire_pb';
import { SessionRole } from './gen/events_pb';

export class ArcRequestError extends Error {
  constructor(readonly code: string, message: string) { super(message); this.name = 'ArcRequestError'; }
}
export class ArcConnectionError extends Error {
  constructor(message: string, readonly accepted: boolean | 'unknown' = false) { super(message); this.name = 'ArcConnectionError'; }
}
type Pending = { resolve: (value: ServerFrame) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> };
type Options = { onDisconnect?: (error: ArcConnectionError) => void; socketFactory?: (url: string) => WebSocket };
let nextId = 1n;
const REQUEST_TIMEOUT = 15_000;
const CONNECT_TIMEOUT = 10_000;
const SEND_IDLE_TIMEOUT = 15 * 60_000;

export class ArcClient {
  private socket?: WebSocket;
  private connecting?: { socket: WebSocket; promise: Promise<void>; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> };
  private pending = new Map<bigint, Pending>();
  private subscribers = new Map<bigint, (notification: Notification) => void>();
  private sendSockets = new Map<WebSocket, (error: ArcConnectionError) => void>();
  private factory: (url: string) => WebSocket;
  private explicitlyClosed = false;

  constructor(private endpoint: string, private options: Options = {}) {
    this.factory = options.socketFactory ?? ((url) => new WebSocket(url));
  }

  get isConnected(): boolean { return this.socket?.readyState === WebSocket.OPEN; }

  connect(): Promise<void> {
    if (this.isConnected) return Promise.resolve();
    if (this.connecting) return this.connecting.promise;
    this.explicitlyClosed = false;
    const ws = this.factory(this.endpoint);
    this.socket = ws;
    ws.binaryType = 'arraybuffer';
    let resolveConnection!: () => void;
    let rejectConnection!: (error: Error) => void;
    const promise = new Promise<void>((resolve, reject) => {
      resolveConnection = resolve;
      rejectConnection = reject;
    });
    const timer = setTimeout(() => {
      if (this.socket !== ws) return;
      this.finishConnect(ws, new ArcConnectionError('WebSocket connection timed out'));
      ws.close();
    }, CONNECT_TIMEOUT);
    this.connecting = { socket: ws, promise, reject: rejectConnection, timer };
    ws.onopen = () => {
      if (this.socket !== ws) return;
      clearTimeout(timer);
      this.connecting = undefined;
      resolveConnection();
    };
    ws.onerror = () => {
      if (this.socket !== ws) return;
      this.finishConnect(ws, new ArcConnectionError('WebSocket connection failed'));
      ws.close();
    };
    ws.onclose = () => {
      if (this.socket !== ws) return;
      this.socket = undefined;
      const error = new ArcConnectionError('WebSocket disconnected');
      this.finishConnect(ws, error);
      this.failPending(error);
      this.stopSends(error);
      this.subscribers.clear();
      if (!this.explicitlyClosed) this.options.onDisconnect?.(error);
    };
    ws.onmessage = ({ data }) => { if (this.socket === ws) this.receive(data); };
    return promise;
  }

  close(): void {
    this.explicitlyClosed = true;
    const error = new ArcConnectionError('Client closed', false);
    const ws = this.socket;
    if (ws) {
      this.socket = undefined;
      this.finishConnect(ws, error);
      ws.close();
    }
    this.stopSends(error);
    this.failPending(error);
    this.subscribers.clear();
  }

  listSessions(): Promise<SessionInfo[]> {
    return this.request('listSessions', {}, 'sessionList').then((reply) => (reply as SessionList).sessions);
  }

  fetchHistory(sessionId: string): Promise<SessionHistory> {
    return this.request('fetchHistory', { sessionId }, 'sessionHistory') as Promise<SessionHistory>;
  }

  listJobs(): Promise<JobInfo[]> {
    return this.request('listJobs', {}, 'jobList').then((reply) => (reply as JobList).jobs);
  }

  listProjects(): Promise<ProjectInfo[]> {
    return this.request('listProjects', {}, 'projectList').then((reply) => (reply as ProjectList).projects);
  }

  listModels(): Promise<ModelChoice[]> {
    return this.request('listModels', {}, 'modelList').then((reply) => (reply as ModelList).choices);
  }

  createSession(project: string, choice: string): Promise<MessageAccepted> {
    return this.request('createSession', { role: SessionRole.CHAT, project, choice }, 'messageAccepted');
  }

  forkSession(sessionId: string, forkPoint: bigint, choice: string): Promise<MessageAccepted> {
    return this.request('forkSession', { sessionId, forkPoint, choice }, 'messageAccepted');
  }

  fetchStatus(sessionId: string): Promise<SessionStatus> {
    return this.request('fetchStatus', { sessionId }, 'sessionStatus');
  }

  setSessionThinking(sessionId: string, thinking: string): Promise<SessionStatus> {
    return this.request('setSessionThinking', { sessionId, thinking }, 'sessionStatus');
  }

  async subscribe(callback: (notification: Notification) => void): Promise<() => void> {
    await this.connect();
    const id = nextId++;
    this.subscribers.set(id, callback);
    try { this.transmit(create(ClientFrameSchema, { requestId: id, msg: { case: 'subscribe', value: {} } })); }
    catch (error) { this.subscribers.delete(id); throw error; }
    return () => this.subscribers.delete(id);
  }

  sendMessage(sessionId: string | undefined, content: string, onFrame: (frame: ServerFrame) => void): Promise<StreamEnd> {
    const ws = this.factory(this.endpoint);
    ws.binaryType = 'arraybuffer';
    const id = nextId++;
    return new Promise((resolve, reject) => {
      let sent = false;
      let accepted = false;
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.sendSockets.delete(ws);
        ws.close();
      };
      const fail = (error: ArcConnectionError) => {
        if (settled) return;
        finish();
        reject(new ArcConnectionError(error.message, accepted ? true : sent ? 'unknown' : false));
      };
      timer = setTimeout(() => fail(new ArcConnectionError('Send acknowledgement timed out', 'unknown')), REQUEST_TIMEOUT);
      this.sendSockets.set(ws, fail);
      ws.onerror = () => fail(new ArcConnectionError('Send connection failed'));
      ws.onclose = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.sendSockets.delete(ws);
        reject(new ArcConnectionError('Send connection lost', accepted ? true : sent ? 'unknown' : false));
      };
      ws.onopen = () => {
        if (settled) return;
        try {
          ws.send(Uint8Array.from(toBinary(ClientFrameSchema, create(ClientFrameSchema, {
            requestId: id,
            msg: { case: 'sendMessage', value: { sessionId: sessionId ?? '', content } },
          }))).buffer);
          sent = true;
        } catch { fail(new ArcConnectionError('Could not send message')); }
      };
      ws.onmessage = ({ data }) => {
        if (settled) return;
        let frame: ServerFrame;
        try { frame = fromBinary(ServerFrameSchema, decodeBinary(data)); }
        catch { fail(new ArcConnectionError('Malformed server frame')); return; }
        if (frame.requestId !== id || !frame.msg.case) return;
        if (frame.msg.case === 'error') {
          if (!settled) { finish(); reject(new ArcRequestError(frame.msg.value.code, frame.msg.value.msg)); }
          return;
        }
        if (frame.msg.case === 'messageAccepted') accepted = true;
        if (accepted) {
          clearTimeout(timer);
          timer = setTimeout(() => fail(new ArcConnectionError('Reply observation idle; refresh history', true)), SEND_IDLE_TIMEOUT);
        }
        try { onFrame(frame); } catch (error) { finish(); reject(error); return; }
        if (frame.msg.case === 'streamEnd' && !settled) { finish(); resolve(frame.msg.value); }
      };
    });
  }

  private finishConnect(ws: WebSocket, error: Error): void {
    const pending = this.connecting;
    if (!pending || pending.socket !== ws) return;
    clearTimeout(pending.timer);
    this.connecting = undefined;
    pending.reject(error);
  }

  private request(caseName: 'listSessions', value: object, response: 'sessionList'): Promise<SessionList>;
  private request(caseName: 'listJobs', value: object, response: 'jobList'): Promise<JobList>;
  private request(caseName: 'listProjects', value: object, response: 'projectList'): Promise<ProjectList>;
  private request(caseName: 'fetchHistory', value: object, response: 'sessionHistory'): Promise<SessionHistory>;
  private request(caseName: 'listModels', value: object, response: 'modelList'): Promise<ModelList>;
  private request(caseName: 'createSession' | 'forkSession', value: object, response: 'messageAccepted'): Promise<MessageAccepted>;
  private request(caseName: 'fetchStatus' | 'setSessionThinking', value: object, response: 'sessionStatus'): Promise<SessionStatus>;
  private async request(caseName: 'listSessions' | 'listJobs' | 'listProjects' | 'fetchHistory' | 'listModels' | 'createSession' | 'forkSession' | 'fetchStatus' | 'setSessionThinking', value: object, response: 'sessionList' | 'jobList' | 'projectList' | 'sessionHistory' | 'modelList' | 'messageAccepted' | 'sessionStatus'): Promise<SessionList | JobList | ProjectList | SessionHistory | ModelList | MessageAccepted | SessionStatus> {
    await this.connect();
    const id = nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        const error = new ArcConnectionError('Request timed out', false);
        reject(error);
        this.disconnect(error);
      }, REQUEST_TIMEOUT);
      this.pending.set(id, { resolve: (frame) => {
        if (frame.msg.case !== response) reject(new ArcConnectionError(`Unexpected response: ${frame.msg.case}`, false));
        else resolve(frame.msg.value as SessionList | JobList | ProjectList | SessionHistory | ModelList | MessageAccepted | SessionStatus);
      }, reject, timer });
      try { this.transmit(create(ClientFrameSchema, { requestId: id, msg: { case: caseName, value } })); }
      catch (error) { clearTimeout(timer); this.pending.delete(id); reject(error); }
    });
  }

  private transmit(frame: ClientFrame): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) throw new ArcConnectionError('Not connected');
    this.socket.send(Uint8Array.from(toBinary(ClientFrameSchema, frame)).buffer);
  }

  private receive(data: unknown): void {
    let frame: ServerFrame;
    try { frame = fromBinary(ServerFrameSchema, decodeBinary(data)); }
    catch { this.disconnect(new ArcConnectionError('Malformed server frame', false)); return; }
    const callback = this.subscribers.get(frame.requestId);
    if (callback && frame.msg.case === 'notification') { callback(frame.msg.value); return; }
    const pending = this.pending.get(frame.requestId);
    if (!pending || !frame.msg.case) return;
    if (frame.msg.case === 'error') {
      clearTimeout(pending.timer);
      this.pending.delete(frame.requestId);
      pending.reject(new ArcRequestError(frame.msg.value.code, frame.msg.value.msg));
      return;
    }
    clearTimeout(pending.timer);
    this.pending.delete(frame.requestId);
    pending.resolve(frame);
  }

  private disconnect(error: ArcConnectionError): void {
    const ws = this.socket;
    if (!ws) return;
    this.socket = undefined;
    this.finishConnect(ws, error);
    this.failPending(error);
    this.stopSends(error);
    this.subscribers.clear();
    ws.close();
    if (!this.explicitlyClosed) this.options.onDisconnect?.(error);
  }

  private failPending(error: ArcConnectionError): void {
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(error);
      this.pending.delete(id);
    }
  }

  private stopSends(error: ArcConnectionError): void {
    for (const stop of this.sendSockets.values()) stop(error);
  }
}

function decodeBinary(data: unknown): Uint8Array {
  if (!(data instanceof ArrayBuffer)) throw new TypeError('Expected a binary WebSocket frame');
  return new Uint8Array(data);
}
