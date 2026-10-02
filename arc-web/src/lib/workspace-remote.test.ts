import { afterEach, describe, expect, it, vi } from 'vitest';
import { ArcConnectionError } from './arc/client';
import { Workspace } from './workspace.svelte';
import type { HostProfile } from './arc/types';

const host: HostProfile = { id: 'remote', name: 'Remote', endpoint: 'wss://arc.test/ws', kind: 'daemon' };
const makeHistory = (content: string) => ({ entries: [{ seq: 1, entry: { case: 'message', value: { source: 2, role: 2, content } } }] });
const defer = <T,>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const tick = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
const store = () => {
  const values = new Map<string, string>();
  return { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key), clear: () => values.clear(), key: () => null, get length() { return values.size; } } as Storage;
};

class FakeClient {
  closed = false;
  sends: string[] = [];
  historyCalls: string[] = [];
  onDisconnect?: (error: ArcConnectionError) => void;
  constructor(
    public sessions: unknown[] = [{ id: 's1', title: 'Session', source: 2 }, { id: 's2', title: 'Second', source: 2 }],
    public historyValue: unknown = makeHistory('from history'),
    private send: (id: string | undefined, text: string, frame: (value: never) => void) => Promise<unknown> = async (_id, _text, frame) => {
      frame({ msg: { case: 'messageAccepted', value: { sessionId: 's1' } } } as never);
      frame({ msg: { case: 'delta', value: { text: 'reply' } } } as never);
      return {};
    }
  ) {}
  async connect() {}
  close() { this.closed = true; }
  async subscribe() {}
  async listSessions() { return this.sessions; }
  async listJobs() { return []; }
  async fetchHistory(id: string) { this.historyCalls.push(id); return typeof this.historyValue === 'function' ? (this.historyValue as (id: string) => Promise<unknown>)(id) : this.historyValue; }
  async sendMessage(id: string | undefined, text: string, frame: (value: never) => void) { this.sends.push(text); return this.send(id, text, frame); }
}

afterEach(() => vi.useRealTimers());

describe('remote Workspace', () => {
  it('loads remote sessions and authoritative history', async () => {
    const client = new FakeClient();
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    expect(workspace.connectionState).toBe('connected');
    expect(workspace.sessions).toMatchObject([{ id: 's1', title: 'Session' }, { id: 's2', title: 'Second' }]);
    expect(workspace.messages).toMatchObject([{ role: 'you', content: 'from history' }]);
    workspace.dispose(); expect(client.closed).toBe(true);
  });

  it('does not display a stale history response after selecting another session', async () => {
    const old = defer<unknown>();
    const client = new FakeClient(undefined, (id: string) => id === 's1' ? old.promise : Promise.resolve(makeHistory('current s2')));
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.selectSession('s2'); await tick();
    old.resolve(makeHistory('stale s1')); await tick();
    expect(workspace.activeSessionId).toBe('s2');
    expect(workspace.messages).not.toContainEqual(expect.objectContaining({ content: 'stale s1' }));
    workspace.dispose();
  });

  it('ignores late responses from a host after switching hosts', async () => {
    const late = defer<unknown>();
    const first = new FakeClient([{ id: 's1', title: 'One', source: 2 }], () => late.promise);
    const second = new FakeClient([{ id: 's2', title: 'Two', source: 2 }], makeHistory('host two'));
    const clients = [first, second];
    const workspace = new Workspace(null, () => clients.shift() as never, host);
    workspace.initialize(); await tick();
    workspace.saveHost('Other', 'wss://other.test/ws'); await tick();
    late.resolve(makeHistory('host one late')); await tick();
    expect(workspace.sessions).toMatchObject([{ id: 's2' }]);
    expect(workspace.messages).not.toContainEqual(expect.objectContaining({ content: 'host one late' }));
    workspace.dispose();
  });

  it('persists pre-ack uncertain input over reload and does not retry it', async () => {
    const storage = store();
    const client = new FakeClient(undefined, makeHistory('before'), (_id, _text) =>
      Promise.reject(new ArcConnectionError('lost', 'unknown')));
    const workspace = new Workspace(storage, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('possibly delivered'); await workspace.send();
    expect(workspace.pendingInputs).toMatchObject([{ content: 'possibly delivered', state: 'uncertain' }]);
    expect(client.sends).toEqual(['possibly delivered']);
    workspace.dispose();
    const replacement = new FakeClient();
    const restored = new Workspace(storage, () => replacement as never, host);
    restored.initialize(); await tick();
    expect(restored.pendingInputs).toMatchObject([{ content: 'possibly delivered', state: 'uncertain' }]);
    expect(replacement.sends).toEqual([]);
    restored.dispose();
  });

  it('restores explicitly rejected input without replacing the next draft', async () => {
    const client = new FakeClient(undefined, makeHistory('history'), (_id, _text) =>
      Promise.reject(new ArcConnectionError('rejected', false)));
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('rejected text'); await workspace.send();
    expect(workspace.pendingInputs).toMatchObject([{ state: 'rejected' }]);
    workspace.setDraft('new draft');
    workspace.restoreInput(workspace.pendingInputs[0].id);
    expect(workspace.draft).toBe('new draft\n\nrejected text');
    expect(workspace.pendingInputs).toHaveLength(0);
    workspace.dispose();
  });

  it('refreshes history on reconnect without resending an accepted input', async () => {
    const client = new FakeClient(undefined, makeHistory('initial'), async (_id, _text, frame) => {
      frame({ msg: { case: 'messageAccepted', value: { sessionId: 's1' } } } as never);
      return {};
    });
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('accepted once'); await workspace.send();
    client.historyValue = makeHistory('refreshed after reconnect');
    client.onDisconnect?.(new ArcConnectionError('lost'));
    workspace.resume(); await tick();
    expect(client.sends).toEqual(['accepted once']);
    expect(workspace.messages).toContainEqual(expect.objectContaining({ content: 'refreshed after reconnect' }));
    workspace.dispose();
  });

  it('keeps a pending send durable when switching hosts closes observation', async () => {
    const send = defer<unknown>();
    const first = new FakeClient(undefined, makeHistory('history'), () => send.promise);
    const second = new FakeClient();
    const clients = [first, second];
    const workspace = new Workspace(null, () => clients.shift() as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('in flight'); const sending = workspace.send();
    expect(first.sends).toEqual(['in flight']);
    workspace.saveHost('Other', 'wss://other.test/ws'); await tick();
    send.reject(new ArcConnectionError('observation lost'));
    await sending;
    workspace.selectHost(host.id);
    expect(workspace.pendingInputs).toMatchObject([{ content: 'in flight', state: 'uncertain' }]);
    expect(second.sends).toEqual([]);
    workspace.dispose();
  });
});
