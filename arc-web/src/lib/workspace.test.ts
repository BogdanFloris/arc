import { describe, expect, it } from 'vitest';
import { Workspace } from './workspace.svelte';

function storage(initial?: string): Storage {
  const values = new Map<string, string>(initial ? [['arc-web.local.v1', initial]] : []);
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => { values.set(key, value); },
    removeItem: (key) => { values.delete(key); },
    clear: () => values.clear(),
    key: (index) => [...values.keys()][index] ?? null,
    get length() { return values.size; }
  };
}
const mockClient = () => ({ connect: async () => {}, close: () => {}, subscribe: async () => {},
  listSessions: async () => [], listJobs: async () => [], fetchHistory: async () => ({ entries: [] }),
  sendMessage: async () => ({}) });
const host = (id: string) => ({ id, name: id, endpoint: `wss://${id}.example/ws`, kind: 'daemon' as const });

describe('Workspace local state', () => {
  it('migrates demo profiles while preserving real profiles, selection, drafts and pending inputs', async () => {
    const real = host('real');
    const store = storage(JSON.stringify({
      hosts: [{ id: 'demo', name: 'Demo', endpoint: '', kind: 'demo' }, real],
      activeHostId: 'real', selected: { demo: 'demo-session', real: 'conversation' },
      drafts: { demo: { 'demo-session': 'test data' }, real: { conversation: 'saved draft' } },
      inputs: { demo: [{ id: 'test', sessionId: null, content: 'fixture', state: 'pending' }],
        real: [{ id: 'real-input', sessionId: 'conversation', content: 'possibly sent', state: 'pending' }] }
    }));
    const workspace = new Workspace(store, () => ({ ...mockClient(),
      listSessions: async () => [{ id: 'conversation', title: 'Saved conversation', source: 2 }],
    }) as never);
    workspace.initialize();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(workspace.hosts).toEqual([real]);
    expect(workspace.activeHostId).toBe('real');
    expect(workspace.activeSessionId).toBe('conversation');
    expect(workspace.draft).toBe('saved draft');
    const saved = JSON.parse(store.getItem('arc-web.local.v1')!);
    expect(saved.hosts).toEqual([real]);
    expect(saved.drafts.real.conversation).toBe('saved draft');
    expect(saved.selected.demo).toBeUndefined();
    expect(saved.drafts.demo).toBeUndefined();
    expect(saved.inputs.demo).toBeUndefined();
    expect(saved.inputs.real).toMatchObject([{ content: 'possibly sent', state: 'uncertain' }]);
    workspace.dispose();
  });

  it('migrates legacy data and removes the old key only after saving it', () => {
    const values = new Map<string, string>([['cairn.local.v1', JSON.stringify({
      hosts: [host('legacy')], activeHostId: 'legacy', selected: { legacy: 'session' },
      drafts: { legacy: { session: 'draft' } },
      inputs: { legacy: [{ id: 'pending', sessionId: 'session', content: 'do not resend', state: 'pending' }] },
    })]]);
    const store = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => { values.set(key, value); },
      removeItem: (key: string) => { values.delete(key); },
      clear: () => values.clear(), key: () => null, get length() { return values.size; },
    } as Storage;
    new Workspace(store, () => mockClient() as never).initialize();
    expect(values.has('cairn.local.v1')).toBe(false);
    expect(JSON.parse(values.get('arc-web.local.v1')!)).toMatchObject({
      hosts: [host('legacy')], selected: { legacy: 'session' },
      drafts: { legacy: { session: 'draft' } },
      inputs: { legacy: [{ id: 'pending', content: 'do not resend', state: 'uncertain' }] },
    });
  });

  it('prefers the new key when both versions exist', () => {
    const legacy = JSON.stringify({ hosts: [host('legacy')], activeHostId: 'legacy', selected: {}, drafts: {}, inputs: {} });
    const current = JSON.stringify({ hosts: [host('current')], activeHostId: 'current', selected: {}, drafts: {}, inputs: {} });
    const values = new Map([['cairn.local.v1', legacy], ['arc-web.local.v1', current]]);
    const store = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => { values.set(key, value); },
      removeItem: (key: string) => { values.delete(key); },
      clear: () => values.clear(), key: () => null, get length() { return values.size; },
    } as Storage;
    const workspace = new Workspace(store, () => mockClient() as never);
    workspace.initialize();
    expect(workspace.hosts).toEqual([host('current')]);
    expect(values.has('cairn.local.v1')).toBe(true);
    expect(values.get('arc-web.local.v1')).toBe(current);
  });

  it('retains legacy data when migration persistence fails', () => {
    const legacy = JSON.stringify({ hosts: [host('legacy')], activeHostId: 'legacy', selected: {}, drafts: {}, inputs: {} });
    const values = new Map([['cairn.local.v1', legacy]]);
    const store = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: () => { throw new DOMException('quota', 'QuotaExceededError'); },
      removeItem: (key: string) => { values.delete(key); },
      clear: () => values.clear(), key: () => null, get length() { return values.size; },
    } as Storage;
    new Workspace(store, () => mockClient() as never).initialize();
    expect(values.get('cairn.local.v1')).toBe(legacy);
    expect(values.has('arc-web.local.v1')).toBe(false);
  });

  it('migrates a demo-only installation to no active host without auto-connecting', () => {
    const store = storage(JSON.stringify({
      hosts: [{ id: 'demo', kind: 'demo' }], activeHostId: 'demo', selected: {}, drafts: {}, inputs: {}
    }));
    let connections = 0;
    const workspace = new Workspace(store, () => { connections++; return mockClient() as never; });
    workspace.initialize();
    expect(workspace.hosts).toEqual([]);
    expect(workspace.activeHostId).toBe('');
    expect(workspace.activeHost).toBeNull();
    expect(workspace.connectionState).toBe('unavailable');
    expect(workspace.messages).toEqual([]);
    expect(workspace.canSend).toBe(false);
    expect(connections).toBe(0);
  });

  it('uses a supplied initial host when migrating demo-only state', async () => {
    const workspace = new Workspace(storage(JSON.stringify({
      hosts: [{ id: 'demo', kind: 'demo' }], activeHostId: 'demo', selected: {}, drafts: {}, inputs: {}
    })), () => mockClient() as never, host('initial'));
    workspace.initialize();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(workspace.activeHostId).toBe('initial');
    expect(workspace.hosts.map((item) => item.id)).toEqual(['initial']);
    workspace.dispose();
  });

  it('permits removing the last host without creating a fallback', () => {
    const workspace = new Workspace(null, () => mockClient() as never);
    workspace.initialize();
    workspace.saveHost('Remote', 'wss://arc.example/ws');
    workspace.removeHost(workspace.activeHostId);
    expect(workspace.hosts).toEqual([]);
    expect(workspace.activeHostId).toBe('');
    expect(workspace.connectionState).toBe('unavailable');
    expect(workspace.messages).toEqual([]);
    workspace.dispose();
  });

  it('validates endpoint security requirements', () => {
    const workspace = new Workspace(null, () => mockClient() as never);
    workspace.initialize();
    for (const endpoint of ['http://localhost', 'ws://example.com', 'wss://user:pass@example.com', 'wss://example.com/#x']) {
      expect(() => workspace.saveHost('Host', endpoint)).toThrow();
    }
    expect(() => workspace.saveHost(' ', 'wss://example.com')).toThrow();
    expect(() => workspace.saveHost('Host', 'ws://[::1]:9000')).not.toThrow();
    workspace.dispose();
  });
});
