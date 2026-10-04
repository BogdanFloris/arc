import { afterEach, describe, expect, it, vi } from 'vitest';
import { ArcConnectionError } from './arc/client';
import { Workspace } from './workspace.svelte';
import type { HostProfile } from './arc/types';
import { create } from '@bufbuild/protobuf';
import { ModelChoiceSchema, SessionStatusSchema, MessageAcceptedSchema, JobInfoSchema } from './arc/gen/wire_pb';

const host: HostProfile = { id: 'remote', name: 'Remote', endpoint: 'wss://arc.test/ws', kind: 'daemon' };
const makeHistory = (content: string) => ({ entries: [{ seq: 1n, entry: { case: 'message', value: { source: 2, role: 2, content } } }] });
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
  sendIds: (string | undefined)[] = [];
  historyCalls: string[] = [];
  projects: { name: string }[] = [];
  models = [create(ModelChoiceSchema, { role: 1, name: 'default', model: 'model-a', provider: 'codex', thinking: 'medium', selected: true }),
    create(ModelChoiceSchema, { role: 1, name: 'other', model: 'model-b', provider: 'codex', thinking: 'high' })];
  creates: { project: string; choice: string }[] = [];
  forks: { id: string; point: bigint; choice: string }[] = [];
  thinking: string[] = [];
  jobItems: ReturnType<typeof create<typeof JobInfoSchema>>[] = [];
  onDisconnect?: (error: ArcConnectionError) => void;
  constructor(
    public sessions: unknown[] = [{ id: 's1', title: 'Session', source: 2 }, { id: 's2', title: 'Second', source: 2 }],
    public historyValue: unknown = makeHistory('from history'),
    private send: (id: string | undefined, text: string, frame: (value: never) => void) => Promise<unknown> = async (id, _text, frame) => {
      frame({ msg: { case: 'messageAccepted', value: { sessionId: id || 's1' } } } as never);
      frame({ msg: { case: 'delta', value: { text: 'reply' } } } as never);
      return {};
    }
  ) {}
  async connect() {}
  close() { this.closed = true; }
  async subscribe() {}
  async listSessions() { return this.sessions; }
  async listProjects() { return this.projects; }
  async listModels() { return this.models; }
  async listJobs() { return this.jobItems; }
  async createSession(project: string, choice: string) {
    this.creates.push({ project, choice });
    const sessionId = `created-${this.creates.length}`;
    const model = this.models.find((item) => item.name === choice)!;
    this.sessions.push({ id: sessionId, source: 2, role: 1, project, provider: model.provider, model: model.model });
    return create(MessageAcceptedSchema, { sessionId });
  }
  async forkSession(id: string, point: bigint, choice: string) {
    this.forks.push({ id, point, choice });
    return create(MessageAcceptedSchema, { sessionId: 'forked' });
  }
  async fetchStatus(id: string) {
    return create(SessionStatusSchema, { sessionId: id, supportedThinking: ['medium', 'high'], effectiveThinking: this.thinking.at(-1) ?? 'medium' });
  }
  async setSessionThinking(id: string, thinking: string) { this.thinking.push(thinking); return this.fetchStatus(id); }
  async fetchHistory(id: string) { this.historyCalls.push(id); return typeof this.historyValue === 'function' ? (this.historyValue as (id: string) => Promise<unknown>)(id) : this.historyValue; }
  async sendMessage(id: string | undefined, text: string, frame: (value: never) => void) { this.sends.push(text); this.sendIds.push(id); return this.send(id, text, frame); }
}

afterEach(() => vi.useRealTimers());

describe('remote Workspace', () => {
  it.each(['text', 'tool'])('waits through acknowledgement until the first %s output', async (output) => {
    const completion = defer<unknown>();
    let frame!: (value: never) => void;
    const client = new FakeClient(undefined, makeHistory('history'), (_id, _text, callback) => {
      frame = callback;
      return completion.promise;
    });
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('start a reply');
    const sending = workspace.send();
    expect(workspace.waitingForReply).toBe(true);
    frame({ msg: { case: 'messageAccepted', value: { sessionId: 's1' } } } as never);
    await tick();
    expect(workspace.waitingForReply).toBe(true);
    frame({ msg: { case: 'delta', value: { text: '' } } } as never);
    await tick();
    expect(workspace.waitingForReply).toBe(true);
    expect(workspace.messages.at(-1)?.role).toBe('you');
    workspace.selectSession('s2'); await tick();
    expect(workspace.waitingForReply).toBe(false);
    workspace.selectSession('s1'); await tick();
    expect(workspace.waitingForReply).toBe(true);
    frame({ msg: output === 'text'
      ? { case: 'delta', value: { text: 'First words' } }
      : { case: 'toolCallStarted', value: { callId: 'first-tool', name: 'read', argumentsJson: '{}' } } } as never);
    await tick();
    expect(workspace.waitingForReply).toBe(false);
    expect(workspace.sending).toBe(true);
    completion.resolve({}); await sending;
    workspace.dispose();
  });

  it.each(['finished', 'failed'])('stops waiting when observation %s before any output', async (outcome) => {
    const completion = defer<unknown>();
    const client = new FakeClient(undefined, makeHistory('history'), (_id, _text, frame) => {
      frame({ msg: { case: 'messageAccepted', value: { sessionId: 's1' } } } as never);
      return completion.promise;
    });
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('start a reply');
    const sending = workspace.send(); await tick();
    expect(workspace.waitingForReply).toBe(true);
    const history = defer<unknown>();
    client.historyValue = () => history.promise;
    if (outcome === 'finished') completion.resolve({});
    else completion.reject(new ArcConnectionError('observation lost', true));
    await tick();
    expect(workspace.waitingForReply).toBe(false);
    history.resolve(makeHistory('durable history')); await sending;
    workspace.dispose();
  });

  it('keeps completed replies visible until history arrives and blocks forks from an unreconciled tail', async () => {
    const client = new FakeClient([{ id: 's1', title: 'Session', source: 2, role: 1, model: 'model-a' }]);
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    const history = defer<unknown>();
    client.historyValue = () => history.promise;
    workspace.setDraft('finish this reply');
    const send = workspace.send(); await tick();
    expect(workspace.messages).toContainEqual(expect.objectContaining({ content: 'reply' }));
    history.reject(new Error('History unavailable')); await send;
    expect(workspace.messages).toContainEqual(expect.objectContaining({ content: 'reply' }));
    expect(workspace.sending).toBe(false);
    workspace.chooseModel('other'); await workspace.confirmModelFork();
    expect(client.forks).toEqual([]);
    expect(workspace.controlsError).toContain('No durable message to fork');
    workspace.dispose();
  });

  it('keeps a daemon-reported job open across refresh and permits steering without changing its controls', async () => {
    const client = new FakeClient();
    client.jobItems = [create(JobInfoSchema, { sessionId: 'job', title: 'Live job', state: 1 })];
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.selectSession('job'); await tick();
    expect(workspace.canConfigure).toBe(false);
    workspace.resume(); await tick();
    expect(workspace.activeSessionId).toBe('job');
    workspace.setDraft('follow up'); await workspace.send();
    expect(client.sendIds).toEqual(['job']);
    expect(workspace.activeSessionId).toBe('job');
    workspace.dispose();
  });

  it('sends once to the explicitly created session and restores host-scoped choices on reload', async () => {
    const client = new FakeClient();
    client.projects = [{ name: 'arc' }];
    const storage = store();
    const workspace = new Workspace(storage, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.newConversation();
    workspace.selectNewProject('arc'); workspace.chooseModel('other');
    workspace.setDraft('send this once'); await workspace.send();
    expect(client.creates).toEqual([{ project: 'arc', choice: 'other' }]);
    expect(client.sends).toEqual(['send this once']);
    expect(client.sendIds).toEqual(['created-1']);
    expect(workspace.draft).toBe('');
    workspace.dispose();
    const restored = new Workspace(storage, () => client as never, host);
    restored.initialize(); await tick();
    restored.newConversation();
    expect(restored.newProject).toBe('arc');
    expect(restored.newModelChoice).toBe('other');
    expect(client.sends).toHaveLength(1);
    restored.dispose();
  });

  it('creates with explicit context, prepares supported thinking, and preserves pre-send drafts', async () => {
    const client = new FakeClient();
    client.projects = [{ name: 'arc' }, { name: 'scratch' }];
    const workspace = new Workspace(store(), () => client as never, host);
    workspace.initialize(); await tick();
    workspace.newConversation();
    workspace.setDraft('unsent work');
    workspace.selectNewProject('arc');
    workspace.chooseModel('other');
    expect(client.creates).toEqual([]);
    await workspace.prepareThinking();
    expect(client.creates).toEqual([{ project: 'arc', choice: 'other' }]);
    expect(workspace.draft).toBe('unsent work');
    expect(workspace.isEmptyConversation).toBe(true);
    await workspace.setThinking('invented');
    expect(client.thinking).toEqual([]);
    await workspace.setThinking('high');
    expect(workspace.effectiveThinking).toBe('high');
    workspace.sending = true;
    await workspace.setThinking('medium');
    expect(client.thinking).toEqual(['high']);
    workspace.sending = false;
    workspace.selectNewProject('scratch');
    expect(workspace.activeSessionId).toBeNull();
    expect(workspace.draft).toBe('unsent work');
    await workspace.prepareThinking();
    expect(client.creates.at(-1)).toEqual({ project: 'scratch', choice: 'other' });
    expect(client.sends).toEqual([]);
    workspace.dispose();
  });

  it('does not fork until confirmation and preserves the original model and draft', async () => {
    const client = new FakeClient([{ id: 's1', title: 'Session', source: 2, role: 1, project: 'arc', provider: 'codex', model: 'model-a' }]);
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.setDraft('next thought');
    workspace.chooseModel('other');
    expect(client.forks).toEqual([]);
    workspace.cancelModelFork();
    expect(workspace.recordedModel).toBe('model-a');
    workspace.chooseModel('other');
    await workspace.confirmModelFork();
    expect(client.forks).toEqual([{ id: 's1', point: 1n, choice: 'other' }]);
    expect(workspace.recordedModel).toBe('model-b');
    expect(workspace.draft).toBe('next thought');
    workspace.selectSession('s1'); await tick();
    expect(workspace.recordedModel).toBe('model-a');
    expect(workspace.draft).toBe('next thought');
    workspace.dispose();
  });

  it('does not send after navigation during creation and refuses duplicate creation', async () => {
    const client = new FakeClient();
    const creation = defer<ReturnType<typeof create<typeof MessageAcceptedSchema>>>();
    const createSession = vi.spyOn(client, 'createSession').mockImplementation(() => creation.promise);
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.newConversation(); workspace.setDraft('stay in new draft');
    const send = workspace.send();
    await workspace.send();
    expect(createSession).toHaveBeenCalledOnce();
    workspace.selectSession('s2'); await tick();
    creation.resolve(create(MessageAcceptedSchema, { sessionId: 'late-create' }));
    await send;
    expect(workspace.activeSessionId).toBe('s2');
    expect(client.sends).toEqual([]);
    workspace.newConversation();
    expect(workspace.draft).toBe('stay in new draft');
    workspace.dispose();
  });

  it('keeps edited drafts unsent while creation waits and keeps creation failures recoverable', async () => {
    const client = new FakeClient();
    const creation = defer<ReturnType<typeof create<typeof MessageAcceptedSchema>>>();
    vi.spyOn(client, 'createSession').mockImplementationOnce(() => creation.promise).mockRejectedValueOnce(new Error('Preset unavailable'));
    const workspace = new Workspace(null, () => client as never, host);
    workspace.initialize(); await tick();
    workspace.newConversation(); workspace.setDraft('original');
    const send = workspace.send();
    workspace.setDraft('edited before creation finished');
    creation.resolve(create(MessageAcceptedSchema, { sessionId: 'created' }));
    await send;
    expect(client.sends).toEqual([]);
    expect(workspace.draft).toBe('edited before creation finished');
    workspace.newConversation(); workspace.setDraft('retry explicitly');
    await workspace.send();
    expect(workspace.controlsError).toBe('Preset unavailable');
    expect(workspace.draft).toBe('retry explicitly');
    expect(workspace.pendingInputs).toEqual([]);
    workspace.dispose();
  });

  it('ignores late status from another session and late creation from another host', async () => {
    const first = new FakeClient();
    const status = defer<ReturnType<typeof create<typeof SessionStatusSchema>>>();
    vi.spyOn(first, 'fetchStatus').mockImplementation((id) => id === 's1' ? status.promise
      : Promise.resolve(create(SessionStatusSchema, { sessionId: id, effectiveThinking: 'high' })));
    const creation = defer<ReturnType<typeof create<typeof MessageAcceptedSchema>>>();
    vi.spyOn(first, 'createSession').mockImplementation(() => creation.promise);
    const second = new FakeClient();
    const clients = [first, second];
    const workspace = new Workspace(store(), () => clients.shift() as never, host);
    workspace.initialize(); await tick();
    workspace.selectSession('s2'); await tick();
    status.resolve(create(SessionStatusSchema, { sessionId: 's1', effectiveThinking: 'medium' })); await tick();
    expect(workspace.sessionStatus?.sessionId).toBe('s2');
    workspace.newConversation(); workspace.setDraft('old host draft');
    const send = workspace.send();
    workspace.saveHost('Other host', 'wss://other.test/ws'); await tick();
    creation.resolve(create(MessageAcceptedSchema, { sessionId: 'old-host-create' })); await send;
    expect(workspace.activeSessionId).toBe('s1');
    expect(workspace.draft).not.toBe('old host draft');
    expect(first.sends).toEqual([]);
    expect(second.sends).toEqual([]);
    workspace.dispose();
  });

  it('uses configured projects, retains removed-project history under All, and scopes project data to each host', async () => {
    const first = new FakeClient([{ id: 'old', title: 'Old project session', project: 'removed', source: 2 }], makeHistory('old'));
    first.projects = [{ name: 'configured' }, { name: 'empty' }];
    const second = new FakeClient([{ id: 'new', title: 'New host session', project: 'other', source: 2 }], makeHistory('new'));
    second.projects = [{ name: 'other' }];
    const clients = [first, second];
    const workspace = new Workspace(null, () => clients.shift() as never, host);
    workspace.initialize(); await tick();
    expect(workspace.projectOptions).toEqual(['configured', 'empty']);
    expect(workspace.visibleSessions).toMatchObject([{ id: 'old' }]);
    workspace.selectedProject = 'configured';
    workspace.resume(); await tick();
    first.projects = [{ name: 'empty' }];
    workspace.resume(); await tick();
    expect(workspace.selectedProject).toBe('');
    expect(workspace.visibleSessions).toMatchObject([{ id: 'old' }]);
    workspace.saveHost('Other', 'wss://other.test/ws'); await tick();
    expect(workspace.projectOptions).toEqual(['other']);
    expect(workspace.visibleSessions).toMatchObject([{ id: 'new' }]);
    workspace.dispose();
  });
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
    expect(workspace.waitingForReply).toBe(false);
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
    expect(workspace.waitingForReply).toBe(true);
    workspace.saveHost('Other', 'wss://other.test/ws'); await tick();
    expect(workspace.waitingForReply).toBe(false);
    send.reject(new ArcConnectionError('observation lost'));
    await sending;
    workspace.selectHost(host.id);
    expect(workspace.pendingInputs).toMatchObject([{ content: 'in flight', state: 'uncertain' }]);
    expect(second.sends).toEqual([]);
    workspace.dispose();
  });
});
