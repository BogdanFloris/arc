import { ArcClient, ArcConnectionError } from './arc/client';
import { conversationSessions, historyMessages, jobSummaries, toolState } from './arc/history';
import type { JobInfo, Notification, ServerFrame, SessionInfo } from './arc/gen/wire_pb';
import type { HostProfile, Job, Message, SessionSummary } from './arc/types';

const STORAGE_KEY = 'arc-web.local.v1';
const LEGACY_STORAGE_KEY = 'cairn.local.v1';
type LocalState = {
  hosts: HostProfile[];
  activeHostId: string;
  selected: Record<string, string>;
  drafts: Record<string, Record<string, string>>;
  inputs: Record<string, PendingInput[]>;
};
export type PendingInput = {
  id: string;
  sessionId: string | null;
  content: string;
  state: 'pending' | 'accepted' | 'uncertain' | 'rejected';
};
type Turn = { input: PendingInput; messages: Message[]; key: string };
type Client = Pick<ArcClient, 'connect' | 'close' | 'listSessions' | 'fetchHistory' | 'listJobs' | 'subscribe' | 'sendMessage'>;
type ClientFactory = (endpoint: string, onDisconnect: (error: ArcConnectionError) => void) => Client;
const emptyState = (): LocalState => ({ hosts: [], activeHostId: '', selected: {}, drafts: {}, inputs: {} });
const draftKey = (sessionId: string | null) => sessionId ?? '__new__';

export class Workspace {
  hosts = $state<HostProfile[]>([]);
  activeHostId = $state('');
  sessions = $state<SessionSummary[]>([]);
  activeSessionId = $state<string | null>(null);
  messages = $state<Message[]>([]);
  jobs = $state<Job[]>([]);
  draft = $state('');
  sending = $state(false);
  notice = $state('');
  connectionState = $state<'connecting' | 'connected' | 'unavailable'>('unavailable');
  loading = $state(false);
  pendingInputs = $state<PendingInput[]>([]);
  selectedProject = $state('');
  private storage: Storage | null;
  private drafts: LocalState['drafts'] = {};
  private selected: LocalState['selected'] = {};
  private generation = 0;
  private histories = new Map<string, Message[]>();
  private initialized = false;
  private legacyStoragePresent = false;
  private inputs: LocalState['inputs'] = {};
  private client: Client | null = null;
  private remoteSessions = $state<SessionInfo[]>([]);
  private liveJobs: JobInfo[] = [];
  private liveTurns = new Map<string, Turn>();
  private historyRequests = new Map<string, number>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private refreshTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay = 1000;
  private listRequest = 0;
  private framePending = false;
  private disposed = false;

  constructor(storage: Storage | null = null, private createClient: ClientFactory = (endpoint, onDisconnect) => new ArcClient(endpoint, { onDisconnect }),
    private initialHost?: HostProfile) {
    this.storage = storage;
  }
  get activeHost() { return this.hosts.find((host) => host.id === this.activeHostId) ?? null; }
  get canSend() { return this.connectionState === 'connected'; }
  get projectOptions() { return [...new Set(this.remoteSessions.map((session) => session.project).filter((project): project is string => !!project))].sort(); }
  get visibleSessions() { return conversationSessions(this.remoteSessions, { project: this.selectedProject || null }); }
  get activeTitle() {
    const session = this.remoteSessions.find((session) => session.id === this.activeSessionId);
    return session?.title || session?.preview.split('\n')[0] || this.jobs.find((job) => job.id === this.activeSessionId)?.title || 'New conversation';
  }
  get uncertainInputs() { return this.pendingInputs.filter((input) => input.sessionId === this.activeSessionId && (input.state === 'uncertain' || input.state === 'rejected')); }

  initialize() {
    if (this.initialized) return;
    this.initialized = true;
    let state = emptyState();
    if (this.initialHost) { state.hosts = [this.initialHost]; state.activeHostId = this.initialHost.id; }
    try {
      let stored = this.storage?.getItem(STORAGE_KEY);
      let loadedLegacy = false;
      if (stored === null || stored === undefined) {
        stored = this.storage?.getItem(LEGACY_STORAGE_KEY);
        loadedLegacy = stored !== null && stored !== undefined;
      }
      if (stored) {
        const parsed = JSON.parse(stored) as Partial<LocalState>;
        this.legacyStoragePresent = loadedLegacy;
        if (Array.isArray(parsed.hosts)) {
          const hosts = parsed.hosts.filter((host) => host.id !== 'demo' && host.kind === 'daemon');
          if (this.initialHost && !hosts.some((host) => host.id === this.initialHost!.id)) hosts.push(this.initialHost);
          state = { ...state, ...parsed, hosts };
        }
      }
    } catch {}
    this.hosts = state.hosts;
    this.activeHostId = state.hosts.some((host) => host.id === state.activeHostId) ? state.activeHostId : (this.initialHost?.id ?? state.hosts[0]?.id ?? '');
    this.selected = state.selected ?? {};
    this.drafts = state.drafts ?? {};
    this.inputs = state.inputs ?? {};
    delete this.selected.demo;
    delete this.drafts.demo;
    delete this.inputs.demo;
    for (const [host, inputs] of Object.entries(this.inputs)) {
      this.inputs[host] = inputs.filter((input) => input.state !== 'accepted').map((input) =>
        ({ ...input, state: input.state === 'pending' ? 'uncertain' : input.state }));
    }
    this.activate();
    this.persist();
  }

  selectHost(id: string) {
    if (!this.hosts.some((host) => host.id === id)) throw new Error('Choose a saved host.');
    if (id === this.activeHostId) return;
    this.rememberDraft();
    this.disconnect();
    this.activeHostId = id;
    this.selectedProject = '';
    this.generation++;
    for (const [sessionId, history] of this.histories) {
      this.histories.set(sessionId, history.map((message) => message.streaming ? { ...message, streaming: false } : message));
    }
    this.activate();
    this.persist();
  }

  saveHost(name: string, endpoint: string) {
    const cleanName = name.trim();
    if (!cleanName) throw new Error('Enter a name for this host.');
    let url: URL;
    try { url = new URL(endpoint); } catch { throw new Error('Enter a valid WebSocket endpoint (wss:// or local ws://).'); }
    const local = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
    if ((url.protocol !== 'wss:' && !(url.protocol === 'ws:' && local)) || url.username || url.password || url.hash) {
      throw new Error('Use wss://, or ws:// only for localhost, without credentials or a fragment.');
    }
    const host: HostProfile = { id: globalThis.crypto.randomUUID(), name: cleanName, endpoint: url.toString(), kind: 'daemon' };
    this.hosts = [...this.hosts, host];
    this.selectHost(host.id);
  }

  removeHost(id: string) {
    if (!this.hosts.some((host) => host.id === id)) return;
    const wasActive = this.activeHostId === id;
    if (wasActive) { this.rememberDraft(); this.disconnect(); this.generation++; }
    this.hosts = this.hosts.filter((host) => host.id !== id);
    if (wasActive) { this.activeHostId = this.hosts[0]?.id ?? ''; this.activate(); }
    this.persist();
  }

  selectSession(id: string) {
    if (!this.remoteSessions.some((session) => session.id === id) && !this.jobs.some((job) => job.id === id)) return;
    this.rememberDraft();
    this.activeSessionId = id;
    this.selected[this.activeHostId] = id;
    this.draft = this.drafts[this.activeHostId]?.[draftKey(id)] ?? '';
    this.activateContent();
    this.sending = this.liveTurns.has(id);
    void this.loadHistory(id);
    this.persist();
  }

  newConversation() {
    this.rememberDraft();
    this.activeSessionId = null;
    this.selected[this.activeHostId] = '__new__';
    this.draft = this.drafts[this.activeHostId]?.__new__ ?? '';
    this.activateContent();
    this.persist();
  }

  restoreInput(id: string) {
    const input = this.pendingInputs.find((item) => item.id === id);
    if (!input || !['uncertain', 'rejected'].includes(input.state)) return;
    this.setDraft(this.draft ? `${this.draft}\n\n${input.content}` : input.content);
    this.dismissInput(id);
  }

  dismissInput(id: string) {
    this.inputs[this.activeHostId] = (this.inputs[this.activeHostId] ?? []).filter((input) => input.id !== id);
    this.pendingInputs = [...this.inputs[this.activeHostId]];
    this.persist();
  }

  resume() {
    if (this.disposed || !this.activeHost) return;
    if (this.liveTurns.size) {
      this.disconnect();
      void this.connectRemote();
      return;
    }
    if (this.connectionState === 'connected') {
      void this.refreshRemote();
      if (this.activeSessionId && !this.liveTurns.has(this.activeSessionId)) void this.loadHistory(this.activeSessionId);
    } else if (this.connectionState !== 'connecting') {
      if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
      void this.connectRemote();
    }
  }

  setDraft(text: string) {
    this.draft = text;
    this.rememberDraft();
    this.persist();
  }

  async send() { await this.sendRemote(); }

  dispose() {
    this.disposed = true;
    this.disconnect();
    this.generation++;
    this.sending = false;
    this.rememberDraft();
    this.persist();
  }

  private activate() {
    const host = this.activeHost;
    this.selectedProject = '';
    this.connectionState = host ? 'connecting' : 'unavailable';
    this.notice = '';
    this.sessions = [];
    this.jobs = [];
    this.remoteSessions = [];
    this.liveJobs = [];
    this.histories.clear();
    this.historyRequests.clear();
    const remembered = this.selected[this.activeHostId];
    this.activeSessionId = remembered && remembered !== '__new__' ? remembered : null;
    this.draft = this.drafts[this.activeHostId]?.[draftKey(this.activeSessionId)] ?? '';
    this.pendingInputs = [...(this.inputs[this.activeHostId] ?? [])];
    this.activateContent();
    if (host) void this.connectRemote();
  }

  private activateContent() {
    const key = draftKey(this.activeSessionId);
    const turn = this.liveTurns.get(key);
    this.messages = [...(this.activeSessionId ? this.histories.get(this.activeSessionId) ?? [] : []), ...(turn?.messages ?? [])];
    this.sending = !!turn;
  }
  private rememberDraft() {
    if (!this.activeHostId) return;
    this.drafts[this.activeHostId] ??= {};
    this.drafts[this.activeHostId][draftKey(this.activeSessionId)] = this.draft;
  }
  private persist() {
    try {
      this.storage?.setItem(STORAGE_KEY, JSON.stringify({ hosts: this.hosts, activeHostId: this.activeHostId, selected: this.selected, drafts: this.drafts, inputs: this.inputs }));
      if (this.legacyStoragePresent) {
        this.storage?.removeItem(LEGACY_STORAGE_KEY);
        this.legacyStoragePresent = false;
      }
    } catch { /* Storage may be unavailable. */ }
  }

  private disconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    this.reconnectTimer = this.refreshTimer = null;
    this.client?.close();
    this.client = null;
    this.loading = false;
    this.reconnectDelay = 1000;
    this.retireTurns();
  }

  private retireTurns() {
    for (const input of this.inputs[this.activeHostId] ?? []) {
      if (input.state === 'pending') input.state = 'uncertain';
    }
    this.pendingInputs = [...(this.inputs[this.activeHostId] ?? [])];
    this.liveTurns.clear();
    this.sending = false;
    this.persist();
  }

  private async connectRemote() {
    const host = this.activeHost;
    if (!host || this.disposed) return;
    const generation = this.generation;
    this.client?.close();
    const client = this.createClient(host.endpoint, (error) => {
      if (this.client !== client || generation !== this.generation || this.disposed) return;
      this.retireTurns();
      this.activateContent();
      this.connectionState = 'unavailable';
      this.notice = `${error.message} Reconnecting… Displayed history may be out of date.`;
      this.scheduleReconnect();
    });
    this.client = client;
    this.connectionState = 'connecting';
    try {
      await client.connect();
      if (generation !== this.generation || this.client !== client || this.disposed) return;
      await client.subscribe((notification) => this.onNotification(notification, generation));
      if (generation !== this.generation || this.client !== client || this.disposed) return;
      this.reconnectDelay = 1000;
      const selected = this.activeSessionId;
      await this.refreshRemote(true);
      if (generation !== this.generation || this.client !== client) return;
      if (this.activeSessionId && this.activeSessionId === selected && !this.liveTurns.has(this.activeSessionId)) await this.loadHistory(this.activeSessionId);
    } catch (error) {
      if (generation !== this.generation || this.client !== client || this.disposed) return;
      this.connectionState = 'unavailable';
      this.notice = `Could not connect to ARC. ${error instanceof Error ? error.message : ''} Reconnecting…`;
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.reconnectTimer || this.disposed) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connectRemote();
    }, this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 10000);
  }

  private async refreshRemote(initial = false) {
    const client = this.client, generation = this.generation;
    if (!client || (this.connectionState !== 'connected' && !initial)) return;
    const request = ++this.listRequest;
    try {
      const [sessions, jobs] = await Promise.all([client.listSessions(), client.listJobs()]);
      if (generation !== this.generation || this.client !== client || request !== this.listRequest) return;
      if (initial) {
        this.connectionState = 'connected';
        this.notice = '';
      }
      this.remoteSessions = sessions;
      this.liveJobs = jobs;
      this.sessions = conversationSessions(sessions, { showAbandoned: true });
      this.jobs = jobSummaries(jobs);
      const remembered = this.selected[this.activeHostId];
      const first = conversationSessions(sessions).at(0);
      if (!this.activeSessionId && remembered !== '__new__' && !this.liveTurns.has('__new__') && first) {
        this.selectSession(first.id);
      } else if (this.activeSessionId && !sessions.some((session) => session.id === this.activeSessionId)
        && !this.liveTurns.has(this.activeSessionId)) {
        const fallback = conversationSessions(sessions, {}).at(0);
        if (fallback) this.selectSession(fallback.id);
        else this.newConversation();
      }
    } catch (error) {
      if (initial) throw error;
      if (generation === this.generation && this.client === client) this.notice = `Could not refresh sessions. ${error instanceof Error ? error.message : ''}`;
    }
  }

  private async loadHistory(id: string) {
    const client = this.client, generation = this.generation;
    if (!client || this.connectionState !== 'connected' || this.liveTurns.has(id)) return;
    const request = (this.historyRequests.get(id) ?? 0) + 1;
    this.historyRequests.set(id, request);
    if (this.activeSessionId === id) this.loading = true;
    try {
      const history = await client.fetchHistory(id);
      if (generation !== this.generation || this.client !== client || this.historyRequests.get(id) !== request || this.liveTurns.has(id)) return;
      this.histories.set(id, historyMessages(history));
      if (this.activeSessionId === id) this.activateContent();
    } catch (error) {
      if (generation === this.generation && this.client === client && this.activeSessionId === id) {
        this.notice = `Could not load conversation. ${error instanceof Error ? error.message : ''}`;
      }
    } finally {
      if (generation === this.generation && this.activeSessionId === id && this.historyRequests.get(id) === request) this.loading = false;
    }
  }

  private onNotification(notification: Notification, generation: number) {
    if (generation !== this.generation) return;
    if (notification.event.case === 'jobChanged') {
      const job = notification.event.value;
      const index = this.liveJobs.findIndex((item) => item.sessionId === job.sessionId);
      if (index < 0) this.liveJobs = [job, ...this.liveJobs];
      else this.liveJobs = this.liveJobs.map((item, row) => row === index ? job : item);
      this.jobs = jobSummaries(this.liveJobs);
    }
    if (notification.event.case !== 'sessionAppended' && notification.event.case !== 'jobChanged') return;
    if (this.refreshTimer) return;
    this.refreshTimer = setTimeout(() => {
      this.refreshTimer = null;
      if (generation !== this.generation) return;
      void this.refreshRemote();
      if (this.activeSessionId && !this.liveTurns.has(this.activeSessionId)) void this.loadHistory(this.activeSessionId);
    }, 120);
  }

  private paintStream() {
    if (this.framePending) return;
    this.framePending = true;
    const paint = () => {
      this.framePending = false;
      if (!this.disposed && this.activeHost) this.activateContent();
    };
    if (typeof requestAnimationFrame === 'function') requestAnimationFrame(paint);
    else queueMicrotask(paint);
  }

  private streamFrame(turn: Turn, frame: ServerFrame) {
    const msg = frame.msg;
    if (msg.case === 'messageAccepted') {
      const id = msg.value.sessionId, oldId = turn.input.sessionId;
      turn.input.state = 'accepted';
      turn.input.sessionId = id;
      turn.messages[0].delivery = undefined;
      this.liveTurns.delete(turn.key);
      turn.key = id;
      this.liveTurns.set(id, turn);
      if (this.activeSessionId === oldId) {
        this.rememberDraft();
        this.activeSessionId = id;
        this.selected[this.activeHostId] = id;
        if (oldId === null) this.drafts[this.activeHostId][id] = this.draft;
      }
      this.pendingInputs = [...(this.inputs[this.activeHostId] ?? [])];
      this.persist();
    } else if (msg.case === 'delta') {
      let tail = turn.messages.at(-1);
      if (!tail || tail.tools || !tail.streaming) {
        tail = { id: `${turn.input.id}-reply-${turn.messages.length}`, role: 'arc', content: '', streaming: true };
        turn.messages.push(tail);
      }
      tail.content += msg.value.text;
    } else if (msg.case === 'toolCallStarted') {
      const value = msg.value;
      const tail = turn.messages.at(-1);
      if (tail) tail.streaming = false;
      turn.messages.push({ id: `${turn.input.id}-tool-${value.callId}`, role: 'arc', content: '', tools: [{
        id: value.callId, name: value.name, arguments: value.argumentsJson, output: '', state: 'running',
      }] });
    } else if (msg.case === 'toolCallEnded') {
      const value = msg.value;
      const tool = turn.messages.flatMap((message) => message.tools ?? []).find((tool) => tool.id === value.callId);
      if (tool) {
        tool.output = value.content;
        tool.state = toolState(value.outcome);
      }
    }
    this.paintStream();
  }

  private async sendRemote() {
    const client = this.client, generation = this.generation, hostId = this.activeHostId;
    const key = draftKey(this.activeSessionId);
    if (!client || this.connectionState !== 'connected' || !this.draft.trim() || this.liveTurns.has(key) || this.loading) return;
    const input: PendingInput = { id: globalThis.crypto.randomUUID(), sessionId: this.activeSessionId, content: this.draft, state: 'pending' };
    const turn: Turn = { input, key, messages: [{ id: input.id, role: 'you', content: input.content, delivery: 'pending' }] };
    this.inputs[hostId] ??= [];
    this.inputs[hostId].push(input);
    this.pendingInputs = [...this.inputs[hostId]];
    this.liveTurns.set(key, turn);
    this.draft = '';
    this.rememberDraft();
    this.persist();
    this.activateContent();
    try {
      const end = await client.sendMessage(input.sessionId ?? undefined, input.content, (frame) => {
        if (generation === this.generation && this.client === client) this.streamFrame(turn, frame);
      });
      if (generation !== this.generation || this.client !== client) return;
      this.dismissInput(input.id);
      if (end.queued) this.notice = 'Message accepted into the running turn. Its reply will appear as history updates.';
      else if (end.partial || end.stepCapped) this.notice = 'ARC returned a partial reply.';
      else this.notice = '';
    } catch (error) {
      if (generation !== this.generation || this.client !== client) return;
      if (input.state === 'accepted') {
        this.dismissInput(input.id);
        this.notice = `Message accepted by ARC; reply observation stopped. Refresh history to check the outcome. ${error instanceof Error ? error.message : ''}`;
      } else {
        const uncertain = error instanceof ArcConnectionError && error.accepted !== false;
        input.state = uncertain ? 'uncertain' : 'rejected';
        this.pendingInputs = [...this.inputs[hostId]];
        this.notice = uncertain ? 'Delivery is unknown. Check conversation history before sending this input again.'
          : `Message not accepted. ${error instanceof Error ? error.message : ''}`;
        this.persist();
      }
    } finally {
      if (generation === this.generation && this.client === client) {
        this.liveTurns.delete(turn.key);
        this.activateContent();
        if (input.sessionId) await this.loadHistory(input.sessionId);
        await this.refreshRemote();
      }
    }
  }
}
