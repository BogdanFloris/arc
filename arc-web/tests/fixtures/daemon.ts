import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import type { WebSocketRoute } from '@playwright/test';
import { Role, SessionRole, Source, ToolOutcome } from '../../src/lib/arc/gen/events_pb';
import {
  ClientFrameSchema, ServerFrameSchema, SessionInfoSchema, SessionListSchema,
  JobListSchema, JobInfoSchema, HistoryEntrySchema, HistoryMessageSchema,
  ProjectListSchema,
  HistoryToolCallSchema, HistoryToolResultSchema, SessionHistorySchema,
  MessageAcceptedSchema, DeltaSchema, StreamEndSchema, ModelListSchema, ModelChoiceSchema,
  SessionStatusSchema,
  AllowanceWindowSchema,
} from '../../src/lib/arc/gen/wire_pb';
import { ContextMeasuredSchema } from '../../src/lib/arc/gen/events_pb';
import { histories, jobs, sessions } from './sessions';

export function fixtureDaemon(options: { jobsRunning?: boolean; streamGate?: () => Promise<void> } = {}) {
  const sessionMetadata = new Map(sessions.map((session) => [session.id, {
    id: session.id, title: session.title, project: '', role: jobs.some((job) => job.id === session.id) ? SessionRole.EXECUTOR : SessionRole.CHAT,
    source: jobs.some((job) => job.id === session.id) ? Source.MODEL : Source.USER,
    provider: 'openai', model: 'gpt-test',
  }]));
  let nextSession = 0;
  const historyEntries = new Map<string, ReturnType<typeof create<typeof SessionHistorySchema>>>();
  for (const [id, messages] of Object.entries(histories)) {
    const entries = messages.flatMap((message, index) => {
      if (message.tools?.length) {
        return [
          create(HistoryEntrySchema, { seq: BigInt(index * 2 + 1), entry: { case: 'toolCall', value: create(HistoryToolCallSchema, { callId: message.tools[0].id, name: message.tools[0].name, argumentsJson: '{}' }) } }),
          create(HistoryEntrySchema, { seq: BigInt(index * 2 + 2), entry: { case: 'toolResult', value: create(HistoryToolResultSchema, { callId: message.tools[0].id, outcome: ToolOutcome.OK, content: message.tools[0].output }) } }),
        ];
      }
      return [create(HistoryEntrySchema, { seq: BigInt(index + 1), entry: { case: 'message', value: create(HistoryMessageSchema, { role: message.role === 'you' ? Role.USER : Role.ASSISTANT, source: message.role === 'you' ? Source.USER : Source.MODEL, content: message.content }) } })];
    });
    historyEntries.set(id, create(SessionHistorySchema, { sessionId: id, entries }));
  }
  const thinking = new Map(sessions.map((session) => [session.id, 'low']));
  let statusOverride: Partial<{ context: ReturnType<typeof create<typeof ContextMeasuredSchema>> | undefined; contextObservedAt: bigint; allowance: ReturnType<typeof create<typeof AllowanceWindowSchema>>[]; allowanceObservedAt: bigint; allowanceStale: boolean; effectiveThinking: string; supportedThinking: string[] }> = {};
  let statusFetches = 0;
  const created = (route: WebSocketRoute) => route.onMessage((data) => {
    if (typeof data === 'string') throw new Error('Expected binary protobuf frame');
    const request = fromBinary(ClientFrameSchema, new Uint8Array(data as Buffer));
    const id = request.requestId;
    const reply = (msg: Parameters<typeof create<typeof ServerFrameSchema>>[1]['msg']) =>
      route.send(Buffer.from(toBinary(ServerFrameSchema, create(ServerFrameSchema, { requestId: id, msg }))));
    switch (request.msg.case) {
      case 'subscribe': break;
      case 'listSessions':
        reply({ case: 'sessionList', value: create(SessionListSchema, { sessions: [...sessionMetadata.values()].map((metadata) => {
          const session = sessions.find((entry) => entry.id === metadata.id);
          const time = BigInt(Math.floor((session?.lastAt ?? new Date()).getTime() / 1000));
          return create(SessionInfoSchema, { ...metadata, preview: session?.preview ?? '',
            startedAt: { seconds: time, nanos: 0 },
            ...(metadata.title ? { lastAt: { seconds: time, nanos: 0 } } : {}) });
        }) }) });
        break;
      case 'listJobs':
        reply({ case: 'jobList', value: create(JobListSchema, { jobs: jobs.map((job) => create(JobInfoSchema, { sessionId: job.id, title: job.title, state: options.jobsRunning === false ? 2 : 1 })) }) });
        break;
      case 'listProjects':
        reply({ case: 'projectList', value: create(ProjectListSchema, { projects: [{ name: 'configured-project' }] }) });
        break;
      case 'listModels':
        reply({ case: 'modelList', value: create(ModelListSchema, { choices: [
          create(ModelChoiceSchema, { role: SessionRole.CHAT, name: 'fast', provider: 'openai', model: 'gpt-fast', thinking: 'low', selected: true }),
          create(ModelChoiceSchema, { role: SessionRole.CHAT, name: 'deep', provider: 'openai', model: 'gpt-deep', thinking: 'high' }),
          create(ModelChoiceSchema, { role: SessionRole.EXECUTOR, name: 'fast', provider: 'openai', model: 'gpt-fast', thinking: 'low', selected: true }),
          create(ModelChoiceSchema, { role: SessionRole.EXECUTOR, name: 'deep', provider: 'openai', model: 'gpt-deep', thinking: 'high' }),
        ] }) });
        break;
      case 'createSession':
      case 'forkSession': {
        const value = request.msg.value;
        const id = `created-${++nextSession}`;
        const parent = request.msg.case === 'forkSession' ? value.sessionId : '';
        const choice = value.choice || 'fast';
        const model = choice === 'deep' ? 'gpt-deep' : 'gpt-fast';
        const parentHistory = parent ? historyEntries.get(parent) : undefined;
        const forkPoint = request.msg.case === 'forkSession' ? value.forkPoint : 0n;
        sessionMetadata.set(id, { id, title: '', project: request.msg.case === 'createSession' ? value.project : sessionMetadata.get(parent)?.project ?? '', role: sessionMetadata.get(parent)?.role ?? SessionRole.CHAT, source: Source.USER, provider: 'openai', model, parentSession: parent });
        thinking.set(id, choice === 'deep' ? 'high' : 'low');
        historyEntries.set(id, create(SessionHistorySchema, { sessionId: id, parentSession: parent, forkPoint, entries: parentHistory?.entries.filter((entry) => entry.seq <= forkPoint) ?? [] }));
        reply({ case: 'messageAccepted', value: create(MessageAcceptedSchema, { sessionId: id }) });
        break;
      }
      case 'fetchStatus':
      case 'setSessionThinking': {
        if (request.msg.case === 'fetchStatus') statusFetches++;
        const sessionId = request.msg.value.sessionId;
        if (request.msg.case === 'setSessionThinking') thinking.set(sessionId, request.msg.value.thinking);
        reply({ case: 'sessionStatus', value: create(SessionStatusSchema, { sessionId,
          context: create(ContextMeasuredSchema, { sessionId, inputTokens: 1200, contextWindow: 8000 }),
          contextObservedAt: BigInt(Math.floor(Date.now() / 1000)),
          codex: true, allowance: [create(AllowanceWindowSchema, { remainingPercent: 72, windowSeconds: 604800n })],
          allowanceObservedAt: BigInt(Math.floor(Date.now() / 1000)), allowanceSource: 'account',
          effectiveThinking: thinking.get(sessionId) ?? 'low',
          supportedThinking: ['low', 'medium', 'high'], ...statusOverride }) });
        break;
      }
      case 'fetchHistory': {
        const history = historyEntries.get(request.msg.value.sessionId);
        if (history) reply({ case: 'sessionHistory', value: history });
        break;
      }
      case 'sendMessage': {
        const sessionId = request.msg.value.sessionId || sessions[0].id;
        void (async () => {
          reply({ case: 'messageAccepted', value: create(MessageAcceptedSchema, { sessionId }) });
          await options.streamGate?.();
          const text = `I’ll help you work through: ${request.msg.value.content}`;
          for (let offset = 0; offset < text.length; offset += 12) {
            await new Promise((resolve) => setTimeout(resolve, 18));
            reply({ case: 'delta', value: create(DeltaSchema, { sessionId, text: text.slice(offset, offset + 12) }) });
          }
          const prior = historyEntries.get(sessionId);
          const seq = (prior?.entries.reduce((last, entry) => entry.seq > last ? entry.seq : last, 0n) ?? 0n) + 1n;
          historyEntries.set(sessionId, create(SessionHistorySchema, { sessionId, entries: [
            ...(prior?.entries ?? []),
            create(HistoryEntrySchema, { seq, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.USER, source: Source.USER, content: request.msg.value.content }) } }),
            create(HistoryEntrySchema, { seq: seq + 1n, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.ASSISTANT, source: Source.MODEL, content: text }) } }),
          ] }));
          reply({ case: 'streamEnd', value: create(StreamEndSchema, { sessionId }) });
        })();
        break;
      }
    }
  });
  return Object.assign(created, {
    setStatus: (status: typeof statusOverride) => { statusOverride = status; },
    get statusFetches() { return statusFetches; },
  });
}
