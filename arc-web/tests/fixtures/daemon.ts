import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import type { WebSocketRoute } from '@playwright/test';
import { Role, SessionRole, Source, ToolOutcome } from '../../src/lib/arc/gen/events_pb';
import {
  ClientFrameSchema, ServerFrameSchema, SessionInfoSchema, SessionListSchema,
  JobListSchema, JobInfoSchema, HistoryEntrySchema, HistoryMessageSchema,
  HistoryToolCallSchema, HistoryToolResultSchema, SessionHistorySchema,
  MessageAcceptedSchema, DeltaSchema, StreamEndSchema,
} from '../../src/lib/arc/gen/wire_pb';
import { histories, jobs, sessions } from './sessions';

export function fixtureDaemon() {
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
  return (route: WebSocketRoute) => route.onMessage((data) => {
    if (typeof data === 'string') throw new Error('Expected binary protobuf frame');
    const request = fromBinary(ClientFrameSchema, new Uint8Array(data as Buffer));
    const id = request.requestId;
    const reply = (msg: Parameters<typeof create<typeof ServerFrameSchema>>[1]['msg']) =>
      route.send(Buffer.from(toBinary(ServerFrameSchema, create(ServerFrameSchema, { requestId: id, msg }))));
    switch (request.msg.case) {
      case 'subscribe': break;
      case 'listSessions':
        reply({ case: 'sessionList', value: create(SessionListSchema, { sessions: sessions.map((session) =>
          create(SessionInfoSchema, { id: session.id, title: session.title, preview: session.preview, source: Source.USER, role: SessionRole.CHAT,
            startedAt: { seconds: BigInt(Math.floor(session.lastAt!.getTime() / 1000)), nanos: 0 }, lastAt: { seconds: BigInt(Math.floor(session.lastAt!.getTime() / 1000)), nanos: 0 } })) }) });
        break;
      case 'listJobs':
        reply({ case: 'jobList', value: create(JobListSchema, { jobs: jobs.map((job) => create(JobInfoSchema, { sessionId: job.id, title: job.title, state: 1 })) }) });
        break;
      case 'fetchHistory': {
        const history = historyEntries.get(request.msg.value.sessionId);
        if (history) reply({ case: 'sessionHistory', value: history });
        break;
      }
      case 'sendMessage': {
        const sessionId = request.msg.value.sessionId || sessions[0].id;
        void (async () => {
          reply({ case: 'messageAccepted', value: create(MessageAcceptedSchema, { sessionId }) });
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
}
