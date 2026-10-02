import { BranchMarked_Disposition, Role, Source, ToolOutcome } from './gen/events_pb';
import { JobInfo_State, type JobInfo, type SessionHistory, type SessionInfo } from './gen/wire_pb';
import type { Job, Message, SessionSummary, Tool } from './types';

export function isConversation(session: SessionInfo): boolean {
  return session.source === Source.USER;
}

export function sessionSummary(session: SessionInfo): SessionSummary {
  return { id: session.id, title: session.title || session.preview.split('\n')[0] || 'Untitled conversation', preview: session.preview, project: session.project };
}

export type SessionFilters = { project?: string | null; showAbandoned?: boolean };

export function conversationSessions(sessions: SessionInfo[], filters: SessionFilters = {}): SessionSummary[] {
  return sessions.filter((session) => isConversation(session)
    && (!!session.title || !!session.preview || session.lastAt !== undefined)
    && (filters.showAbandoned || session.disposition !== BranchMarked_Disposition.ABANDONED)
    && (!filters.project || session.project === filters.project))
    .sort((a, b) => {
      const left = a.lastAt ?? a.startedAt, right = b.lastAt ?? b.startedAt;
      if (left && right) {
        if (left.seconds !== right.seconds) return left.seconds > right.seconds ? -1 : 1;
        if (left.nanos !== right.nanos) return right.nanos - left.nanos;
      } else if (left || right) {
        return left ? -1 : 1;
      }
      return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
    }).map(sessionSummary);
}

export function toolState(outcome: ToolOutcome): Tool['state'] {
  if (outcome === ToolOutcome.OK) return 'finished';
  if (outcome === ToolOutcome.ERROR) return 'failed';
  return 'unknown';
}

function handbackParts(content: string): { subject: string; body: string } | null {
  const newline = content.indexOf('\n');
  if (newline < 0) return null;
  const subject = content.slice(0, newline).trimEnd();
  if (!/^Job [\da-fA-F-]{32,36} (finished\.|stopped: .+\.)$/.test(subject)) return null;
  return { subject, body: content.slice(newline + 1) };
}

export function historyMessages(history: SessionHistory): Message[] {
  const messages: Message[] = [];
  const tools = new Map<string, Tool>();
  for (const entry of history.entries) {
    const id = `history-${entry.seq}`;
    const inherited = !!history.parentSession && entry.seq <= history.forkPoint;
    const item = entry.entry;
    switch (item.case) {
      case 'message': {
        const value = item.value;
        if (value.source === Source.SYSTEM) {
          const handback = handbackParts(value.content);
          if (handback) messages.push({ id, role: 'handoff', subject: handback.subject, content: handback.body, inherited });
        } else if (value.role === Role.USER || (value.role === Role.ASSISTANT && value.content)) {
          messages.push({ id, role: value.role === Role.USER ? 'you' : 'arc', content: value.content, partial: value.partial, inherited });
        }
        break;
      }
      case 'toolCall': {
        const value = item.value;
        const tool: Tool = { id: value.callId, name: value.name, arguments: value.argumentsJson, output: '', state: 'unknown' };
        tools.set(value.callId, tool);
        messages.push({ id, role: 'arc', content: '', tools: [tool], inherited });
        break;
      }
      case 'toolResult': {
        const value = item.value;
        const tool = tools.get(value.callId);
        if (tool) {
          tool.output = value.content;
          tool.state = toolState(value.outcome);
          tool.truncated = value.truncated;
        } else {
          messages.push({ id, role: 'arc', content: '', inherited, tools: [{
            id: value.callId, name: 'Tool result', output: value.content,
            state: toolState(value.outcome), truncated: value.truncated,
          }] });
        }
        break;
      }
      case 'serverCall':
        messages.push({ id, role: 'arc', content: '', inherited, tools: [{
          id, name: item.value.name, arguments: item.value.argumentsJson, output: item.value.responseJson, state: 'finished',
        }] });
        break;
    }
  }
  return messages;
}

export function jobSummaries(live: JobInfo[]): Job[] {
  return live.map((job) => {
    const state: Job['state'] = job.state === JobInfo_State.RUNNING ? 'running'
      : job.state === JobInfo_State.FINISHED ? 'finished'
      : job.state === JobInfo_State.FAILED ? 'failed'
      : job.state === JobInfo_State.OVER_BUDGET ? 'over budget' : 'unknown';
    return { id: job.sessionId, title: job.title || 'Job', state };
  });
}
