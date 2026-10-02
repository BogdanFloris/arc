import { describe, expect, it } from 'vitest';
import { create } from '@bufbuild/protobuf';
import { conversationSessions, historyMessages, isConversation, jobSummaries } from './history';
import { BranchMarked_Disposition, Role, SessionRole, Source, ToolOutcome } from './gen/events_pb';
import { JobInfoSchema, JobInfo_State, SessionInfoSchema } from './gen/wire_pb';

describe('ARC history mapping', () => {
  it('maps user, system, assistant and ordered tool events', () => {
    const messages = historyMessages({ entries: [
      { seq: 1, entry: { case: 'message', value: { source: Source.USER, role: Role.USER, content: 'question' } } },
      { seq: 2, entry: { case: 'message', value: { source: Source.MODEL, role: Role.ASSISTANT, content: 'working' } } },
      { seq: 3, entry: { case: 'toolCall', value: { callId: 'c1', name: 'shell', argumentsJson: '{"cmd":"ls"}' } } },
      { seq: 4, entry: { case: 'toolResult', value: { callId: 'c1', content: 'output', outcome: ToolOutcome.ERROR, truncated: true } } },
      { seq: 5, entry: { case: 'message', value: { source: Source.SYSTEM, role: Role.ASSISTANT, content: 'handed back' } } },
    ] } as never);
    expect(messages.map(({ role }) => role)).toEqual(['you', 'arc', 'arc']);
    expect(messages[2].tools?.[0]).toMatchObject({ id: 'c1', name: 'shell', arguments: '{"cmd":"ls"}', output: 'output', state: 'failed', truncated: true });
  });

  it('collapses exact daemon job handbacks and omits system prompts without folding user prose', () => {
    const subject = 'Job 01234567-89ab-cdef-0123-456789abcdef finished.';
    const messages = historyMessages({ entries: [
      { seq: 1n, entry: { case: 'message', value: { source: Source.SYSTEM, role: Role.USER, content: `${subject}\nThe work is done.` } } },
      { seq: 2n, entry: { case: 'message', value: { source: Source.SYSTEM, role: Role.USER, content: 'Internal handoff instruction' } } },
      { seq: 3n, entry: { case: 'message', value: { source: Source.USER, role: Role.USER, content: `${subject}\nMy own words.` } } },
      { seq: 4n, entry: { case: 'message', value: { source: Source.SYSTEM, role: Role.USER, content: 'Job not-a-uuid finished.\nNot a daemon handback.' } } },
    ] } as never);
    expect(messages).toMatchObject([
      { role: 'handoff', subject, content: 'The work is done.' },
      { role: 'you', content: `${subject}\nMy own words.` },
    ]);
  });

  it('uses the TUI source split rather than role or parent metadata', () => {
    expect(isConversation({ id: 'user', source: Source.USER } as never)).toBe(true);
    expect(isConversation({ id: 'model', source: Source.MODEL } as never)).toBe(false);
    expect(isConversation({ id: 'unspecified', source: Source.UNSPECIFIED } as never)).toBe(false);
    expect(isConversation({ id: 'system', source: Source.SYSTEM } as never)).toBe(false);
    expect(isConversation({ id: 'user-executor', source: Source.USER, role: SessionRole.EXECUTOR, dispatchedBy: 'parent' } as never)).toBe(true);
  });

  it('orders newest activity first with started-at fallback, nanoseconds and stable ID ties', () => {
    const session = (id: string, seconds?: bigint, nanos = 0) => create(SessionInfoSchema, {
      id, title: id, source: Source.USER, startedAt: seconds === undefined ? undefined : { seconds, nanos },
    });
    const sessions = [session('undated'), session('b', 10n), session('a', 10n),
      session('nanos', 10n, 1), session('huge', 2n ** 60n),
      create(SessionInfoSchema, { id: 'active', title: 'active', source: Source.USER, startedAt: { seconds: 1n }, lastAt: { seconds: 11n } })];
    expect(conversationSessions(sessions).map((row) => row.id)).toEqual(['huge', 'active', 'nanos', 'a', 'b', 'undated']);
    expect(sessions[0].id).toBe('undated');
  });

  it('hides empty, non-user and abandoned sessions, but keeps untitled sessions with activity', () => {
    const sessions = [
      create(SessionInfoSchema, { id: 'empty', source: Source.USER, startedAt: { seconds: 10n } }),
      create(SessionInfoSchema, { id: 'untitled', source: Source.USER, lastAt: { seconds: 0n } }),
      create(SessionInfoSchema, { id: 'job', title: 'Job', source: Source.MODEL }),
      create(SessionInfoSchema, { id: 'unknown', title: 'Unknown', source: Source.UNSPECIFIED }),
      create(SessionInfoSchema, { id: 'abandoned', title: 'Wrong turn', source: Source.USER, disposition: BranchMarked_Disposition.ABANDONED }),
    ];
    expect(conversationSessions(sessions).map((row) => row.id)).toEqual(['untitled']);
    expect(conversationSessions(sessions, { showAbandoned: true }).map((row) => row.id)).toEqual(['untitled', 'abandoned']);
  });

  it('defaults to all projects and scopes only when a project is explicitly selected', () => {
    const sessions = [
      create(SessionInfoSchema, { id: 'arc', title: 'Fix transport', preview: 'Origin checks', project: 'arc', source: Source.USER }),
      create(SessionInfoSchema, { id: 'arc-web', title: 'Sort sessions', preview: 'Recent activity', project: 'arc-web', source: Source.USER }),
      create(SessionInfoSchema, { id: 'unscoped', title: 'Read a book', source: Source.USER }),
    ];
    expect(conversationSessions(sessions, { project: 'arc' }).map((row) => row.id)).toEqual(['arc']);
    expect(conversationSessions(sessions).map((row) => row.id)).toEqual(['arc', 'arc-web', 'unscoped']);
    expect(conversationSessions(sessions, { project: 'arc-web' }).map((row) => row.id)).toEqual(['arc-web']);
  });

  it('shows only daemon job reports in daemon order without fabricating failure states', () => {
    const live = [
      create(JobInfoSchema, { sessionId: 'running', state: JobInfo_State.RUNNING }),
      create(JobInfoSchema, { sessionId: 'finished', state: JobInfo_State.FINISHED }),
      create(JobInfoSchema, { sessionId: 'budget', state: JobInfo_State.OVER_BUDGET }),
      create(JobInfoSchema, { sessionId: 'unknown', state: JobInfo_State.UNSPECIFIED }),
    ];
    expect(jobSummaries(live).map((job) => [job.id, job.state])).toEqual([
      ['running', 'running'], ['finished', 'finished'], ['budget', 'over budget'], ['unknown', 'unknown'],
    ]);
    expect(jobSummaries([])).toEqual([]);
  });
});
