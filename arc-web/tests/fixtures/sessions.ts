import type { Job, Message, SessionSummary } from '../../src/lib/arc/types';

export const sessions: (SessionSummary & { lastAt: Date })[] = [
  { id: 'test-planning', title: 'Plan a focused week', preview: 'A lighter plan with room to recover.', lastAt: new Date('2025-01-03T12:00:00Z') },
  { id: 'test-debugging', title: 'Trace a flaky test', preview: 'The race is in the cleanup path.', lastAt: new Date('2025-01-02T12:00:00Z') },
  { id: 'test-writing', title: 'Notes on a first draft', preview: 'Keep the opening concrete.', lastAt: new Date('2025-01-01T12:00:00Z') }
];

export const histories: Record<string, Message[]> = {
  'test-planning': [
    { id: 'p1', role: 'you', content: 'Help me plan a focused week.' },
    { id: 'p2', role: 'arc', content: 'Pick three outcomes, then protect space for the work and recovery.' },
    { id: 'p3', role: 'arc', content: 'I grouped the commitments before suggesting a schedule.', tools: [{ id: 't1', name: 'calendar review', output: 'Two afternoons are already committed; Friday is open.' }] }
  ],
  'test-debugging': [
    { id: 'd1', role: 'you', content: 'Why does this test fail intermittently?' },
    { id: 'd2', role: 'arc', content: 'The cleanup can race with the next test. Make teardown await the worker.' }
  ],
  'test-writing': [
    { id: 'w1', role: 'you', content: 'How can I make this opening stronger?' },
    { id: 'w2', role: 'arc', content: 'Start with the specific moment, then let the broader idea emerge.' }
  ]
};

export const jobs: Job[] = [
  { id: 'job-review', title: 'Review draft outline', state: 'running' }
];

