import { describe, expect, it } from 'vitest';
import { create } from '@bufbuild/protobuf';
import { SessionStatusSchema } from './arc/gen/wire_pb';
import { allowanceIsStale, observedAt, resetAt, windowLabel, statusWarning } from './status';

describe('status readings', () => {
  it('warns for measured context or fresh low allowance, never stale allowance', () => {
    const status = create(SessionStatusSchema, { allowanceObservedAt: 1000n, allowance: [{ remainingPercent: 5 }] });
    expect(statusWarning(null, 1000000)).toBe('');
    expect(statusWarning(status, 1000000)).toBe('Low allowance');
    expect(statusWarning(status, 1120001)).toBe('');
    status.context = { $typeName: 'arc.v1.ContextMeasured', sessionId: 'session', inputTokens: 900, contextWindow: 1000 };
    expect(statusWarning(status, 1120001)).toBe('Context high');
  });

  it('expires allowances independently of the server freshness flag', () => {
    const status = create(SessionStatusSchema, { allowanceObservedAt: 1000n });
    expect(allowanceIsStale(status, 1120000)).toBe(false);
    expect(allowanceIsStale(status, 1120001)).toBe(true);
    status.allowanceStale = true;
    expect(allowanceIsStale(status, 1000000)).toBe(true);
    status.allowanceStale = false;
    status.allowanceObservedAt = 0n;
    expect(allowanceIsStale(status, 1000000)).toBe(true);
  });

  it('shows unknown dates explicitly and formats supplied observations and windows', () => {
    expect(observedAt(0n, Date.now())).toBe('Not reported');
    expect(observedAt(1000n, 1020000)).toContain('20s ago');
    expect(observedAt(1000n, 1001000 + 86400000)).toContain('1d ago');
    expect(resetAt()).toBe('Reset time unavailable');
    expect(resetAt(2n ** 63n)).toBe('Reset time unavailable');
    expect(windowLabel()).toBe('window');
    expect(windowLabel(604800n)).toBe('week');
    expect(windowLabel(18000n)).toBe('5h');
    expect(windowLabel(90n)).toBe('1m');
    expect(windowLabel(604800n, 'rolling')).toBe('rolling');
  });
});
