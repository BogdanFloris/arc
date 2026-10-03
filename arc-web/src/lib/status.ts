import type { SessionStatus } from './arc/gen/wire_pb';

function observedDate(seconds?: bigint) {
  if (seconds === undefined || seconds <= 0n) return null;
  const date = new Date(Number(seconds) * 1000);
  return Number.isFinite(date.getTime()) ? date : null;
}

export function allowanceIsStale(status: SessionStatus, now: number) {
  const observed = observedDate(status.allowanceObservedAt);
  return status.allowanceStale || !observed || now - observed.getTime() > 120_000;
}

export function statusWarning(status: SessionStatus | null, now: number) {
  if (!status) return '';
  const context = status.context;
  if (context?.contextWindow && context.inputTokens >= context.contextWindow * 0.9) return 'Context high';
  if (!allowanceIsStale(status, now) && status.allowance.some((window) => window.remainingPercent <= 10)) return 'Low allowance';
  return '';
}

export function observedAt(seconds: bigint, now: number) {
  const date = observedDate(seconds);
  if (!date) return 'Not reported';
  const age = Math.max(0, Math.floor((now - date.getTime()) / 1000));
  const elapsed = age < 60 ? `${age}s` : age < 3600 ? `${Math.floor(age / 60)}m`
    : age < 86400 ? `${Math.floor(age / 3600)}h` : `${Math.floor(age / 86400)}d`;
  return `${date.toLocaleString()} · ${elapsed} ago`;
}

export function windowLabel(seconds?: bigint, label = '') {
  if (label) return label;
  if (seconds === undefined || seconds <= 0n) return 'window';
  return seconds === 604800n ? 'week' : seconds % 3600n === 0n
    ? `${seconds / 3600n}h` : `${seconds / 60n}m`;
}

export function resetAt(seconds?: bigint) {
  return observedDate(seconds)?.toLocaleString() ?? 'Reset time unavailable';
}
