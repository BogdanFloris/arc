export type HostProfile = {
  id: string;
  name: string;
  endpoint: string;
  kind: 'daemon';
};

export type SessionSummary = { id: string; title: string; preview: string; project?: string };

export type Tool = {
  id: string;
  name: string;
  output: string;
  arguments?: string;
  state?: 'running' | 'finished' | 'failed' | 'unknown';
  truncated?: boolean;
};

export type Message = {
  id: string;
  role: 'you' | 'arc' | 'handoff';
  content: string;
  subject?: string;
  streaming?: boolean;
  partial?: boolean;
  inherited?: boolean;
  delivery?: 'pending' | 'uncertain';
  tools?: Tool[];
};

export type Job = {
  id: string;
  title: string;
  state: 'running' | 'finished' | 'failed' | 'over budget' | 'unknown';
};
