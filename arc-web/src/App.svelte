<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { Workspace } from './lib/workspace.svelte';
  import SessionFilters from './lib/SessionFilters.svelte';
  import Markdown from './lib/Markdown.svelte';
  import type { HostProfile } from './lib/arc/types';
  import { pwa } from './pwa.svelte';
  import { allowanceIsStale, observedAt, resetAt, windowLabel, statusWarning } from './lib/status';
  import logo from '../assets/logo.svg';

  function createWorkspace(): Workspace {
    let initialHost: HostProfile | undefined;
    if (window.location.protocol === 'https:') {
      const host = window.location.hostname.split('.')[0];
      initialHost = { id: 'same-origin', name: host.charAt(0).toUpperCase() + host.slice(1),
        endpoint: new URL('/arc', window.location.origin).toString().replace(/^https:/, 'wss:'), kind: 'daemon' };
    }
    try {
      return new Workspace(window.localStorage, undefined, initialHost);
    } catch {
      return new Workspace(null, undefined, initialHost);
    }
  }
  const workspace = createWorkspace();
  let panel: HTMLDialogElement;
  let panelSurface: HTMLDivElement;
  let panelTitle: HTMLHeadingElement;
  let composer: HTMLTextAreaElement;
  let transcript: HTMLElement;
  let panelKind = $state<'sessions' | 'settings' | 'jobs' | 'status'>('sessions');
  let name = $state('');
  let endpoint = $state('');
  let adding = $state(false);
  let follow = $state(true);
  let ready = $state(false);
  let existingModelMenu = $state('__recorded__');
  let hostError = $state('');
  let jobReturn = $state<{ hostId: string | null; sessionId: string | null; title: string; jobId: string } | null>(null);
  let opener: HTMLElement | null = null;
  const scrollPositions = new Map<string, { top: number; follow: boolean }>();
  let previousTranscriptKey = '';
  let previousTail = '';
  let previousScrollTop = 0;
  let statusNow = $state(Date.now());
  let activeId = $derived(workspace.activeSessionId);
  let currentHost = $derived(workspace.activeHost);
  let composerProject = $derived(workspace.activeSession?.project ?? workspace.newProject);
  let connected = $derived(workspace.canSend);
  let connectionLabel = $derived(workspace.connectionState === 'connected' ? 'Connected'
    : workspace.connectionState === 'connecting' ? 'Connecting' : 'Disconnected');
  let readingWarning = $derived(statusWarning(workspace.sessionStatus, statusNow));
  let transcriptKey = $derived(`${workspace.activeHostId ?? ''}:${activeId ?? ''}`);

  onMount(() => {
    const statusTimer = setInterval(() => {
      const now = Date.now();
      if ((panel?.open && panelKind === 'status') || now - statusNow >= 30_000) statusNow = now;
    }, 1000);
    workspace.initialize();
    ready = true;
    const resume = () => { if (document.visibilityState === 'visible') workspace.resume(); };
    window.addEventListener('online', resume);
    window.addEventListener('pageshow', resume);
    document.addEventListener('visibilitychange', resume);
    return () => {
      clearInterval(statusTimer);
      window.removeEventListener('online', resume);
      window.removeEventListener('pageshow', resume);
      document.removeEventListener('visibilitychange', resume);
    };
  });
  onDestroy(() => workspace.dispose());

  async function openStatus(source: HTMLElement) {
    statusNow = Date.now();
    await open('status', source);
    void workspace.refreshStatus();
  }

  async function open(kind: typeof panelKind, source: HTMLElement) {
    panelKind = kind;
    opener = source;
    await tick();
    panel.showModal();
    panelTitle.focus({ preventScroll: true });
  }

  function dialogClosed() {
    opener?.focus();
    opener = null;
  }

  function dismissBackdrop(event: MouseEvent) {
    const bounds = panelSurface.getBoundingClientRect();
    if (event.detail === 0 || event.clientX < bounds.left || event.clientX > bounds.right ||
      event.clientY < bounds.top || event.clientY > bounds.bottom) panel.close();
  }

  function selectSession(id: string) {
    jobReturn = null;
    saveScrollPosition();
    workspace.selectSession(id);
    if (panel.open) panel.close();
  }

  function newConversation() {
    jobReturn = null;
    saveScrollPosition();
    workspace.newConversation();
    if (panel.open) panel.close();
    composer?.focus();
  }

  function saveScrollPosition() {
    if (transcript && activeId) {
      scrollPositions.set(transcriptKey, { top: transcript.scrollTop, follow });
    }
  }

  function selectHost(id: string) {
    jobReturn = null;
    saveScrollPosition();
    workspace.selectHost(id);
  }

  function openJob(id: string) {
    const route = jobReturn ?? {
      hostId: workspace.activeHostId,
      sessionId: workspace.activeSessionId,
      title: workspace.activeSession ? workspace.activeTitle : 'new conversation',
      jobId: id
    };
    jobReturn = { ...route, jobId: id };
    saveScrollPosition();
    workspace.selectSession(id);
    if (panel.open) panel.close();
  }

  function returnFromJob() {
    const route = jobReturn;
    if (!route || route.hostId !== workspace.activeHostId) return;
    jobReturn = null;
    saveScrollPosition();
    if (route.sessionId) workspace.selectSession(route.sessionId);
    else workspace.newConversation();
    if (panel.open) panel.close();
  }

  $effect(() => {
    const route = jobReturn;
    if (route && (workspace.activeHostId !== route.hostId || activeId !== route.jobId)) jobReturn = null;
  });

  function onScroll() {
    if (!transcript) return;
    const top = transcript.scrollTop;
    const atBottom = transcript.scrollHeight - top - transcript.clientHeight < 48;
    // Markdown layout growth is not a request to stop following.
    if (top < previousScrollTop - 1 || atBottom) follow = atBottom;
    previousScrollTop = top;
    saveScrollPosition();
  }

  function markdownRendered() {
    if (!transcript) return;
    if (transcript.scrollTop < previousScrollTop - 1) onScroll();
    if (follow) {
      transcript.scrollTo({ top: transcript.scrollHeight });
      previousScrollTop = transcript.scrollTop;
    }
  }

  $effect(() => {
    const msgs = workspace.messages;
    const key = transcriptKey;
    const tail = msgs.length ? `${msgs[msgs.length - 1].id}:${msgs[msgs.length - 1].content.length}` : '';
    if (!ready) return;
    if (key !== previousTranscriptKey) {
      previousTranscriptKey = key;
      previousTail = tail;
      const saved = scrollPositions.get(key);
      requestAnimationFrame(() => {
        if (!transcript || key !== transcriptKey) return;
        follow = saved?.follow ?? true;
        transcript.scrollTop = follow ? transcript.scrollHeight : saved?.top ?? 0;
        previousScrollTop = transcript.scrollTop;
      });
      return;
    }
    if (tail !== previousTail) {
      previousTail = tail;
      if (follow) requestAnimationFrame(() => {
        if (follow && key === transcriptKey) markdownRendered();
      });
    }
  });

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    await workspace.send();
  }

  function cancelModelFork() {
    workspace.cancelModelFork();
    existingModelMenu = '__recorded__';
  }

  function onDraftInput(event: Event) {
    const value = (event.currentTarget as HTMLTextAreaElement).value;
    workspace.setDraft(value);
  }

  $effect(() => {
    workspace.draft;
    if (ready) tick().then(resizeComposer);
  });

  function resizeComposer() {
    if (!composer) return;
    composer.style.height = 'auto';
    composer.style.height = `${Math.min(composer.scrollHeight, 180)}px`;
  }

  function onKeydown(event: KeyboardEvent) {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing && event.keyCode !== 229) {
      event.preventDefault();
      composer.form?.requestSubmit();
    }
  }

  function addHost(event: SubmitEvent) {
    event.preventDefault();
    try {
      workspace.saveHost(name.trim(), endpoint.trim());
      name = '';
      endpoint = '';
      adding = false;
      hostError = '';
    } catch (error) {
      hostError = error instanceof Error ? error.message : 'Could not save this host.';
    }
  }
</script>

<svelte:head>
  <title>ARC</title>
  <meta name="theme-color" content="#282828" />
</svelte:head>

<div class="shell">
  <aside class="sidebar" aria-label="Sessions">
    <div class="brand"><img src={logo} alt="ARC" /></div>
    <div class="eyebrow">SESSIONS</div>
    <SessionFilters {workspace} />
    <nav class="session-list" aria-label="Session list">
      {#each workspace.visibleSessions as session (session.id)}
        <button class="session" aria-current={activeId === session.id ? 'true' : undefined} onclick={() => selectSession(session.id)}>
          <span>{session.title}</span>
          {#if session.preview}<small>{session.preview}</small>{/if}
        </button>
      {/each}
    </nav>
  </aside>

  <main class="main">
    <header class="top">
      <button class="icon-button glass mobile-only" aria-label="Open sessions" title="Sessions" onclick={(event) => open('sessions', event.currentTarget)}>
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h16M4 12h11M4 18h16"/></svg>
      </button>
      <div class="title">{workspace.activeTitle}</div>
      <div class="header-actions">
        <button class="icon-button glass" aria-label="New conversation" title="New conversation" onclick={newConversation}>
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1h6m5-1 5 5-9 9-6 1 1-6 9-9Z"/></svg>
        </button>
        <button class="icon-button glass" aria-label="Settings" title="Settings" onclick={(event) => open('settings', event.currentTarget)}>
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 17h16"/><circle cx="9" cy="7" r="3" fill="var(--bg)"/><circle cx="15" cy="17" r="3" fill="var(--bg)"/></svg>
        </button>
      </div>
      <div class="context">
        <button class="status-button" aria-label="ARC status" onclick={(event) => openStatus(event.currentTarget)}>
          <span class:unavailable={workspace.connectionState === 'unavailable'} class:connecting={workspace.connectionState === 'connecting'} class="status">
            <span class="status-dot" aria-hidden="true"></span><span class="host-name" title={currentHost?.name}>{currentHost?.name ?? 'No host'}</span><span>· {connectionLabel}</span>
          </span>
        </button>
        {#if currentHost}
          <button class="job-status" aria-label="Jobs" onclick={(event) => open('jobs', event.currentTarget)}>
            {#if workspace.jobs.some((job) => job.state === 'running')}{workspace.jobs.filter((job) => job.state === 'running').length} job running{:else}Jobs{/if}
            <span aria-hidden="true">›</span>
          </button>
        {/if}
      </div>
    </header>

    {#if jobReturn}<div class="job-return"><button class="quiet-action" onclick={returnFromJob}>Back to {jobReturn.title}</button></div>{/if}
    <section class="transcript" aria-label="Conversation" aria-busy={workspace.loading || workspace.connectionState === 'connecting'} bind:this={transcript} onscroll={onScroll}>
      <div class="transcript-content">
        {#each workspace.messages as message (message.id)}
          <article class="message" class:from-user={message.role === 'you'} class:activity-row={message.role === 'handoff' || (!message.content && !!message.tools?.length)}>
            {#if message.role === 'handoff'}
              <details class="tool handoff">
                <summary aria-label={`Job handoff: ${message.subject}`}>
                  <span class="tool-name" title={message.subject}>{message.subject}</span>
                  <svg class="tool-chevron" viewBox="0 0 24 24" aria-hidden="true"><path d="m9 5 7 7-7 7"/></svg>
                </summary>
                <div class="handoff-output"><Markdown content={message.content} /></div>
              </details>
            {:else if message.content || message.streaming}
              <div class="role">{message.role === 'you' ? 'YOU' : 'ARC'}{#if message.streaming}<span class="meta"> · responding</span>{/if}{#if message.delivery}<span class="meta"> · {message.delivery}</span>{/if}{#if message.partial}<span class="meta"> · partial</span>{/if}{#if message.inherited}<span class="meta"> · inherited</span>{/if}</div>
              {#if message.role === 'arc'}
                <Markdown content={message.content} streaming={message.streaming} onrender={markdownRendered} />
              {:else}
                <p>{message.content}</p>
              {/if}
            {/if}
            {#each message.tools ?? [] as tool (tool.id)}
              <details class="tool tool-call">
                <summary aria-label={`Tool activity: ${tool.name}`}>
                  <svg class="tool-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 6 5 5-5 5m8 0h6"/></svg>
                  <span class="tool-name" title={tool.name}>{tool.name}</span>
                  <span class="tool-label">{tool.state === 'running' ? 'running' : tool.state === 'failed' ? 'failed' : 'output'}</span>
                  <svg class="tool-chevron" viewBox="0 0 24 24" aria-hidden="true"><path d="m9 5 7 7-7 7"/></svg>
                </summary>
                {#if tool.arguments}<pre class="tool-arguments">{tool.arguments}</pre>{/if}
                <pre class="tool-output">{tool.output || (tool.state === 'running' ? 'Waiting for output…' : 'No recorded output.')}</pre>
                {#if tool.truncated}<small class="muted">Recorded output was truncated.</small>{/if}
              </details>
            {/each}
          </article>
        {/each}
        {#if ready && !workspace.messages.length && !workspace.loading && (!currentHost || connected)}
          <p class="empty">{!currentHost ? 'Add an ARC host in Settings to load conversations.' : activeId ? 'No messages yet.' : 'Start a conversation or choose one from Sessions.'}</p>
        {/if}
      </div>
    </section>
    <form class="composer" onsubmit={submit}>
      {#each workspace.uncertainInputs as input (input.id)}
        <details class="uncertain-input">
          <summary>{input.state === 'uncertain' ? 'Delivery unknown — check history before sending again' : 'Message not accepted — input saved'}</summary>
          <pre>{input.content}</pre>
          <button class="quiet-action" type="button" onclick={() => workspace.restoreInput(input.id)}>Restore to draft</button>
          <button class="quiet-action" type="button" onclick={() => workspace.dismissInput(input.id)}>Dismiss</button>
        </details>
      {/each}
      {#if !follow && workspace.messages.length}
        <button class="jump" type="button" aria-label="Jump to latest" title="Jump to latest" onclick={() => { follow = true; transcript.scrollTo({ top: transcript.scrollHeight, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth' }); }}>
          <span aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 5v14m-6-6 6 6 6-6"/></svg></span>
        </button>
      {/if}
      <div class="compose-row">
        <textarea bind:this={composer} value={workspace.draft} oninput={onDraftInput} onkeydown={onKeydown} aria-label="Message to ARC" placeholder="Message ARC…" rows="1"></textarea>
        <div class="compose-toolbar">
          <div class="conversation-controls" aria-label="Conversation settings">
            <div class="project-choice">
              {#if workspace.isEmptyConversation}
                <select aria-label="Project" title="Conversation project" value={composerProject} disabled={!workspace.canConfigure} onchange={(event) => workspace.selectNewProject(event.currentTarget.value)}>
                  {#if composerProject && !workspace.projectOptions.includes(composerProject)}<option value={composerProject}>{composerProject} (unavailable)</option>{/if}
                  <option value="">No project</option>
                  {#each workspace.projectOptions as project}<option value={project}>{project}</option>{/each}
                </select>
              {:else}<span class="control-fixed composer-project" title={composerProject || 'No project'}>{composerProject || 'No project'}</span>{/if}
            </div>
            <div class="model-choice">
              {#if !workspace.activeSessionId}
                <select aria-label="Model" title="Model preset" value={workspace.newModelChoice} disabled={!workspace.canConfigure || !workspace.availableModels.length} onchange={(event) => workspace.chooseModel(event.currentTarget.value)}>
                  {#if workspace.newModelChoice && !workspace.availableModels.some((choice) => choice.name === workspace.newModelChoice)}<option value={workspace.newModelChoice}>{workspace.newModelChoice} (unavailable)</option>{/if}
                  {#if !workspace.availableModels.length}<option value="">No models</option>{/if}
                  {#each workspace.availableModels as choice}<option value={choice.name}>{choice.name}</option>{/each}
                </select>
              {:else}
                <select aria-label="Model" title={workspace.recordedModel} bind:value={existingModelMenu} disabled={!workspace.canConfigure || !workspace.availableModels.length} onchange={() => { if (existingModelMenu !== '__recorded__') { workspace.chooseModel(existingModelMenu); existingModelMenu = '__recorded__'; } }}>
                  <option value="__recorded__">{workspace.recordedModel}</option>
                  {#each workspace.availableModels as choice}<option value={choice.name}>{choice.name}</option>{/each}
                </select>
              {/if}
            </div>
            <div class="thinking-control">
              {#if !workspace.sessionStatus && workspace.isEmptyConversation}
                <button class="thinking-button" type="button" aria-label="Choose thinking" title="Choose thinking effort" disabled={!workspace.canConfigure} onclick={() => void workspace.prepareThinking()}>{workspace.statusLoading ? '…' : workspace.effectiveThinking || 'thinking'}</button>
              {:else if workspace.sessionStatus?.supportedThinking.length}
                <select aria-label="Thinking" title="Thinking effort" value={workspace.sessionStatus.supportedThinking.includes(workspace.effectiveThinking) ? workspace.effectiveThinking : ''} disabled={!workspace.canConfigure} onchange={(event) => void workspace.setThinking(event.currentTarget.value)}>
                  {#if !workspace.sessionStatus.supportedThinking.includes(workspace.effectiveThinking)}<option value="">{workspace.effectiveThinking || 'Unknown'} · current</option>{/if}
                  {#each workspace.sessionStatus.supportedThinking as level}<option value={level}>{level}</option>{/each}
                </select>
              {:else}<span class="control-fixed thinking-readonly" aria-label={`Thinking ${workspace.effectiveThinking || 'unknown'}, read-only`} title="Current effort; the daemon does not offer changes for this model.">{workspace.effectiveThinking || '—'}</span>{/if}
            </div>
          </div>
          <button class="send" type="submit" aria-label="Send" title="Send message" disabled={!connected || workspace.loading || workspace.sending || workspace.controlsBusy || !workspace.draft.trim()}>
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 19V5m-6 6 6-6 6 6"/></svg>
          </button>
        </div>
        {#if connected}<p class="keyboard-hint">Enter to send · Shift+Enter for newline</p>{/if}
      </div>
      {#if workspace.pendingModelFork}
        <div class="fork-confirm" role="group" aria-label="Confirm model fork">
          <span>Fork conversation with {workspace.pendingModelFork}?</span>
          <button type="button" onclick={() => void workspace.confirmModelFork()}>Fork</button>
          <button type="button" onclick={cancelModelFork}>Cancel</button>
        </div>
      {/if}
    </form>
  </main>
</div>

<dialog class="panel-layer" bind:this={panel} onclose={dialogClosed} aria-labelledby="panel-title">
  <button class="panel-dismiss" tabindex="-1" aria-label="Dismiss panel" onclick={dismissBackdrop}></button>
  <div class="panel-surface glass" class:list-panel={panelKind !== 'settings'} bind:this={panelSurface}>
  <div class="panel-header">
    <h2 id="panel-title" tabindex="-1" bind:this={panelTitle}>{panelKind === 'sessions' ? 'Sessions' : panelKind === 'settings' ? 'Settings' : panelKind === 'jobs' ? 'Jobs' : 'ARC status'}</h2>
    <button class="icon-button" aria-label="Close" title="Close panel" onclick={() => panel.close()}>
      <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18"/></svg>
    </button>
  </div>
  {#if panelKind === 'sessions'}
    <SessionFilters {workspace} />
    <div class="dialog-list">
      {#each workspace.visibleSessions as session (session.id)}
        <button class="session" aria-current={activeId === session.id ? 'true' : undefined} onclick={() => selectSession(session.id)}>
          <span>{session.title}</span>{#if session.preview}<small>{session.preview}</small>{/if}
        </button>
      {:else}<p class="muted">{workspace.sessions.length ? 'No sessions match these filters.' : 'No conversations on this host.'}</p>{/each}
    </div>
  {:else if panelKind === 'settings'}
    <section class="settings-section" aria-labelledby="connection-title">
      <h3 id="connection-title">Connection</h3>
      <div class="settings-group host-list">
        {#each workspace.hosts as host (host.id)}
          <div class="host-row">
            <button class="host-select" aria-current={workspace.activeHostId === host.id ? 'true' : undefined} onclick={() => selectHost(host.id)}>
              <svg class="host-icon" viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="4" width="16" height="12" rx="2"/><path d="M8 20h8m-4-4v4"/></svg>
              <span class="host-copy"><strong>{host.name}</strong><small title={host.endpoint}>{host.endpoint}</small></span>
              {#if workspace.activeHostId === host.id}
                <svg class="host-check" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg>
              {/if}
            </button>
            <button class="icon-button remove" aria-label={`Remove ${host.name}`} title={`Remove ${host.name}`} onclick={() => workspace.removeHost(host.id)}>
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13M10 11v5m4-5v5"/></svg>
            </button>
          </div>
        {/each}
      </div>
      {#if adding}
        <form class="host-form" onsubmit={addHost}>
          <label>Name<input bind:value={name} placeholder="Erebor" required /></label>
          <label>Endpoint<input bind:value={endpoint} type="url" placeholder="wss://erebor.example/ws" autocapitalize="off" spellcheck="false" required /></label>
          <div class="form-actions">
            <button class="action" type="submit">Add host</button>
            <button class="quiet-action" type="button" onclick={() => { adding = false; hostError = ''; }}>Cancel</button>
          </div>
        </form>
      {:else}
        <button class="add-host" onclick={() => adding = true}><span aria-hidden="true">＋</span>Add daemon host</button>
      {/if}
      <div class="connection-actions">
        <p class="settings-note">{currentHost ? connectionLabel : 'No host configured'}</p>
        {#if currentHost}<button class="quiet-action" onclick={() => workspace.resume()}>Reconnect / refresh</button>{/if}
      </div>
      {#if hostError}<p class="panel-notice" role="alert">{hostError}</p>{/if}
    </section>
    <section class="settings-section" aria-labelledby="app-settings-title">
      <h3 id="app-settings-title">App</h3>
      <div class="settings-group">
        <div class="settings-row">
          <div class="setting-copy"><strong>Updates</strong><small>{pwa.updateAvailable ? 'A new version is ready' : 'No update waiting'}</small></div>
          {#if pwa.updateAvailable}
            <button class="quiet-action" disabled={pwa.refreshing} onclick={() => pwa.applyUpdate()}>{pwa.refreshing ? 'Updating…' : 'Apply update'}</button>
          {/if}
        </div>
        {#if pwa.offlineReady}
          <div class="settings-row">
            <div class="setting-copy"><strong>Offline launch</strong><small>App shell only · ARC needs a connection</small></div>
            <span class="settings-value">Ready</span>
          </div>
        {/if}
      </div>
      {#if pwa.error}<p class="panel-notice" role="alert">{pwa.error}</p>{/if}
    </section>
  {:else if panelKind === 'status'}
    <div class="status-content">
      <section class="status-section"><h3>Connection</h3><p>{currentHost?.name ?? 'No host'} · {connectionLabel}</p><button class="quiet-action" disabled={workspace.statusLoading} onclick={() => { if (connected) void workspace.refreshStatus(); else workspace.resume(); }}>Refresh</button></section>
      {#if workspace.activeSessionId || workspace.newProject || workspace.newModelChoice || workspace.effectiveThinking}
        <section class="status-section"><h3>Session</h3>{#if workspace.activeSessionId}<p>{workspace.activeSession?.project || 'No project'} · {workspace.recordedModel}</p><p>{workspace.activeSession?.provider || 'Provider unknown'}</p>{:else}<p>{workspace.newProject || 'No project'} · {workspace.newModelChoice || 'No model selected'}</p>{/if}<p>Thinking · {workspace.effectiveThinking || 'Unknown'}</p></section>
      {/if}
      {#if workspace.sessionStatus}
        {@const context = workspace.sessionStatus.context}
        <section class="status-section"><h3>Context</h3><p>{context ? `${context.inputTokens.toLocaleString()} tokens${context.contextWindow ? ` of ${context.contextWindow.toLocaleString()}` : ' · window unknown'}` : 'Not measured · window unknown'}{#if !connected || workspace.statusError} · last known{/if}</p>{#if context?.compactAt}<p>Compaction at {context.compactAt.toLocaleString()} tokens</p>{/if}</section>
        {@const allowanceStale = allowanceIsStale(workspace.sessionStatus, statusNow)}
        <section class="status-section"><h3>Account allowance{#if allowanceStale && workspace.sessionStatus.allowance.length} · stale{/if}</h3>{#if workspace.sessionStatus.allowance.length}{#each workspace.sessionStatus.allowance as window}<p>{windowLabel(window.windowSeconds, window.label)} · {window.remainingPercent}% remaining · resets {resetAt(window.resetsAt)}</p>{/each}{:else}<p>{workspace.sessionStatus.codex || workspace.sessionStatus.allowanceSource ? 'Unavailable' : 'Not reported by this provider'}</p>{/if}<p>{workspace.sessionStatus.allowanceSource || 'Source unavailable'} · {observedAt(workspace.sessionStatus.allowanceObservedAt, statusNow)}</p></section>
      {:else}
        <section class="status-section"><h3>Context</h3><p>{workspace.activeSessionId ? 'Reading unavailable' : 'Not measured'}</p></section>
      {/if}
      {#if workspace.notice || workspace.controlsError || workspace.statusError || readingWarning}
        <section class="status-section"><h3>Notices</h3>{#if readingWarning}<p>{readingWarning}</p>{/if}{#if workspace.notice}<p>{workspace.notice}</p>{/if}{#if workspace.controlsError}<p>{workspace.controlsError}</p>{/if}{#if workspace.statusError}<p>{workspace.statusError}</p>{/if}</section>
      {/if}
    </div>
  {:else}
    <p class="muted">{connected ? 'Current daemon jobs, not archived conversations.' : 'Last known daemon jobs; reconnect to refresh.'}</p>
    <div class="dialog-list">
      {#each workspace.jobs as job (job.id)}
        <article class="job"><div class="job-copy"><strong>{job.title}</strong><span class="meta">{job.state}</span></div><button class="quiet-action job-open" onclick={() => openJob(job.id)}>Open</button></article>
      {:else}<p class="muted">No jobs on this host.</p>{/each}
    </div>
  {/if}
  </div>
</dialog>
