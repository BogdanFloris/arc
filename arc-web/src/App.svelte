<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { Workspace } from './lib/workspace.svelte';
  import SessionFilters from './lib/SessionFilters.svelte';
  import Markdown from './lib/Markdown.svelte';
  import type { HostProfile } from './lib/arc/types';
  import { pwa } from './pwa.svelte';
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
  let panelKind = $state<'sessions' | 'settings' | 'jobs'>('sessions');
  let name = $state('');
  let endpoint = $state('');
  let adding = $state(false);
  let follow = $state(true);
  let ready = $state(false);
  let hostError = $state('');
  let opener: HTMLElement | null = null;
  const scrollPositions = new Map<string, { top: number; follow: boolean }>();
  let previousTranscriptKey = '';
  let previousTail = '';
  let previousScrollTop = 0;
  let activeId = $derived(workspace.activeSessionId);
  let currentHost = $derived(workspace.activeHost);
  let connected = $derived(workspace.canSend);
  let connectionLabel = $derived(workspace.connectionState === 'connected' ? 'Connected'
    : workspace.connectionState === 'connecting' ? 'Connecting…' : 'Unavailable');
  let transcriptKey = $derived(`${workspace.activeHostId ?? ''}:${activeId ?? ''}`);

  onMount(() => {
    workspace.initialize();
    ready = true;
    const resume = () => { if (document.visibilityState === 'visible') workspace.resume(); };
    window.addEventListener('online', resume);
    window.addEventListener('pageshow', resume);
    document.addEventListener('visibilitychange', resume);
    return () => {
      window.removeEventListener('online', resume);
      window.removeEventListener('pageshow', resume);
      document.removeEventListener('visibilitychange', resume);
    };
  });
  onDestroy(() => workspace.dispose());

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
    saveScrollPosition();
    workspace.selectSession(id);
    if (panel.open) panel.close();
  }

  function newConversation() {
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
    saveScrollPosition();
    workspace.selectHost(id);
  }

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
        <span class:unavailable={!connected} class="status">
          <span class="host-name" title={currentHost?.name}>{currentHost?.name ?? 'No host'}</span>
          <span>· {connectionLabel}</span>
        </span>
        {#if currentHost}
          <button class="job-status" aria-label="Jobs" onclick={(event) => open('jobs', event.currentTarget)}>
            {#if workspace.jobs.some((job) => job.state === 'running')}{workspace.jobs.filter((job) => job.state === 'running').length} job running{:else}Jobs{/if}
            <span aria-hidden="true">›</span>
          </button>
        {/if}
      </div>
    </header>

    {#if workspace.notice}<p class="notice" role="status">{workspace.notice}</p>{/if}
    <section class="transcript" aria-label="Conversation" bind:this={transcript} onscroll={onScroll}>
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
        {#if !workspace.messages.length}
          <p class="empty">{workspace.loading ? 'Loading conversation…' : !currentHost ? 'Add an ARC host in Settings to load conversations.' : !connected ? 'Connect to ARC to load conversations. Your draft stays on this device.' : activeId ? 'No messages yet.' : 'Start a conversation or choose one from Sessions.'}</p>
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
        <textarea bind:this={composer} value={workspace.draft} oninput={onDraftInput} onkeydown={onKeydown} aria-label="Message to ARC" placeholder={connected ? 'Message ARC…' : 'Draft a message (not connected)'} rows="1"></textarea>
        <div class="compose-toolbar">
          <p class="hint">
            <span>{!connected ? connectionLabel : workspace.sending ? 'Responding…' : workspace.loading ? 'Loading…' : 'ARC'}</span>
            {#if connected}<span class="keyboard-hint">Enter to send · Shift+Enter for newline</span>{/if}
          </p>
          <button class="send" type="submit" aria-label="Send" title="Send message" disabled={!connected || workspace.loading || workspace.sending || !workspace.draft.trim()}>
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 19V5m-6 6 6-6 6 6"/></svg>
          </button>
        </div>
      </div>
    </form>
  </main>
</div>

<dialog class="panel-layer" bind:this={panel} onclose={dialogClosed} aria-labelledby="panel-title">
  <button class="panel-dismiss" tabindex="-1" aria-label="Dismiss panel" onclick={dismissBackdrop}></button>
  <div class="panel-surface glass" class:list-panel={panelKind !== 'settings'} bind:this={panelSurface}>
  <div class="panel-header">
    <h2 id="panel-title" tabindex="-1" bind:this={panelTitle}>{panelKind === 'sessions' ? 'Sessions' : panelKind === 'settings' ? 'Settings' : 'Jobs'}</h2>
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
  {:else}
    <p class="muted">{connected ? 'Current daemon jobs, not archived conversations.' : 'Last known daemon jobs; reconnect to refresh.'}</p>
    <div class="dialog-list">
      {#each workspace.jobs as job (job.id)}
        <article class="job"><strong>{job.title}</strong><span class="meta">{job.state}</span><button class="quiet-action" onclick={() => selectSession(job.id)}>Open job conversation</button></article>
      {:else}<p class="muted">No jobs on this host.</p>{/each}
    </div>
  {/if}
  </div>
</dialog>
