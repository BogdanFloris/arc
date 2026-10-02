<script lang="ts">
  import { onDestroy, tick, untrack } from 'svelte';
  import { renderMarkdown } from './markdown';

  let { content, streaming = false, onrender }: {
    content: string;
    streaming?: boolean;
    onrender?: () => void;
  } = $props();

  let html = $state('');
  let renderedContent = '';
  let timer: ReturnType<typeof setTimeout> | undefined;
  let disposed = false;

  async function renderLatest() {
    if (timer) clearTimeout(timer);
    timer = undefined;
    if (content === renderedContent) return;
    html = renderMarkdown(content);
    renderedContent = content;
    await tick();
    if (!disposed) onrender?.();
  }

  $effect(() => {
    const latest = content;
    const active = streaming;
    if (latest === renderedContent) return;
    if (!active) {
      untrack(() => void renderLatest());
    } else if (!timer) {
      timer = setTimeout(() => void renderLatest(), 125);
    }
  });

  onDestroy(() => {
    disposed = true;
    if (timer) clearTimeout(timer);
  });
</script>

<div class="markdown">{@html html}</div>
