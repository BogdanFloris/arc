<script lang="ts">
  import type { Workspace } from './workspace.svelte';
  let { workspace }: { workspace: Workspace } = $props();
</script>

<div class="session-filters">
  <div class="project-picker">
    <svg class="project-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M3 7V5a1 1 0 0 1 1-1h5l2 3h9a1 1 0 0 1 1 1v11a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V7Z"/></svg>
    <select class="project-select" aria-label="Project" bind:value={workspace.selectedProject}>
      <option value="">All projects</option>
      {#each workspace.projectOptions as project}
        <option value={project}>{project}</option>
      {/each}
    </select>
    <svg class="project-chevron" viewBox="0 0 24 24" aria-hidden="true"><path d="m7 10 5 5 5-5"/></svg>
  </div>
</div>
