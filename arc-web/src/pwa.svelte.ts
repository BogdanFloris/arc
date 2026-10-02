import { registerSW } from 'virtual:pwa-register';

export const pwa = $state({
  offlineReady: false,
  updateAvailable: false,
  refreshing: false,
  error: '',
  async applyUpdate() {
    pwa.refreshing = true;
    try {
      await updateServiceWorker(true);
    } catch (error) {
      pwa.error = error instanceof Error ? error.message : 'The update could not be applied.';
      pwa.refreshing = false;
    }
  },
});

const updateServiceWorker = registerSW({
  onOfflineReady() {
    pwa.offlineReady = true;
  },
  onNeedRefresh() {
    pwa.updateAvailable = true;
  },
  onRegisterError(error: unknown) {
    pwa.error = error instanceof Error ? error.message : 'Offline installation is unavailable.';
  },
});
