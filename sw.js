// Atlas service worker — network-first so the app always loads the latest version.
// Bumping CACHE purges any older cache (e.g. the previous cache-first 'atlas-v1'
// that could pin a stale/broken build) on activate.
const CACHE = 'atlas-v3';

self.addEventListener('install', e => { self.skipWaiting(); });

self.addEventListener('activate', e => e.waitUntil((async () => {
  const keys = await caches.keys();
  await Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)));
  await self.clients.claim();
})()));

// Network-first: always try the network, fall back to cache only when offline.
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  e.respondWith((async () => {
    try {
      const fresh = await fetch(e.request);
      const cache = await caches.open(CACHE);
      cache.put(e.request, fresh.clone());
      return fresh;
    } catch (_) {
      const cached = await caches.match(e.request);
      return cached || Response.error();
    }
  })());
});
