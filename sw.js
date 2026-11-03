var CACHE = 'primos-pwa-v4';
var archivos = [
  '/',
  '/index.html',
  '/manifest.json'
];

self.addEventListener('install', function(e) {
  e.waitUntil(
    caches.open(CACHE).then(function(cache) {
      console.log('[SW] Cache abierto');
      return cache.addAll(archivos);
    })
  );
  self.skipWaiting();
});

self.addEventListener('activate', function(e) {
  e.waitUntil(
    caches.keys().then(function(nombres) {
      return Promise.all(
        nombres.filter(function(n) { return n !== CACHE; })
              .map(function(n) { return caches.delete(n); })
      );
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', function(e) {
  var url = new URL(e.request.url);
  if (url.pathname.startsWith('/api/')) {
    e.respondWith(
      fetch(e.request).catch(function() {
        return new Response(
          JSON.stringify({ok: false, error: 'sin conexion (offline)'}),
          {headers: {'Content-Type': 'application/json'}}
        );
      })
    );
    return;
  }
  e.respondWith(
    caches.match(e.request).then(function(resp) {
      if (resp) return resp;
      return fetch(e.request).then(function(r) {
        if (r && r.status === 200) {
          var copia = r.clone();
          caches.open(CACHE).then(function(c) { c.put(e.request, copia); });
        }
        return r;
      });
    }).catch(function() {
      return new Response('<h1>Sin conexion</h1><p>Vuelve a intentar cuando tengas internet.</p>', {
        headers: {'Content-Type': 'text/html'}
      });
    })
  );
});
