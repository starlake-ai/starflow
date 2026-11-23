// Node 25 and later define a global localStorage (and sessionStorage) that is undefined unless
// node runs with --localstorage-file, and it shadows the jsdom one vitest installs. Put the jsdom
// storages back so the tests behave as in a browser on every Node version.
const dom = (globalThis as { jsdom?: { window: Window } }).jsdom;
if (dom) {
  for (const name of ['localStorage', 'sessionStorage'] as const) {
    Object.defineProperty(globalThis, name, {
      value: dom.window[name],
      configurable: true,
      writable: true,
    });
  }
}
