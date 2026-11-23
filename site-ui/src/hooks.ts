import { useCallback, useEffect, useState } from 'react';
import { parseHash, type Route } from './router';

/** Calls `f` on first use only and returns the same value afterwards. */
export function memo<T>(f: () => T): () => T {
  let done = false;
  let value: T;
  return () => {
    if (!done) {
      value = f();
      done = true;
    }
    return value;
  };
}

export function useHashRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener('hashchange', onChange);
    return () => window.removeEventListener('hashchange', onChange);
  }, []);
  return route;
}

export function usePersistentSet(
  key: string,
  initial: string[],
): [Set<string>, (k: string) => void, (keys: string[]) => void] {
  const [set, setSet] = useState<Set<string>>(() => {
    try {
      const raw = localStorage.getItem(key);
      return new Set(raw ? (JSON.parse(raw) as string[]) : initial);
    } catch {
      return new Set(initial);
    }
  });
  const update = useCallback(
    (change: (next: Set<string>) => void) =>
      setSet((prev) => {
        const next = new Set(prev);
        change(next);
        if (next.size === prev.size && [...next].every((k) => prev.has(k))) return prev;
        try {
          localStorage.setItem(key, JSON.stringify([...next]));
        } catch {
          // storage unavailable: keep the state in memory only
        }
        return next;
      }),
    [key],
  );
  const toggle = useCallback(
    (k: string) => update((next) => (next.has(k) ? next.delete(k) : next.add(k))),
    [update],
  );
  const expand = useCallback((keys: string[]) => update((next) => keys.forEach((k) => next.add(k))), [update]);
  return [set, toggle, expand];
}
