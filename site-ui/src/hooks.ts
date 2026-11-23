import { useCallback, useEffect, useState } from 'react';
import { parseHash, type Route } from './router';

export function useHashRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener('hashchange', onChange);
    return () => window.removeEventListener('hashchange', onChange);
  }, []);
  return route;
}

export function usePersistentSet(key: string, initial: string[]): [Set<string>, (k: string) => void] {
  const [set, setSet] = useState<Set<string>>(() => {
    try {
      const raw = localStorage.getItem(key);
      return new Set(raw ? (JSON.parse(raw) as string[]) : initial);
    } catch {
      return new Set(initial);
    }
  });
  const toggle = useCallback(
    (k: string) =>
      setSet((prev) => {
        const next = new Set(prev);
        if (next.has(k)) next.delete(k);
        else next.add(k);
        try {
          localStorage.setItem(key, JSON.stringify([...next]));
        } catch {
          // storage unavailable: keep the state in memory only
        }
        return next;
      }),
    [key],
  );
  return [set, toggle];
}
