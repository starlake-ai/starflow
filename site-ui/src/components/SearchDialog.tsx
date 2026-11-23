import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import type { SearchHit, SearchIndex } from '../search/index';
import { nodeHref } from '../router';
import { KindBadge } from './common';

export function SearchDialog({ index, onClose }: { index: SearchIndex; onClose: () => void }) {
  const [q, setQ] = useState('');
  const [active, setActive] = useState(0);
  const hits = useMemo(() => index.search(q), [index, q]);
  const list = useRef<HTMLUListElement>(null);

  // Give focus back to whatever opened the dialog once it closes. Captured during the first render,
  // because autoFocus moves focus to the input before any effect runs.
  const [opener] = useState(() => (document.activeElement instanceof HTMLElement ? document.activeElement : null));
  useEffect(() => () => opener?.focus(), [opener]);
  useEffect(() => {
    list.current?.querySelector<HTMLElement>(`#search-hit-${active}`)?.scrollIntoView({ block: 'nearest' });
  }, [active, hits]);

  const go = (h: SearchHit) => {
    window.location.hash = nodeHref(h.nodeId, h.column);
    onClose();
  };
  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      e.stopPropagation();
      onClose();
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, hits.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Enter' && hits[active]) go(hits[active]);
  };
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label="Search the project"
        onClick={(e) => e.stopPropagation()}
        onKeyDown={onKey}
      >
        <input
          autoFocus
          placeholder="Search tables, tasks, columns, SQL"
          aria-label="Search"
          role="combobox"
          aria-expanded={hits.length > 0}
          aria-controls="search-hits"
          aria-activedescendant={hits[active] ? `search-hit-${active}` : undefined}
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setActive(0);
          }}
        />
        <ul className="hits" id="search-hits" role="listbox" aria-label="Results" ref={list}>
          {hits.map((h, i) => (
            <li
              key={h.id}
              id={`search-hit-${i}`}
              role="option"
              aria-selected={i === active}
              className={i === active ? 'active' : ''}
              onMouseEnter={() => setActive(i)}
              onClick={() => go(h)}
            >
              <KindBadge kind={h.kind} /> {h.label}
            </li>
          ))}
        </ul>
        {q.trim() && hits.length === 0 && <p className="muted no-hits">No results</p>}
      </div>
    </div>
  );
}
