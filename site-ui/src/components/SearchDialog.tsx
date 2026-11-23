import { useMemo, useState, type KeyboardEvent } from 'react';
import type { SearchHit, SearchIndex } from '../search/index';
import { nodeHref } from '../router';
import { KindBadge } from './common';

export function SearchDialog({ index, onClose }: { index: SearchIndex; onClose: () => void }) {
  const [q, setQ] = useState('');
  const [active, setActive] = useState(0);
  const hits = useMemo(() => index.search(q), [index, q]);
  const go = (h: SearchHit) => {
    window.location.hash = nodeHref(h.nodeId, h.column);
    onClose();
  };
  const onKey = (e: KeyboardEvent) => {
    if (e.key === 'Escape') onClose();
    else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, hits.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === 'Enter' && hits[active]) go(hits[active]);
  };
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <input
          autoFocus
          placeholder="Search tables, tasks, columns, SQL"
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setActive(0);
          }}
          onKeyDown={onKey}
        />
        <ul className="hits">
          {hits.map((h, i) => (
            <li key={h.id} className={i === active ? 'active' : ''} onMouseEnter={() => setActive(i)} onClick={() => go(h)}>
              <KindBadge kind={h.kind} /> {h.label}
            </li>
          ))}
          {q.trim() && hits.length === 0 && <li className="muted">No results</li>}
        </ul>
      </div>
    </div>
  );
}
