import { useEffect, useMemo, useState } from 'react';
import type { SiteModel } from '../types';
import { databaseTree, filterTree, pathTo, projectTree, type TreeNode } from '../tree';
import { usePersistentSet } from '../hooks';
import { nodeHref } from '../router';

type TreeProps = { nodes: TreeNode[]; open: Set<string>; toggle: (k: string) => void; forceOpen: boolean; currentId?: string };

function Tree({ nodes, open, toggle, forceOpen, currentId }: TreeProps) {
  return (
    <ul className="tree">
      {nodes.map((n) => {
        if (n.nodeId)
          return (
            <li key={n.key}>
              <a
                className={n.nodeId === currentId ? 'leaf current' : 'leaf'}
                aria-current={n.nodeId === currentId ? 'page' : undefined}
                href={nodeHref(n.nodeId)}
              >
                {n.label}
              </a>
            </li>
          );
        const expanded = forceOpen || open.has(n.key);
        return (
          <li key={n.key}>
            <button className="branch" aria-expanded={expanded} onClick={() => toggle(n.key)}>
              {expanded ? '▾' : '▸'} {n.label}
            </button>
            {expanded && <Tree nodes={n.children} open={open} toggle={toggle} forceOpen={forceOpen} currentId={currentId} />}
          </li>
        );
      })}
    </ul>
  );
}

const TABS = [
  ['project', 'Project'],
  ['database', 'Database'],
] as const;

export function Sidebar({ model, currentId }: { model: SiteModel; currentId?: string }) {
  const [tab, setTab] = useState<'project' | 'database'>('project');
  const [filter, setFilter] = useState('');
  const [open, toggle, expand] = usePersistentSet('sl-tree-open', ['load', 'transform']);
  const trees = useMemo(() => ({ project: projectTree(model), database: databaseTree(model) }), [model]);
  const shown = useMemo(() => filterTree(trees[tab], filter), [trees, tab, filter]);
  // Reveal the page being viewed: open every branch on a path to it.
  useEffect(() => {
    if (currentId) expand(pathTo(trees[tab], currentId));
  }, [currentId, trees, tab, expand]);
  return (
    <aside className="sidebar">
      <div className="tabs" role="tablist" aria-label="Sidebar view">
        {TABS.map(([key, label]) => (
          <button
            key={key}
            role="tab"
            aria-selected={tab === key}
            className={tab === key ? 'active' : ''}
            onClick={() => setTab(key)}
          >
            {label}
          </button>
        ))}
      </div>
      <input className="filter" placeholder="Filter" aria-label="Filter the tree" value={filter} onChange={(e) => setFilter(e.target.value)} />
      <Tree nodes={shown} open={open} toggle={toggle} forceOpen={filter.trim() !== ''} currentId={currentId} />
    </aside>
  );
}
