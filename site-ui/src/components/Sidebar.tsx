import { useMemo, useState } from 'react';
import type { SiteModel } from '../types';
import { databaseTree, filterTree, projectTree, type TreeNode } from '../tree';
import { usePersistentSet } from '../hooks';
import { nodeHref } from '../router';

type TreeProps = { nodes: TreeNode[]; open: Set<string>; toggle: (k: string) => void; forceOpen: boolean; currentId?: string };

function Tree({ nodes, open, toggle, forceOpen, currentId }: TreeProps) {
  return (
    <ul className="tree">
      {nodes.map((n) =>
        n.nodeId ? (
          <li key={n.key}>
            <a className={n.nodeId === currentId ? 'leaf current' : 'leaf'} href={nodeHref(n.nodeId)}>
              {n.label}
            </a>
          </li>
        ) : (
          <li key={n.key}>
            <button className="branch" onClick={() => toggle(n.key)}>
              {forceOpen || open.has(n.key) ? '▾' : '▸'} {n.label}
            </button>
            {(forceOpen || open.has(n.key)) && (
              <Tree nodes={n.children} open={open} toggle={toggle} forceOpen={forceOpen} currentId={currentId} />
            )}
          </li>
        ),
      )}
    </ul>
  );
}

export function Sidebar({ model, currentId }: { model: SiteModel; currentId?: string }) {
  const [tab, setTab] = useState<'project' | 'database'>('project');
  const [filter, setFilter] = useState('');
  const [open, toggle] = usePersistentSet('sl-tree-open', ['load', 'transform']);
  const trees = useMemo(() => ({ project: projectTree(model), database: databaseTree(model) }), [model]);
  const shown = useMemo(() => filterTree(trees[tab], filter), [trees, tab, filter]);
  return (
    <aside className="sidebar">
      <div className="tabs">
        <button className={tab === 'project' ? 'active' : ''} onClick={() => setTab('project')}>
          Project
        </button>
        <button className={tab === 'database' ? 'active' : ''} onClick={() => setTab('database')}>
          Database
        </button>
      </div>
      <input className="filter" placeholder="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} />
      <Tree nodes={shown} open={open} toggle={toggle} forceOpen={filter.trim() !== ''} currentId={currentId} />
    </aside>
  );
}
