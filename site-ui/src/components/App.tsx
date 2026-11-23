import { useEffect, useMemo, useState } from 'react';
import type { SiteData } from '../types';
import { buildGraph } from '../graph/graph';
import { buildSearchIndex } from '../search/index';
import { useHashRoute } from '../hooks';
import { TopBar } from './TopBar';
import { Sidebar } from './Sidebar';
import { SearchDialog } from './SearchDialog';
import { Overview } from './Overview';
import { NodePage } from './NodePage';
import { GraphView } from './GraphView';
import { ErrorBoundary } from './ErrorBoundary';

export function App({ data }: { data: SiteData }) {
  const { model, stats } = data;
  const graph = useMemo(() => buildGraph(model), [model]);
  const index = useMemo(() => buildSearchIndex(model), [model]);
  const route = useHashRoute();
  const [searchOpen, setSearchOpen] = useState(false);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setSearchOpen(true);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
  useEffect(() => {
    document.title = `${model.project.name} docs`;
  }, [model]);

  let page;
  if (route.page === 'node') {
    const node = model.nodes[route.id];
    page = node ? (
      <NodePage key={route.id} model={model} stats={stats} graph={graph} node={node} column={route.column} />
    ) : (
      <div className="empty">Unknown node {route.id}</div>
    );
  } else if (route.page === 'graph') {
    page = <GraphView model={model} graph={graph} select={route.select} exclude={route.exclude} />;
  } else page = <Overview model={model} stats={stats} />;

  return (
    <div className="app">
      <TopBar model={model} stats={stats} onSearch={() => setSearchOpen(true)} />
      <div className="body">
        <Sidebar model={model} currentId={route.page === 'node' ? route.id : undefined} />
        <main className="main">
          <ErrorBoundary resetKey={JSON.stringify(route)}>{page}</ErrorBoundary>
        </main>
      </div>
      {searchOpen && <SearchDialog index={index} onClose={() => setSearchOpen(false)} />}
    </div>
  );
}
