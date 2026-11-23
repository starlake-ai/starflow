import { createRoot } from 'react-dom/client';
import '@xyflow/react/dist/style.css';
import './styles.css';
import { App } from './components/App';
import { ErrorBoundary } from './components/ErrorBoundary';
import { loadData } from './data/load';
import { initTheme } from './theme';

initTheme();
const root = createRoot(document.getElementById('root')!);
loadData()
  .then((data) =>
    root.render(
      <ErrorBoundary>
        <App data={data} />
      </ErrorBoundary>,
    ),
  )
  .catch((e: Error) => root.render(<div className="fatal">{e.message}</div>));
