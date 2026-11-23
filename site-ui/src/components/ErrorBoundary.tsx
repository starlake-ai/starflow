import { Component, type ErrorInfo, type ReactNode } from 'react';

type Props = { children: ReactNode; /** The boundary resets when this changes (the current route). */ resetKey?: string };
type State = { error: Error | null; key?: string };

/** Shows the failure and the current route instead of a blank page; recovers on navigation. */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null, key: this.props.resetKey };

  static getDerivedStateFromError(error: unknown): Partial<State> {
    return { error: error instanceof Error ? error : new Error(String(error)) };
  }

  static getDerivedStateFromProps(props: Props, state: State): Partial<State> | null {
    return props.resetKey !== state.key ? { error: null, key: props.resetKey } : null;
  }

  private onHashChange = () => {
    if (this.state.error) this.setState({ error: null });
  };

  componentDidMount() {
    window.addEventListener('hashchange', this.onHashChange);
  }

  componentWillUnmount() {
    window.removeEventListener('hashchange', this.onHashChange);
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Site UI render error', error, info.componentStack);
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;
    return (
      <div className="fatal" role="alert">
        <h2>This page failed to render</h2>
        <p>
          <code>{error.message}</code>
        </p>
        <p className="muted">Route: {window.location.hash || '#/'}</p>
        <p>
          <a href="#/">Back to the overview</a>
        </p>
      </div>
    );
  }
}
