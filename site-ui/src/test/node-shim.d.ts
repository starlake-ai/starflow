// The UI does not depend on @types/node; tests that read repository files only need these.
declare module 'node:fs' {
  export function readFileSync(path: string, encoding: 'utf8'): string;
}
declare module 'node:path' {
  export function dirname(path: string): string;
  export function resolve(...paths: string[]): string;
}
declare module 'node:url' {
  export function fileURLToPath(url: string): string;
}
interface ImportMeta {
  readonly url: string;
}
