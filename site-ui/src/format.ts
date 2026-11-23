const trim = (x: number): string => (x >= 100 ? Math.round(x).toString() : x.toFixed(1).replace(/\.0$/, ''));

export function formatCount(n: number | null | undefined, estimated = false): string {
  if (n == null) return '';
  if (!estimated) return n.toLocaleString('en-US');
  const units: [number, string][] = [[1e9, 'B'], [1e6, 'M'], [1e3, 'k']];
  for (const [v, u] of units) if (Math.abs(n) >= v) return `~${trim(n / v)}${u}`;
  return `~${n}`;
}

export function formatBytes(b: number | null | undefined): string {
  if (b == null) return '';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let v = b;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(1).replace(/\.0$/, '')} ${units[i]}`;
}
