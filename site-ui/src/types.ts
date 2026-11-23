export type NodeKind = 'load' | 'transform' | 'external';
export type Json = Record<string, unknown>;

// The Scala serializer omits empty values (None, "", empty list or map), so every field that can be
// empty is optional. Consumers must default them (for example node.tags ?? []).

export type SiteColumn = {
  name: string;
  rename?: string | null;
  type: string;
  sqlType?: string | null;
  required: boolean;
  primaryKey: boolean;
  foreignKey?: string | null;
  foreignKeyNode?: string | null;
  privacy?: string | null;
  script?: string | null;
  default?: string | null;
  metricType?: string | null;
  description?: string | null;
  tags?: string[];
  /** Present only when true: declared but not created in the warehouse. */
  ignore?: boolean;
  columns?: SiteColumn[];
};

export type SiteSql = { raw?: string | null; compiled?: string | null };
export type SiteExpectation = { expect: string; failOnError: boolean };
export type SiteAcl = { role: string; grants?: string[] };
export type SiteRls = { name: string; predicate: string; grants?: string[]; description?: string };

export type SiteNode = {
  id: string;
  kind: NodeKind;
  name: string;
  /** Absent for an external table referenced without a schema. */
  domain?: string;
  table: string;
  database?: string | null;
  connectionRef?: string | null;
  engine?: string | null;
  description?: string | null;
  tags?: string[];
  filePath?: string | null;
  columns?: SiteColumn[];
  expectations?: SiteExpectation[];
  acl?: SiteAcl[];
  rls?: SiteRls[];
  sink?: Json | null;
  dag?: string | null;
  primaryKey?: string[];
  presql?: string[];
  postsql?: string[];
  writeStrategy?: Json | null;
  pattern?: string | null;
  patternSample?: string | null;
  metadata?: Json | null;
  sql?: SiteSql | null;
  python?: string | null;
  freshness?: Json | null;
  schedule?: string | null;
  warnings?: string[];
};

export type SiteEdge = { from: string; to: string; kind: 'lineage' | 'fk' };
export type SiteColumnEdge = { from: string; to: string; task: string };
export type SiteGroup = { name: string; description?: string | null; nodes: string[] };
export type SiteDagGroup = { name: string; template?: string; comment?: string; nodes: string[] };

export type SiteModel = {
  schemaVersion: number;
  generatedAt: string;
  starlakeVersion: string;
  project: { name: string; env?: string; defaultConnection?: string; overview?: string | null };
  nodes: Record<string, SiteNode>;
  edges?: SiteEdge[];
  columnEdges?: SiteColumnEdge[];
  groups: { domains?: SiteGroup[]; jobs?: SiteGroup[]; dags?: SiteDagGroup[] };
  docs?: Record<string, string>;
};

export type ColumnDiffStatus = 'match' | 'mismatch' | 'missingInWarehouse' | 'extraInWarehouse';
export type ColumnDiff = {
  name: string;
  declared?: string | null;
  actual?: string | null;
  status: ColumnDiffStatus;
};

export type TableStats = {
  connectionRef: string;
  exists: boolean;
  tableType?: string | null;
  format?: string | null;
  rowCount?: number | null;
  rowCountEstimated?: boolean;
  bytes?: number | null;
  lastModified?: string | null;
  columns?: { name: string; type: string; nullable: boolean }[];
  columnDiff?: ColumnDiff[];
};

export type ConnectionStats = {
  engine: string;
  status: 'ok' | 'error' | 'unsupported';
  error?: string | null;
  durationMs?: number;
};

export type SiteStats = {
  schemaVersion: number;
  generatedAt: string;
  connections?: Record<string, ConnectionStats>;
  tables?: Record<string, TableStats>;
};

export type SiteData = { model: SiteModel; stats: SiteStats | null };
