import type { TConnectionParam } from './connection'

// Single source for the list of supported databases: it follows "kind" of the
// connection, so a database added to the connection types cannot be missed here.
export type TDbKind = TConnectionParam['kind']
