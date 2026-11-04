// TResult now lives in db/types.ts (to break the tresult <-> types import cycle). This
// shim keeps every existing `from '../tresult'` / `from '../../tresult'` import working.
export type { TResult } from './db/types'
