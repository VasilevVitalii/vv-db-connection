import type { TDbError } from './db/types'

export type TResult<T, E = TDbError> = { ok: true; result: T } | { ok: false; error: E }
