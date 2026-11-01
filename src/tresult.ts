export type TResult<T, E = string> = { ok: true; result: T } | { ok: false; error: E }
