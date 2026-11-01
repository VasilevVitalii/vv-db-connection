// A string literal, a quoted identifier, a line comment or a block comment - the places
// where what looks like a placeholder is just text. Everything else is code. Both escapes
// are the SQL-standard doubled quote ('' inside a string, "" inside a quoted identifier).
export const SQL_ATOM = /'(?:[^']|'')*'|"(?:[^"]|"")*"|--[^\n]*|\/\*[\s\S]*?\*\//

// Calls fn for every :name placeholder that is real code - not inside a string literal, a
// quoted identifier or a comment - and replaces it with whatever fn returns. A colon that
// follows another colon is a cast/scope operator (::), never a placeholder. Everything
// else is left byte for byte as it was. `to` optionally bounds the scan: a placeholder at
// or past that offset is left alone (EXECUTE BLOCK passes the index of its AS - the PSQL
// body after it must not be rewritten).
export function mapPlaceholders(query: string, fn: (name: string) => string, to?: number): string {
	const scan = new RegExp(`${SQL_ATOM.source}|(?<!:):(\\w+)`, 'g')
	return query.replace(scan, (token: string, name: string | undefined, offset: number) => {
		if (name === undefined) return token
		if (to !== undefined && offset >= to) return token
		return fn(name)
	})
}
