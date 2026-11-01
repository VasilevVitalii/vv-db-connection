// A string literal, a quoted identifier, a line comment or a block comment - the places
// where what looks like a placeholder is just text. Everything else is code. Both escapes
// are the SQL-standard doubled quote ('' inside a string, "" inside a quoted identifier).
const SQL_ATOM = /'(?:[^']|'')*'|"(?:[^"]|"")*"|--[^\n]*|\/\*[\s\S]*?\*\//

// Calls fn for every :name placeholder that is real code - not inside a string literal, a
// quoted identifier or a comment - and replaces it with whatever fn returns. A colon that
// follows another colon is a cast/scope operator (::), never a placeholder. Everything
// else is left byte for byte as it was.
export function mapPlaceholders(query: string, fn: (name: string) => string): string {
	const scan = new RegExp(`${SQL_ATOM.source}|(?<!:):(\\w+)`, 'g')
	return query.replace(scan, (token: string, name: string | undefined) => {
		if (name === undefined) return token
		return fn(name)
	})
}
