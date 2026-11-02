// A string literal, a quoted identifier, a line comment or a block comment - the places
// where what looks like a placeholder is just text. Everything else is code. Both escapes
// are the SQL-standard doubled quote ('' inside a string, "" inside a quoted identifier);
// a backslash escape (\') also covers pg's E'' strings, [...] covers MSSQL's
// bracket-quoted identifiers, and $tag$...$tag$ covers pg's dollar-quoted strings (DO
// blocks, function bodies) - the tag may be empty ($$...$$) and is back-referenced so the
// closer must match the opener.
export const SQL_ATOM =
	/'(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*"|\[[^\]]*\]|\$([A-Za-z_]\w*)?\$[\s\S]*?\$\1\$|--[^\n]*|\/\*[\s\S]*?\*\//

// Calls fn for every :name placeholder that is real code - not inside a string literal, a
// quoted identifier or a comment - and replaces it with whatever fn returns. A colon that
// follows another colon is a cast/scope operator (::), never a placeholder. A name starts
// with a letter or underscore, so a slice like arr[1:2] is not a placeholder either.
// Everything else is left byte for byte as it was. `to` optionally bounds the scan: a
// placeholder at or past that offset is left alone (EXECUTE BLOCK passes the index of its
// AS - the PSQL body after it must not be rewritten).
export function mapPlaceholders(query: string, fn: (name: string) => string, to?: number): string {
	const scan = new RegExp(`${SQL_ATOM.source}|(?<!:):(?<ph>[A-Za-z_]\\w*)`, 'g')
	return query.replace(scan, (...args: unknown[]): string => {
		const match = args[0] as string
		// With a named group the callback args are (match, ...captures, offset, string,
		// groups), so the name comes from groups and the offset sits two before the end.
		const groups = args[args.length - 1] as { ph?: string }
		if (groups.ph === undefined) return match
		const offset = args[args.length - 3] as number
		if (to !== undefined && offset >= to) return match
		return fn(groups.ph)
	})
}
