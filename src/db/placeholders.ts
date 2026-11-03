// A string literal, a quoted identifier, a line comment or a block comment - the places
// where what looks like a placeholder is just text. Everything else is code. The
// SQL-standard escape for both kinds of quoted text is a doubled quote ('' inside a
// string, "" inside a quoted identifier); a backslash is an ordinary character in them,
// exactly as it is in MSSQL, Oracle, Firebird and in pg's standard (non-E) strings -
// so 'C:\' is a complete string and \' does not swallow what follows. Only pg's E''
// strings treat a backslash as an escape, and those are recognized separately in
// SQL_ATOM_PG. pg's dollar-quoted strings ($tag$...$tag$ or the empty $$...$$, used by
// DO blocks and function bodies) live in SQL_ATOM_PG only: in the other dialects a $ is
// an ordinary character, so a :name inside $...$ is still a placeholder there.
//
// MSSQL's bracket-quoted identifiers ([...]) are deliberately NOT part of this atom:
// only MSSQL quotes identifiers with brackets, and pg needs the brackets to stay code so
// that ARRAY[:a, :b] and arr[:i] substitute their placeholders. The bracket form lives in
// SQL_ATOM_MSSQL, which the mssql adapter passes explicitly.
export const SQL_ATOM =
	/'(?:[^']|'')*'|"(?:[^"]|"")*"|--[^\n]*|\/\*[\s\S]*?\*\//

// SQL_ATOM plus MSSQL's bracket-quoted identifiers, so [a:b] stays a single identifier
// rather than being split into [a and @b].
export const SQL_ATOM_MSSQL = new RegExp(`\\[[^\\]]*\\]|${SQL_ATOM.source}`)

// SQL_ATOM plus pg's escape strings (E'' or e''), the one place where a backslash does
// escape the next character: E'C:\' holds a single backslash, not the end of the string.
// The leading E/e must stand alone - a typed literal like date'...' or time'...' ends in
// 'e' too, so without the lookbehind its leading token would be mistaken for an E-string.
const PG_ESCAPE_STRING = /(?<![\w$])[Ee]'(?:[^'\\]|\\.|'')*'/
// pg's dollar-quoted strings ($tag$...$tag$, or the empty tag $$...$$): the tag is
// back-referenced so the closer must match the opener. This is pg-only - in mssql,
// oracle and firebird a $ is an ordinary character, so $tag$...$tag$ is code and a
// :name inside it is still a placeholder.
const PG_DOLLAR_QUOTE = /\$(?<tag>[A-Za-z_]\w*)?\$[\s\S]*?\$\k<tag>\$/
export const SQL_ATOM_PG = new RegExp(`${PG_ESCAPE_STRING.source}|${PG_DOLLAR_QUOTE.source}|${SQL_ATOM.source}`)

// Oracle's alternative quoting: q'[text]' for any bracket, or q'XtextX' for any other
// single-byte delimiter X (the closer is the matching bracket, or X again). The whole
// thing is one string literal - an apostrophe inside it does not end it, and a :name
// inside it is plain text, not a placeholder. The leading q/Q is matched without regard
// to case (Oracle accepts q', Q', nq', NQ', ... - an optional leading n/N marks a
// national-character literal). The bracket forms are listed first so a delimiter like [
// never falls through to the generic named-group-with-backreference branch, and the
// delimiter itself is a named group (?<q>.) so its backreference \k<q> cannot be knocked
// loose by groups added to the atom this one is spliced into (SQL_ATOM_ORA).
const ORA_Q_QUOTE = /[nN]?[qQ]'(?:\[[^\]]*\]|\{[^}]*\}|\([^)]*\)|<[^>]*>|(?<q>.)[\s\S]*?\k<q>)'/
export const SQL_ATOM_ORA = new RegExp(`${ORA_Q_QUOTE.source}|${SQL_ATOM.source}`)

// Calls fn for every :name placeholder that is real code - not inside a string literal, a
// quoted identifier or a comment - and replaces it with whatever fn returns. A colon that
// follows another colon is a cast/scope operator (::), never a placeholder. A name starts
// with a letter or underscore, so a slice like arr[1:2] is not a placeholder either.
// Everything else is left byte for byte as it was. `to` optionally bounds the scan: a
// placeholder at or past that offset is left alone (EXECUTE BLOCK passes the index of its
// AS - the PSQL body after it must not be rewritten). `atom` picks the atoms to skip:
// pg passes SQL_ATOM_PG so its E'' strings are treated as strings, the other dialects use
// the default SQL_ATOM, where a backslash is an ordinary character.
export function mapPlaceholders(query: string, fn: (name: string) => string, to?: number, atom: RegExp = SQL_ATOM): string {
	const scan = new RegExp(`${atom.source}|(?<!:):(?<ph>[A-Za-z_]\\w*)`, 'g')
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
