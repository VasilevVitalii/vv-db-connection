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
const PG_DOLLAR_QUOTE = /(?<![\w$])\$(?<tag>[A-Za-z_]\w*)?\$[\s\S]*?\$\k<tag>\$/
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
const ORA_Q_QUOTE = /(?<![\w$])[nN]?[qQ]'(?:\[[^\]]*\]|\{[^}]*\}|\([^)]*\)|<[^>]*>|(?<q>.)[\s\S]*?\k<q>)'/
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
// The scan regex is `atom` plus the placeholder alternative; rebuilding it on every call
// recompiles on each exec (and in a transaction, on every statement). Cache by atom - the
// atom set is closed (SQL_ATOM, SQL_ATOM_PG, SQL_ATOM_ORA, SQL_ATOM_MSSQL), so the cache
// stays tiny. String.replace resets a global regex's lastIndex, so reuse is safe across
// calls - but the regex is shared: fn must NOT re-enter mapPlaceholders with the same
// atom (a call inside fn would reset lastIndex mid-scan and corrupt the outer match).
const scanCache = new Map<RegExp, RegExp>()
function scanFor(atom: RegExp): RegExp {
	let scan = scanCache.get(atom)
	if (scan === undefined) {
		scan = new RegExp(`${atom.source}|(?<!:):(?<ph>[A-Za-z_]\\w*)`, 'g')
		scanCache.set(atom, scan)
	}
	return scan
}

export function mapPlaceholders(query: string, fn: (name: string) => string, to?: number, atom: RegExp = SQL_ATOM): string {
	const scan = scanFor(atom)
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

// pg nests block comments (/* /* x */ y */); the flat SQL_ATOM block-comment alternative
// stops at the first */ and would expose the tail of an outer comment, letting a :name
// inside it be rewritten. This pre-pass masks every well-formed block comment - nesting
// included - to a `<sep><index><sep>` token (no colon, so the placeholder scan cannot see
// into it); the caller restores the tokens after mapPlaceholders. Strings, quoted
// identifiers and line comments are copied through verbatim and stay in place for
// SQL_ATOM_PG to handle, so a /* inside a string or identifier is not treated as a
// comment. An unterminated block comment is left as-is (a SQL error, not this parser's
// to fix). The separator is picked below from control characters absent from the input,
// so it cannot collide with the text being masked - including a literal NUL, which would
// otherwise break restore().
export function maskPgBlockComments(sql: string): { masked: string; restore: (s: string) => string } {
	const comments: string[] = []
	// The mask token is `<sep><index><sep>`. sep must never occur in the input, or
	// restore() would split a literal copy of it and rewrite that text into a comment.
	// NUL is the usual choice (illegal in SQL text), but rather than assume, take the
	// first control character absent from sql - control characters other than \t \n \r
	// are not legal SQL, so the first free one is guaranteed unique. The private-use
	// fallback only covers a degenerate input made entirely of control characters.
	const CONTROL = '\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u000B\u000C\u000E\u000F\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F'
	const sep = [...CONTROL].find((c) => !sql.includes(c)) ?? '\uE000'
	let out = ''
	let i = 0
	while (i < sql.length) {
		const c = sql[i]
		if ((c === 'E' || c === 'e') && sql[i + 1] === "'" && !/[\w$]/.test(sql[i - 1] ?? '')) {
			// pg escape string (E''): a backslash escapes the next character, so \' is
			// not the end of the string. Skipped verbatim so a /* inside it stays text.
			out += c + "'"; i += 2
			while (i < sql.length) {
				out += sql[i]
				if (sql[i] === '\\') { out += sql[i + 1] ?? ''; i += 2; continue }
				if (sql[i] === "'") {
					if (sql[i + 1] === "'") { out += "'"; i += 2; continue }
					i++; break
				}
				i++
			}
			continue
		}
		if (c === "'") {
			// String literal: '' doubles the quote, a backslash is an ordinary character
			// in a standard string.
			out += c; i++
			while (i < sql.length) {
				out += sql[i]
				if (sql[i] === "'") {
					if (sql[i + 1] === "'") { out += "'"; i += 2; continue }
					i++; break
				}
				i++
			}
			continue
		}
		if (c === '"') {
			// Quoted identifier: "" doubles the quote.
			out += c; i++
			while (i < sql.length) {
				out += sql[i]
				if (sql[i] === '"') {
					if (sql[i + 1] === '"') { out += '"'; i += 2; continue }
					i++; break
				}
				i++
			}
			continue
		}
		if (c === '$' && !/[\w$]/.test(sql[i - 1] ?? '')) {
			// pg dollar-quoted string: $tag$...$tag$ or the empty $$...$$. The closer
			// must repeat the tag, so a /* inside it is plain text, not a comment.
			const dq = /^\$(?<tag>[A-Za-z_]\w*)?\$/.exec(sql.slice(i))
			if (dq) {
				const close = `$${dq.groups!.tag ?? ''}$`
				const end = sql.indexOf(close, i + dq[0].length)
				if (end >= 0) {
					out += sql.slice(i, end + close.length)
					i = end + close.length
					continue
				}
				// unterminated: fall through and copy the $ literally.
			}
		}
		if (c === '-' && sql[i + 1] === '-') {
			// Line comment runs to the end of the line.
			const nl = sql.indexOf('\n', i)
			const end = nl < 0 ? sql.length : nl
			out += sql.slice(i, end)
			i = end
			continue
		}
		if (c === '/' && sql[i + 1] === '*') {
			// Block comment, possibly nested: balance /* against */.
			let depth = 1
			let j = i + 2
			while (j < sql.length && depth > 0) {
				if (sql[j] === '/' && sql[j + 1] === '*') { depth++; j += 2; continue }
				if (sql[j] === '*' && sql[j + 1] === '/') { depth--; j += 2; continue }
				j++
			}
			if (depth === 0) {
				comments.push(sql.slice(i, j))
				out += `${sep}${comments.length - 1}${sep}`
				i = j
				continue
			}
			// Unterminated: fall through and copy the two characters literally.
		}
		out += c
		i++
	}
	return {
		masked: out,
		restore: (s: string): string => {
			let r = s
			// split/join, not replace: a comment may contain `$` (e.g. `$tag$`, `$1`), which
			// replace() would treat as a special pattern.
			for (let k = comments.length - 1; k >= 0; k--) r = r.split(`${sep}${k}${sep}`).join(comments[k]!)
			return r
		},
	}
}
