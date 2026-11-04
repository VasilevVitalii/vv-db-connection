import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mapPlaceholders, maskPgBlockComments, SQL_ATOM_PG, SQL_ATOM_ORA, SQL_ATOM_MSSQL } from '../src/db/placeholders'

const toAt = (name: string): string => `@${name}`

test('replaces :name in plain code', () => {
	assert.equal(mapPlaceholders('SELECT :v', toAt), 'SELECT @v')
	assert.equal(mapPlaceholders('SELECT :a, :a, :b', toAt), 'SELECT @a, @a, @b')
	assert.equal(mapPlaceholders(':a :b', toAt), '@a @b')
})

test('leaves a single-quoted string literal alone', () => {
	assert.equal(mapPlaceholders("SELECT 'a:b' AS x", toAt), "SELECT 'a:b' AS x")
	assert.equal(mapPlaceholders("SELECT '12:30'", toAt), "SELECT '12:30'")
	assert.equal(mapPlaceholders("SELECT ':v'", toAt), "SELECT ':v'")
})

test('handles doubled quotes inside a string literal', () => {
	assert.equal(mapPlaceholders("SELECT 'it''s :v'", toAt), "SELECT 'it''s :v'")
})

test('leaves a double-quoted identifier alone', () => {
	assert.equal(mapPlaceholders('SELECT "a:b" AS x', toAt), 'SELECT "a:b" AS x')
})

test('leaves a line comment alone', () => {
	assert.equal(mapPlaceholders('SELECT 1 -- :v\nFROM t', toAt), 'SELECT 1 -- :v\nFROM t')
})

test('leaves a block comment alone', () => {
	assert.equal(mapPlaceholders('SELECT /* :v */ 1', toAt), 'SELECT /* :v */ 1')
})

test('only real placeholders are replaced', () => {
	const q = "SELECT :a, ':b', \":c\", '12:30' -- :d\nFROM t /* :e */"
	const want = "SELECT @a, ':b', \":c\", '12:30' -- :d\nFROM t /* :e */"
	assert.equal(mapPlaceholders(q, toAt), want)
})

test('replaces a placeholder right after a literal', () => {
	assert.equal(mapPlaceholders("SELECT 'x'||:v", toAt), "SELECT 'x'||@v")
})

test('leaves a :: cast alone', () => {
	assert.equal(mapPlaceholders('SELECT 1::int', toAt), 'SELECT 1::int')
	assert.equal(mapPlaceholders('SELECT :v::int', toAt), 'SELECT @v::int')
})

test('bounds the rewrite at an offset (EXECUTE BLOCK body)', () => {
	assert.equal(mapPlaceholders('SELECT :a, :b', toAt, 11), 'SELECT @a, :b')
	assert.equal(mapPlaceholders(':a :b :c', toAt, 3), '@a :b :c')
})

test('does not treat a digit-leading name as a placeholder (array slice)', () => {
	assert.equal(mapPlaceholders('SELECT arr[1:2]', toAt), 'SELECT arr[1:2]')
})

test('leaves a bracket-quoted identifier alone (MSSQL)', () => {
	assert.equal(mapPlaceholders('SELECT [a:b] AS x', toAt, undefined, SQL_ATOM_MSSQL), 'SELECT [a:b] AS x')
})

test('substitutes placeholders inside brackets for the other dialects', () => {
	// pg arrays/slices keep brackets as code, so :a/:b/:i must still be substituted.
	assert.equal(mapPlaceholders('SELECT ARRAY[:a, :b]', toAt, undefined, SQL_ATOM_PG), 'SELECT ARRAY[@a, @b]')
	assert.equal(mapPlaceholders('SELECT arr[:i]', toAt), 'SELECT arr[@i]')
})

test('leaves a dollar-quoted string alone (pg DO block)', () => {
	assert.equal(mapPlaceholders('DO $$ SELECT :v $$', toAt, undefined, SQL_ATOM_PG), 'DO $$ SELECT :v $$')
	assert.equal(mapPlaceholders('SELECT $body$ :x $body$', toAt, undefined, SQL_ATOM_PG), 'SELECT $body$ :x $body$')
})

test('treats $...$ as code in the non-pg dialects', () => {
	// Dollar-quoting is pg-only: in mssql/oracle/firebird a $ is an ordinary character,
	// so a :name inside $...$ is a real placeholder, not text.
	assert.equal(mapPlaceholders('SELECT $body$ :x $body$', toAt), 'SELECT $body$ @x $body$')
})

test('handles a backslash-escaped quote in a pg E string', () => {
	assert.equal(mapPlaceholders("SELECT E'it\\'s :v'", toAt, undefined, SQL_ATOM_PG), "SELECT E'it\\'s :v'")
	assert.equal(mapPlaceholders("SELECT e'it\\'s :v'", toAt, undefined, SQL_ATOM_PG), "SELECT e'it\\'s :v'")
})

test('treats a backslash as an ordinary character in a string (MSSQL/Oracle/Firebird)', () => {
	const q = "WHERE path = 'C:\\' AND id = :id AND name = 'x'"
	assert.equal(mapPlaceholders(q, toAt), "WHERE path = 'C:\\' AND id = @id AND name = 'x'")
})

test('pg: a backslash is ordinary in a standard string, an escape only in an E string', () => {
	const q = "WHERE path = 'C:\\' AND id = :id AND name = 'x'"
	assert.equal(mapPlaceholders(q, toAt, undefined, SQL_ATOM_PG), "WHERE path = 'C:\\' AND id = @id AND name = 'x'")
})

test('does not treat a backslash-quote as an escape in a quoted identifier', () => {
	assert.equal(mapPlaceholders('SELECT "C:\\" AS p, :v', toAt), 'SELECT "C:\\" AS p, @v')
})

// review2 §4 (group C): Oracle alternative quoting q'[...]' is one string literal, so a
// :name inside it is plain text, not a placeholder - for every delimiter form.
test('leaves an Oracle q-quote string alone', () => {
	assert.equal(mapPlaceholders("SELECT q'[it's a :name]' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT q'[it's a :name]' AS s")
	assert.equal(mapPlaceholders("SELECT q'!it's a :name!' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT q'!it's a :name!' AS s")
	assert.equal(mapPlaceholders("SELECT q'{has :x inside}' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT q'{has :x inside}' AS s")
	assert.equal(mapPlaceholders("SELECT q'(a:b)' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT q'(a:b)' AS s")
	assert.equal(mapPlaceholders("SELECT q'<a:b>' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT q'<a:b>' AS s")
})

test('replaces placeholders outside an Oracle q-quote', () => {
	assert.equal(mapPlaceholders("SELECT q'[a :y]', :v FROM dual", toAt, undefined, SQL_ATOM_ORA), "SELECT q'[a :y]', @v FROM dual")
})

// review3 §2.9: Oracle's q-quote accepts any case (q'/Q') and an optional national
// prefix (nq'/NQ'/Nq'/...). The atom must recognise all of them, or a :name inside such
// a literal is wrongly substituted.
test('leaves uppercase and national Oracle q-quotes alone', () => {
	assert.equal(mapPlaceholders("SELECT Q'[it's :x]' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT Q'[it's :x]' AS s")
	assert.equal(mapPlaceholders("SELECT nq'[it's :x]' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT nq'[it's :x]' AS s")
	assert.equal(mapPlaceholders("SELECT NQ'{has :x}' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT NQ'{has :x}' AS s")
	assert.equal(mapPlaceholders("SELECT Q'!a :b!' AS s", toAt, undefined, SQL_ATOM_ORA), "SELECT Q'!a :b!' AS s")
})

// review3 §2.10: date'...' / time'...' are pg typed literals whose leading token ends in
// 'e' - they must not be parsed as E'' escape strings (backslash would then escape), and
// a placeholder right after them must still bind.
test('does not mistake a pg date/time typed literal for an E-string', () => {
	assert.equal(mapPlaceholders("SELECT date'2020-01-01' AS d, :v", toAt, undefined, SQL_ATOM_PG), "SELECT date'2020-01-01' AS d, @v")
	assert.equal(mapPlaceholders("SELECT time'12:30:00' AS t, :v", toAt, undefined, SQL_ATOM_PG), "SELECT time'12:30:00' AS t, @v")
})

// pg nests block comments; the flat SQL_ATOM block-comment alternative would stop at the
// first */ and expose a :name inside the outer comment. maskPgBlockComments (used by pg's
// buildQuery) must hide the whole nested comment from the placeholder scan, then restore it.
function pgRewrite(sql: string): string {
	const { masked, restore } = maskPgBlockComments(sql)
	return restore(mapPlaceholders(masked, toAt, undefined, SQL_ATOM_PG))
}

test('pg: a nested block comment hides a :name inside it', () => {
	assert.equal(pgRewrite('SELECT 1 /* outer /* inner */ :v */ , :a'), 'SELECT 1 /* outer /* inner */ :v */ , @a')
})

test('pg: a nested comment containing $ is restored verbatim', () => {
	assert.equal(pgRewrite('SELECT 1 /* $1 $tag$ :x */ , :a'), 'SELECT 1 /* $1 $tag$ :x */ , @a')
})

test('pg: a /* inside a string literal is not a comment', () => {
	assert.equal(pgRewrite("SELECT 'a/*b*/c' , :a"), "SELECT 'a/*b*/c' , @a")
	assert.equal(pgRewrite('SELECT "a/*b*/c" , :a'), 'SELECT "a/*b*/c" , @a')
})

test('pg: a placeholder after a comment still binds', () => {
	assert.equal(pgRewrite('SELECT /* c */ :a'), 'SELECT /* c */ @a')
})

// review9 §2.3: maskPgBlockComments must not mistake a /* inside an E'' escape string
// or a $tag$...$tag$ dollar-quote for a block comment - the masked scan would stop at a
// phantom */ and expose a :name inside what is actually one string literal.
test('pg: a /* inside an E string is not a comment', () => {
	assert.equal(pgRewrite("SELECT E'a/*b*/c' , :a"), "SELECT E'a/*b*/c' , @a")
})

test('pg: an escaped quote in an E string does not end it early', () => {
	assert.equal(pgRewrite("SELECT E'it\\'s /* not a comment */' , :a"), "SELECT E'it\\'s /* not a comment */' , @a")
})

test('pg: a /* inside a dollar-quoted string is not a comment', () => {
	assert.equal(pgRewrite('SELECT $$ a/*b*/ :x $$ , :a'), 'SELECT $$ a/*b*/ :x $$ , @a')
	assert.equal(pgRewrite('SELECT $body$ a/*b*/ :x $body$ , :a'), 'SELECT $body$ a/*b*/ :x $body$ , @a')
})
