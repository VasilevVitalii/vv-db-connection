import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mapPlaceholders, SQL_ATOM_PG } from '../src/db/placeholders'

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
	assert.equal(mapPlaceholders('SELECT [a:b] AS x', toAt), 'SELECT [a:b] AS x')
})

test('leaves a dollar-quoted string alone (pg DO block)', () => {
	assert.equal(mapPlaceholders('DO $$ SELECT :v $$', toAt), 'DO $$ SELECT :v $$')
	assert.equal(mapPlaceholders('SELECT $body$ :x $body$', toAt), 'SELECT $body$ :x $body$')
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
