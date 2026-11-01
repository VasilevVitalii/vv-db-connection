import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mapPlaceholders } from '../src/db/placeholders'

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
