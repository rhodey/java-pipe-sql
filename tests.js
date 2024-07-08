const test = require('tape')
const { DateTime } = require('luxon')
const { Pool } = require('./index.js')

const timeout = 10 * 1000

function sleep(ms) {
  return new Promise((res, rej) => {
    setTimeout(res, ms)
  })
}

function awaitBoot(pool) {
  return pool.boot()
}

function init(t, max=1, retry=0, ka=0, threads=null) {
  const pool = new Pool({
    java: process.env.java, jar: process.env.jar,
    jdbcUrl: process.env.jdbc_url,
    user: process.env.user, password: process.env.password,
    bootTimeoutMillis: parseInt(process.env.boot_timeout_millis),
    connectionTimeoutMillis: parseInt(process.env.connection_timeout_millis),
    idleTimeoutMillis: parseInt(process.env.idle_timeout_millis),
    query_timeout: parseInt(process.env.query_timeout),
    keepAliveMillis: ka, retry, threads, max,
  })
  t.teardown(() => pool.end())
  pool.on('error', (err) => {
    console.log('pool error', err)
    t.fail(err.message)
  })
  return pool
}

async function createTestsTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tests (
      id BIGSERIAL,
      key TEXT PRIMARY KEY NOT NULL,
      str TEXT,
      num1 INTEGER,
      num2 BIGINT,
      num3 DOUBLE PRECISION,
      time TIMESTAMPTZ,
      bbool BOOLEAN
    )`
  )
  await pool.query(`DELETE FROM tests`)
  await pool.query(`ALTER SEQUENCE tests_id_seq RESTART WITH 1`)
}

async function createJsonTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS json_tests (
      key TEXT PRIMARY KEY NOT NULL,
      json JSONB
    )`
  )
  await pool.query(`DELETE FROM json_tests`)
}

test('testBootFail', function (t) {
  t.plan(3)
  t.timeoutAfter(timeout)
  const pool = new Pool({ jdbcUrl: null })
  t.teardown(() => pool.end())

  pool.on('error', (err) => t.ok(err, 'error ok'))
  pool.query('SELECT 1').catch((err) => {
    t.ok(err, 'error ok')
    t.ok(err.message.includes('boot failed'), 'error ok')
  })
})

test('testBootedEvent', function (t) {
  t.plan(1)
  t.timeoutAfter(timeout)
  const pool = init(t)
  pool.on('booted', () => t.ok(1, 'boot event emitted'))
})

test('testBootedPromise', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  t.ok(1, 'boot promise resolved')
  pool.end()
  t.ok(1, 'pool.end ok')
})

test('testConnectTimeout', async function (t) {
  t.plan(2)
  t.timeoutAfter(20 * 1000)
  const pool = init(t)
  await awaitBoot(pool)
  const client1 = await pool.connect()

  try {
    const client2 = await pool.connect()
  } catch (err) {
    t.ok(err, 'threw error')
    t.equal(err.message, 'timeout exceeded when trying to connect', 'error correct')
  }
})

test('testQueryTimeout', async function (t) {
  t.plan(2)
  t.timeoutAfter(20 * 1000)
  const pool = init(t)
  await awaitBoot(pool)

  try {
    await pool.query('SELECT pg_sleep(12)')
  } catch (err) {
    t.ok(err, 'threw error')
    t.equal(err.message, 'Query read timeout', 'error correct')
  }
})

test('testArgsNotOk', async function (t) {
  t.plan(4)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  try {
    await pool.query('SELECT $1, $2', [1])
  } catch (err) {
    t.ok(err, 'threw error')
    t.ok(err.message.includes('args do not match'), 'error correct')
  }

  try {
    await pool.query('SELECT $1, $2', [1, 2, 3])
  } catch (err) {
    t.ok(err, 'threw error')
    t.ok(err.message.includes('args do not match'), 'error correct')
  }
})

test('testSelect123', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT 123`)
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `1`, `column named 1`)
  t.equal(data[`1`], 123, `123 returned`)
})

test('testSelect123Multi', async function (t) {
  t.plan(6)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT 1, 2, 3`)
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 3, `row has one column`)
  t.equal(data[`1`], 1, `1 returned`)
  t.equal(data[`2`], 2, `2 returned`)
  t.equal(data[`3`], 3, `3 returned`)
})

test('testSelectString', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT 'str'`)
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `1`, `column named 1`)
  t.equal(data[`1`], 'str', `str returned`)
})

test('testSelectArg123', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT $1::int AS num`, [123])
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `num`, `column named num`)
  t.equal(data.num, 123, `arg 123 returned`)
})

test('testSelectArgString', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT $1 AS str`, ['str'])
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `str`, `column named str`)
  t.equal(data.str, 'str', `arg str returned`)
})

test('testSelectArgStringAsText', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT $1::text AS str`, ['str'])
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `str`, `column named str`)
  t.equal(data.str, 'str', `arg str returned`)
})

test('testSelectManyArgs', async function (t) {
  t.plan(14)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  const args = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
  let data = await pool.query(`SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11`, args)
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, args.length, `row has ${args.length} columns`)

  for (let i = 1; i <= args.length; i++) {
    const key = i+""
    const val = args[i-1]+""
    t.equal(data[key], val, `arg ${i} returned`)
  }
})

test('testSelectArgsOutOfOrder', async function (t) {
  t.plan(7)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT $2, $1, $5, $3, $4`, [1, 2, 3, 4, 5])
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data[`1`], '2', `arg $2 returned`)
  t.equal(data[`2`], '1', `arg $1 returned`)
  t.equal(data[`3`], '5', `arg $5 returned`)
  t.equal(data[`4`], '3', `arg $3 returned`)
  t.equal(data[`5`], '4', `arg $4 returned`)
})

test('testSelectArgsDuplicates', async function (t) {
  t.plan(7)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  let data = await pool.query(`SELECT $2, $1, $1, $3, $2`, [1, 2, 3])
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data[`1`], '2', `arg $2 returned`)
  t.equal(data[`2`], '1', `arg $1 returned`)
  t.equal(data[`3`], '1', `arg $1 returned`)
  t.equal(data[`4`], '3', `arg $3 returned`)
  t.equal(data[`5`], '2', `arg $2 returned`)
})

test('testEmptySelect', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `no updates`)
  t.equal(data.rows.length, 0, `rows.length === 0`)
})

test('testInsertAndSelect', async function (t) {
  t.plan(8)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', 123])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data.id, 1, `row.id ok`)
  t.equal(data.key, 'keyy', `row.key ok`)
  t.equal(data.str, 'strr', `row.str ok`)
  t.equal(data.num1, 123, `row.num1 ok`)
})

test('testInsertTwoAndSelect', async function (t) {
  t.plan(14)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', 123])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyyy', 'strrr', 456])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 2, `rows.length === 2`)

  let row = data.rows[0]
  t.equal(row.id, 1, `row.id ok`)
  t.equal(row.key, 'keyy', `row.key ok`)
  t.equal(row.str, 'strr', `row.str ok`)
  t.equal(row.num1, 123, `row.num1 ok`)

  row = data.rows[1]
  t.equal(row.id, 2, `row.id ok`)
  t.equal(row.key, 'keyyy', `row.key ok`)
  t.equal(row.str, 'strrr', `row.str ok`)
  t.equal(row.num1, 456, `row.num1 ok`)
})

test('testInsertWithNullFirst', async function (t) {
  t.plan(8)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (str, key, num1) VALUES ($1, $2, $3)`, [null, 'keyy', 123])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data.id, 1, `row.id ok`)
  t.equal(data.key, 'keyy', `row.key ok`)
  t.equal(data.str, null, `row.str ok`)
  t.equal(data.num1, 123, `row.num1 ok`)
})

test('testInsertWithNullMiddle', async function (t) {
  t.plan(8)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', null, 123])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data.id, 1, `row.id ok`)
  t.equal(data.key, 'keyy', `row.key ok`)
  t.equal(data.str, null, `row.str ok`)
  t.equal(data.num1, 123, `row.num1 ok`)
})

test('testInsertWithNullLast', async function (t) {
  t.plan(8)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', null])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data.id, 1, `row.id ok`)
  t.equal(data.key, 'keyy', `row.key ok`)
  t.equal(data.str, 'strr', `row.str ok`)
  t.equal(data.num1, null, `row.num1 ok`)
})

test('testInsertWithNullNotOk', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  try {
    await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, [null, 'strr', 123])
    t.fail(`should have thrown error`)
  } catch (err) {
    t.ok(err, 'error thrown')
    const specific = err.message.includes('violates not-null constraint')
    t.ok(specific, 'specific error thrown')
  }
})

test('testInsertWithReturning', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3) RETURNING num1`, ['keyy', 'strr', 123])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `num1`, `column named num1`)
  t.equal(data.num1, 123, `123 returned`)
})

test('testAllTypes', async function (t) {
  t.plan(14)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let date = Date.now()
  date = date - (date % 100)
  date = new Date(date)

  let data = await pool.query(`
    INSERT INTO tests (key, num1, num2, num3, time, bbool)
      VALUES ($1, $2, $3, $4, $5, $6)`, ['keyy', -3, Number.MAX_SAFE_INTEGER, 4.2, date, true]
  )
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  const cols = Object.keys(data)
  t.equal(cols.length, 8, `row has 8 columns`)

  t.equal(data.id, 1, `id returned`)
  t.equal(data.key, 'keyy', `key returned`)
  t.equal(data.num1, -3, `num1 returned`)
  t.equal(data.num2, Number.MAX_SAFE_INTEGER, `num2 returned`)
  t.equal(data.num3, 4.2, `num3 returned`)
  t.equal(data.time.getTime(), date.getTime(), `time returned`)
  t.equal(data.bbool, true, `bbool returned`)

  data = await pool.query(`UPDATE tests SET bbool = $1`, [false])
  t.equal(data.rowCount, 1, `1 update`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]
  t.equal(data.bbool, false, `bbool false returned`)
})

test('testAllNullTypes', async function (t) {
  t.plan(12)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`
    INSERT INTO tests (key, str, num1, num2, num3, time, bbool)
      VALUES ($1, $2, $3, $4, $5, $6, $7)`, ['keyy', null, null, null, null, null, null]
  )
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  const cols = Object.keys(data)
  t.equal(cols.length, 8, `row has 8 columns`)

  t.equal(data.id, 1, `id returned`)
  t.equal(data.key, 'keyy', `key returned`)
  t.equal(data.str, null, `null returned`)
  t.equal(data.num1, null, `null returned`)
  t.equal(data.num2, null, `null returned`)
  t.equal(data.num3, null, `null returned`)
  t.equal(data.time, null, `null returned`)
  t.equal(data.bbool, null, `null returned`)
})

test('testEmptyString', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str) VALUES ($1, $2)`, ['keyy', ''])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  t.equal(data.str, '', `empty string returned`)
})

test('testReturningEmptyString', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  let data = await pool.query(`INSERT INTO tests (key, str) VALUES ($1, $2) RETURNING str`, ['keyy', ''])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  const cols = Object.keys(data)
  t.equal(cols.length, 1, `row has one column`)
  t.equal(cols[0], `str`, `column named str`)
  t.equal(data.str, '', `empty string returned`)
})

test('testLongFloat', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const num = 123.4567890123
  let data = await pool.query(`INSERT INTO tests (key, num3) VALUES ($1, $2)`, ['keyy', num])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  t.equal(data.num3, num, `long float returned`)
})

test('testLongString', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const str = Buffer.alloc(1024 * 1024).fill("a").toString()
  let data = await pool.query(`INSERT INTO tests (key, str) VALUES ($1, $2)`, ['keyy', str])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  t.equal(data.str, str, `long string returned`)
})

test('testTenLongString', async function (t) {
  t.plan(12)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const pending = []
  const str = Buffer.alloc(1024 * 1024).fill("a").toString()
  for (let i = 0; i < 10; i++) {
    pending.push(pool.query(`INSERT INTO tests (key, str) VALUES ($1, $2)`, [`key${i+1}`, str]))
  }

  await Promise.all(pending)
  const data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 10, `rows.length === 10`)

  for (let row of data.rows) {
    t.equal(row.str, str, `long string returned`)
  }
})

test('testJson', async function (t) {
  t.plan(8)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createJsonTable(pool)

  const json = {a: 1, b: 2}
  let data = await pool.query(`INSERT INTO json_tests (key, json) VALUES ($1, $2)`, ['keyy', json])
  t.equal(data.rowCount, 1, `1 update`)

  data = await pool.query(`SELECT * FROM json_tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  let cols = Object.keys(data)
  t.equal(cols.length, 2, `row has 2 columns`)
  t.equal(data.key, 'keyy', `key returned`)
  t.ok(data.json, `json returned`)

  cols = Object.keys(data.json)
  t.equal(cols.length, 2, `json has 2 keys`)
  t.equal(data.json.a, 1, `json key a = 1`)
  t.equal(data.json.b, 2, `json key b = 1`)
})

test('testJsonEmpty', async function (t) {
  t.plan(6)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createJsonTable(pool)

  const json = { }
  let data = await pool.query(`INSERT INTO json_tests (key, json) VALUES ($1, $2)`, ['keyy', json])
  t.equal(data.rowCount, 1, `1 update`)

  data = await pool.query(`SELECT * FROM json_tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  let cols = Object.keys(data)
  t.equal(cols.length, 2, `row has 2 columns`)
  t.equal(data.key, 'keyy', `key returned`)
  t.ok(data.json, `json returned`)

  cols = Object.keys(data.json)
  t.equal(cols.length, 0, `json has 0 keys`)
})

test('testJsonNull', async function (t) {
  t.plan(5)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createJsonTable(pool)

  let data = await pool.query(`INSERT INTO json_tests (key, json) VALUES ($1, $2)`, ['keyy', null])
  t.equal(data.rowCount, 1, `1 update`)

  data = await pool.query(`SELECT * FROM json_tests`)
  t.equal(data.rows.length, 1, `rows.length === 1`)

  data = data.rows[0]
  let cols = Object.keys(data)
  t.equal(cols.length, 2, `row has 2 columns`)
  t.equal(data.key, 'keyy', `key returned`)
  t.equal(data.json, null, `null returned`)
})

test('testNoRetryEvents', async function (t) {
  t.plan(1)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  pool.on('retry', (err) => {
    t.fail('expected no retry events')
  })

  try {
    await pool.query(`SELECT 123`, ['bad'])
    t.fail('expected error thrown')
  } catch (err) {
    t.ok(err, 'error thrown')
    await sleep(2000)
  }
})

test('testRetry', async function (t) {
  t.plan(4)
  t.timeoutAfter(timeout)
  const pool = init(t, 1, 2)
  await awaitBoot(pool)

  let retry = 0
  pool.on('retry', (err) => {
    t.ok((typeof err?.message === 'string'), 'retry has error message')
    retry++
  })

  try {
    await pool.query(`SELECT 123`, ['bad'])
  } catch (err) {
    t.ok(err, 'error thrown')
    t.equal(retry, 2, 'retry events came first')
    await sleep(2000)
  }
})

test('testInfo', async function (t) {
  t.plan(3)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  pool.on('info', (line) => {
    t.ok((typeof line === 'string'), 'info has stack trace')
    t.ok(line.includes('query args do not match'), 'info has correct stack trace')
  })

  try {
    await pool.query(`SELECT 123`, ['bad'])
  } catch (err) {
    t.ok(err, 'error thrown')
  }
})

test('testTxnCommit', async function (t) {
  t.plan(14)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const client = await pool.connect()

  try {

    await client.query('BEGIN')
    let data = await client.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', 123])
    t.equal(data.rowCount, 1, `1 update`)
    t.equal(data.rows.length, 0, `rows.length === 0`)

    data = await client.query(`SELECT * FROM tests`)
    t.equal(data.rowCount, 0, `0 updates`)
    t.equal(data.rows.length, 1, `rows.length === 1`)
    data = data.rows[0]

    t.equal(data.id, 1, `row.id ok`)
    t.equal(data.key, 'keyy', `row.key ok`)
    t.equal(data.str, 'strr', `row.str ok`)
    t.equal(data.num1, 123, `row.num1 ok`)
    await client.query('COMMIT')

  } catch (err) {
    t.fail(err.message)
  } finally {
    client.release()
  }

  let data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]

  t.equal(data.id, 1, `row.id ok`)
  t.equal(data.key, 'keyy', `row.key ok`)
  t.equal(data.str, 'strr', `row.str ok`)
  t.equal(data.num1, 123, `row.num1 ok`)
})

test('testTxnNoCommit', async function (t) {
  t.plan(4)
  t.timeoutAfter(timeout)
  const pool = init(t, 2)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const client = await pool.connect()

  try {

    await client.query('BEGIN')
    let data = await client.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', 123])
    t.equal(data.rowCount, 1, `1 update`)
    t.equal(data.rows.length, 0, `rows.length === 0`)

  } catch (err) {
    t.fail(err.message)
  } finally {
    client.release()
  }

  const data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 0, `rows.length === 0`)
})

test('testTxnRollback', async function (t) {
  t.plan(4)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const client = await pool.connect()

  try {

    await client.query('BEGIN')
    let data = await client.query(`INSERT INTO tests (key, str, num1) VALUES ($1, $2, $3)`, ['keyy', 'strr', 123])
    t.equal(data.rowCount, 1, `1 update`)
    t.equal(data.rows.length, 0, `rows.length === 0`)
    await client.query('ROLLBACK')

  } catch (err) {
    t.fail(err.message)
  } finally {
    client.release()
  }

  const data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 0, `rows.length === 0`)
})

test('testParallelMax1', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  const q1 = pool.query(`SELECT pg_sleep(3)`).then(() => 3)
  const q2 = pool.query(`SELECT pg_sleep(1)`).then(() => 1)
  const [r1, r2] = await Promise.all([q1, q2])

  t.equal(r1, 3, `query1 completed`)
  t.equal(r2, 1, `query2 completed`)
})

test('testParallelMax2', async function (t) {
  t.plan(4)
  t.timeoutAfter(timeout)
  const pool = init(t, 2)
  await awaitBoot(pool)

  const begin = Date.now()
  const q1 = pool.query(`SELECT pg_sleep(5)`)
  const q2 = pool.query(`SELECT pg_sleep(3)`)

  await q2
  let diff = Date.now() - begin
  console.log('diff = ', diff)
  t.ok(diff < 4000, 'query2 returned in less than 4 seconds')
  t.ok(diff > 2500, 'query2 returned in more than 2.5 seconds')

  await q1
  diff = Date.now() - begin
  console.log('diff = ', diff)
  t.ok(diff < 6000, 'query1 returned in less than 6 seconds')
  t.ok(diff > 4500, 'query2 returned in more than 4.5 seconds')
})

test('testTxnMulti', async function (t) {
  t.plan(12)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const date1 = DateTime.utc()
  let data = await pool.query(`INSERT INTO tests (key, time) VALUES ($1, $2)`, ['keyy', date1])
  t.equal(data.rowCount, 1, `1 update`)
  t.equal(data.rows.length, 0, `rows.length === 0`)

  data = await pool.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]
  t.equal(data.time.getTime(), date1.ts, `date returned`)

  const client = await pool.connect()
  await client.query('BEGIN')
  await client.query('LOCK TABLE tests IN ACCESS EXCLUSIVE MODE')

  const date2 = DateTime.utc()
  data = await client.query(`SELECT key, time FROM tests WHERE time IS NULL OR time <= $1 ORDER BY time ASC LIMIT $2;`, [date2, 1])
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]
  t.equal(data.time.getTime(), date1.ts, `date returned`)

  const pending = []
  pending.push(client.query(`UPDATE tests SET (time, key) = ($1, $2) WHERE 1=1`, [date2, 'keyyy']))
  pending.push(client.query('COMMIT'))
  await Promise.all(pending)

  data = await client.query(`SELECT * FROM tests`)
  t.equal(data.rowCount, 0, `0 updates`)
  t.equal(data.rows.length, 1, `rows.length === 1`)
  data = data.rows[0]
  t.equal(data.key, 'keyyy', `keyyy returned`)
  t.equal(data.time.getTime(), date2.ts, `date returned`)
})

test('testParallelOrdered', async function (t) {
  t.plan(11)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)
  await createTestsTable(pool)

  const client = await pool.connect()

  const pending = []
  for (let i = 0; i < 10; i++) {
    pending.push(client.query(`INSERT INTO tests (key) VALUES ($1) RETURNING id`, [`key${i+1}`]))
  }

  const data = await Promise.all(pending)
  t.equal(data.length, 10, `10 inserts`)

  const rows = data.map((d) => d.rows[0])
  for (let i = 0; i < 10; i++) {
    t.equal(rows[i].id, i+1, `id ${i+1} returned`)
  }
})

test('testClientBusy', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  const client = await pool.connect()
  t.ok(client.busy, 'client.busy === true')
  client.release()
  t.ok(client.busy === false, 'client.busy === false')
})

test('testClientInflight', async function (t) {
  t.plan(2)
  t.timeoutAfter(timeout)
  const pool = init(t)
  await awaitBoot(pool)

  const client = await pool.connect()
  const query = client.query('SELECT pg_sleep(2)')
  await sleep(1000)
  t.equal(client.inflight, 1, 'client.inflight === 1')
  await query
  t.equal(client.inflight, 0, 'client.inflight === 0')
})

test('testConnectAfterInflight', async function (t) {
  t.plan(1)
  t.timeoutAfter(timeout)
  const pool = init(t, 2)
  await awaitBoot(pool)

  const q1 = pool.query('SELECT pg_sleep(2)')
  const q2 = pool.query('SELECT pg_sleep(2)')
  await sleep(1000)

  const client = await pool.connect()
  let data = await client.query('SELECT 123 AS num')
  data = data.rows[0]
  t.equal(data.num, 123, `123 returned`)

  await q1
  await q2
})
