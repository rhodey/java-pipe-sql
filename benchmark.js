const { Pool } = require('./index.js')
const pg = require('pg')

const zone = 'UTC'

function onError(err) {
  console.error('error', err)
  process.exit(1)
}

async function testOurs() {
  console.log(`\nours`)
  const client = await ours.connect()

  try {

    for (let i = 0; i < 10; i++) {
      const begin = Date.now()
      await client.query('SELECT $1', ['str'])
      const diff = Date.now() - begin
      console.log('diff', diff)
    }

  } finally {
    client.release()
  }
}

async function testTheirs() {
  console.log(`theirs`)
  const client = await theirs.connect()

  try {

    for (let i = 0; i < 10; i++) {
      const begin = Date.now()
      await client.query('SELECT $1', ['str'])
      const diff = Date.now() - begin
      console.log('diff', diff)
    }

  } finally {
    client.release()
  }
}

const ours = new Pool({
  jdbcUrl: 'jdbc:postgresql://localhost:5432/postgres?prepareThreshold=0',
  user: 'postgres', password: 'postgres',
  connectionTimeoutMillis: 1000 * 10,
  query_timeout: 1000 * 30,
  max: 1,
})
ours.on('error', onError)

const theirs = new pg.Pool({
  host: 'localhost', port: 5433,
  user: 'postgres', password: 'postgres',
  connectionTimeoutMillis: 1000 * 10,
  query_timeout: 1000 * 30,
  max: 1,
})
theirs.on('error', onError)

testTheirs().then(testOurs).then(() => {
  process.exit(0)
}).catch(onError)
