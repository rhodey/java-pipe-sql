const split = require('split')
const EventEmitter = require('events')
const changeCase = require('change-case')
const spawn = require('child_process').spawn
const { Readable } = require('stream')

function noop() { }

function sleep(ms) {
  return new Promise((res, rej) => {
    setTimeout(() => res({timeout: true}), ms)
  })
}

function wrap(child) {
  return new Promise((res, rej) => {
    child.once('error', rej)
    if (child.pid) { res(child) }
    rej('no child pid, check that java is available')
  })
}

function driver(conf) {
  const env = { }
  Object.keys(conf).forEach((key) => env[changeCase.snakeCase(key)] = conf[key])
  const stdio = ['pipe', 'pipe', 'pipe']
  const child = spawn(conf.java, ['-jar', conf.jar], { stdio, env })
  return wrap(child).then((child) => {
    const input = new Readable({ read() {} })
    input.pipe(child.stdin)
    child.stdin = input
    child.stderr.setEncoding('utf8')
    child.stdout.setEncoding('utf8')
    child.stderr = child.stderr.pipe(split())
    child.stdout = child.stdout.pipe(split())
    child.stderr.setMaxListeners(0)
    child.stdout.setMaxListeners(0)
    return child
  })
}

function encode(input) {
  if (input === null) { return '' }
  if (typeof input.toISOString === 'function') { return "t" + input.toISOString() } // Date
  if (typeof input.toISO === 'function') { return "t" + input.setZone('UTC').toISO() } // luxon DateTime
  if (typeof input === 'object') { return "j" + Buffer.from(JSON.stringify(input)).toString('base64') }
  if (typeof input !== 'string') { return input }
  if (input.length === 0) { input = `""` }
  return "s" + Buffer.from(input).toString('base64')
}

function encodeArr(input) {
  return input.map(encode).join(',')
}

function isSupportedType(type) {
  return type === 'text' || type === 'jsonb' || type === 'varchar' || type === 'timestamptz' ||
    type === 'bool' || type === 'int4' || type === 'int8' || type === 'numeric' || type === 'bigserial' ||
    type === 'float8' || type === 'void'
}

function readCol(type, value) {
  if (!isSupportedType(type)) { throw new Error(`unsupported sql type ${type}`) }
  if (value === '' || type === 'void') { return null }
  switch (type) {
    case 'text':
    case 'jsonb':
    case 'varchar':
    case 'timestamptz':
      if (value.startsWith('s')) {
        const str = Buffer.from(value.substring(1), 'base64').toString()
        if (str === `""`) { return '' }
        if (type !== 'jsonb') { return str }
        return JSON.parse(str)
      }
      return new Date(Date.parse(value))

    case 'bool':
      return value === 'true'

    case 'int4':
    case 'int8':
    case 'numeric':
    case 'bigserial':
      return parseInt(value)

    case 'float8':
      return parseFloat(value)
  }
}

async function readRows(cols, rows) {
  const out = []
  for (let row of rows) {
    let c = 0
    const obj = { }
    row = row.split(',')
    for (let col of cols) {
      const name = col.split(':')[0]
      const type = col.split(':')[1]
      obj[name] = readCol(type, row[c++])
    }
    out.push(obj)
  }
  return out
}

function readLine(line, conn) {
  if (line.trim().length <= 0) { return null }
  if (line.split(',').length >= 3) { return null }

  const idx = line.indexOf(',')
  if (idx <= 0 && conn === null) { return line }
  if (idx <= 0) { return null }

  let recvConn = line.substring(0, idx)
  if (recvConn === '*' && conn === null) {
    return line.substring(idx + 1)
  } else if (recvConn === '*' || recvConn === 'i') {
    return null
  }

  recvConn = parseInt(recvConn)
  if (isNaN(recvConn) && conn === null) { return line }
  if (recvConn === conn) { return line.substring(idx + 1) }
  return null
}

function awaitData(driver, data) {
  let [listen1, listen2, listen3] = [null, null, null]
  let conn = null
  const idx = data.indexOf(',')
  if (idx > 0) { conn = parseInt(data.substring(0, idx)) }
  if (isNaN(conn)) { conn = null }
  if (!isNaN(conn)) { data = data.substring(idx + 1) }
  const pending = new Promise((res, rej) => {
    listen1 = (line) => {
      line = readLine(line, conn)
      if (line) { rej(new Error(line)) }
    }
    listen2 = (line) => {
      line = readLine(line, conn)
      if (line && line === data) {
        res()
      } else if (line) {
        rej(new Error(`recv '${line}' is not '${data}'`))
      }
    }
    listen3 = () => rej(new Error('stream end'))
    driver.stderr.on('data', listen1)
    driver.stdout.on('data', listen2)
    driver.stderr.once('end', listen3)
    driver.stdout.once('end', listen3)
  })
  pending.catch(noop).finally(() => {
    driver.stderr.off('data', listen1)
    driver.stdout.off('data', listen2)
    driver.stderr.off('end', listen3)
    driver.stdout.off('end', listen3)
  })
  return pending
}

const defaults = {
  java: '/opt/java/openjdk/bin/java',
  jar: '/app/pipe/target/java-pipe-sql-0.0.1.jar',
  jdbcUrl: 'jdbc:postgresql://localhost:5432/postgres',
  user: 'postgres', password: 'postgres',
  bootTimeoutMillis: 1000 * 10, threads: null,
  retry: 0, retryDelayMillis: 600, keepAliveMillis: 0,
  connectionTimeoutMillis: 1000 * 10,
  idleTimeoutMillis: 0,
  query_timeout: 0,
  max: 10,
}

class Pool extends EventEmitter {
  constructor(conf={}) {
    super()
    this.client = 0
    this.clients = []
    this.querySubs = { }
    this.conf = Object.assign({}, defaults, conf)
    for (let key of Object.keys(conf)) {
      if (defaults[key] === undefined) { throw new Error(`config ${key} not implemented`) }
    }
    if (!this.conf.threads) { this.conf.threads = 1 + (4 * this.conf.max) }
    this.conf.threads = Math.max(this.conf.threads, 1 + 10)
    this.booting = this.__boot().then(() => this.emit('booted')).catch((err) => {
      this.bootFailed = true
      this.emitError(err)
      this.end()
    })
  }

  boot() {
    return new Promise((res, rej) => {
      this.once('booted', res)
      this.once('error', rej)
    })
  }

  emitError(err) {
    if (this.ended) { return }
    this.emit('error', err)
  }

  emitInfo(line) {
    if (this.ended) { return }
    this.emit('info', line)
  }

  emitRetry(err) {
    if (this.ended) { return }
    this.emit('retry', err)
  }

  async __boot() {
    this.exitHandle = () => this.end()
    process.once('exit', this.exitHandle)
    this.driver = await driver(this.conf)
    const booting = awaitData(this.driver, 'boot').then(() => {
      this.driver.on('error', (err) => this.emitError(err))
      this.driver.stdin.on('error', (err) => this.emitError(err))
      this.driver.stderr.on('error', (err) => this.emitError(err))
      this.driver.stdout.on('error', (err) => this.emitError(err))
      this.driver.stderr.once('end', () => this.emitError(new Error('stream end')))
      this.driver.stdout.once('end', () => this.emitError(new Error('stream end')))
      this.driver.stderr.on('data', (line) => this.onError(line).catch((err) => this.emitError(err)))
      this.driver.stdout.on('data', (line) => this.onData(line).catch((err) => this.emitError(err)))
    }).then(() => {
      const connecting = []
      for (let i = 0; i < this.conf.max; i++) {
        const client = new Client(this, i)
        connecting.push(client.connect())
      }
      return Promise.all(connecting)
    }).then((clients) => this.clients = clients)
    this.driver.stdin.push(`boot\n`)
    const timeout = sleep(this.conf.bootTimeoutMillis)
    const result = await Promise.race([timeout, booting])
    if (result?.timeout === true) { throw new Error('boot timeout') }
  }

  onClose(conn) {
    this.clients[conn].release()
    const subs = Object.keys(this.querySubs).filter((qid) => {
      const qc = parseInt(qid.split(':')[0])
      return qc === conn
    })
    subs.forEach((qid) => {
      this.querySubs[qid].onError(new Error(`connection closed unexpectedly`))
      delete this.querySubs[qid]
    })
  }

  async onError(line) {
    if (line.trim().length <= 0) { return }
    if (!line.includes(',')) { throw new Error(`driver says error: ${line}`) }
    const idx1 = line.indexOf(',')
    let conn = line.substring(0, idx1)
    let error = line.substring(idx1 + 1)
    if (conn === '*') { throw new Error(error) }
    if (conn === 'i') { return this.emitInfo(error) }
    conn = parseInt(conn)
    if (isNaN(conn)) { throw new Error(`driver says error: ${line}`) }
    const idx2 = line.indexOf(',', idx1 + 1)
    if (idx2 < 0 && error === 'closed') { return this.onClose(conn) }
    if (idx2 < 0) { return }
    let qid = line.substring(idx1 + 1, idx2)
    qid = `${conn}:${qid}`
    if (!this.querySubs[qid]) { return }
    error = line.substring(idx2 + 1)
    this.querySubs[qid].onError(new Error(error))
  }

  async onData(line) {
    if (line.trim().length <= 0) { return }
    if (!line.includes(',')) { throw new Error('data has no connection number') }
    const idx1 = line.indexOf(',')
    let conn = line.substring(0, idx1)
    conn = parseInt(conn)
    if (isNaN(conn)) { throw new Error('data connection number NaN') }
    const idx2 = line.indexOf(',', idx1 + 1)
    if (idx2 < 0) { return }
    let qid = line.substring(idx1 + 1, idx2)
    qid = `${conn}:${qid}`
    if (!this.querySubs[qid]) { return }
    const data = line.substring(idx2 + 1)
    this.querySubs[qid].onData(data)
  }

  sub(qid, onError, onData) {
    if (this.querySubs[qid]) {
      onError(new Error(`duplicate sub ${qid}`))
      return
    }
    this.querySubs[qid] = { onError, onData }
  }

  unsub(qid) {
    delete this.querySubs[qid]
  }

  async nextClient(connect=true, begin=0) {
    if (begin <= 0) { begin = Date.now() }
    const diff = (Date.now() - begin) - 1000
    if (diff >= this.conf.connectionTimeoutMillis) { return null }
    const okQuery = (client) => client.connected && !client.busy
    const okConnect = (client) => client.connected && client.inflight <= 0 && !client.busy
    let cidx = this.client++ % this.clients.length
    let client = this.clients[cidx]
    if (!connect && okQuery(client)) {
      return client
    } else if (connect && okConnect(client)) {
      return client
    }
    for (let i = 0; i < this.conf.max; i++) {
      cidx = this.client++ % this.clients.length
      client = this.clients[cidx]
      if (!connect && okQuery(client)) {
        return client
      } else if (connect && okConnect(client)) {
        return client
      }
    }
    await sleep(10)
    return this.nextClient(connect, begin)
  }

  connect(again=true) {
    let timedout = false
    if (this.bootFailed) { return Promise.reject(new Error('boot failed, check pool for error events')) }
    if (again) { return this.booting.then(() => this.connect(false)) }
    return new Promise(async (res, rej) => {
      if (this.conf.connectionTimeoutMillis > 0) {
        sleep(this.conf.connectionTimeoutMillis)
          .then(() => { timedout = true; rej(new Error('timeout exceeded when trying to connect')) })
      }
      const client = await this.nextClient(true)
      if (timedout || !client) { return }
      client.busy = true
      res(client)
    })
  }

  query(query, args=[], again=true) {
    let timedout = false
    if (this.bootFailed) { return Promise.reject(new Error('boot failed, check pool for error events')) }
    if (again) { return this.booting.then(() => this.query(query, args, false)) }
    return new Promise(async (res, rej) => {
      if (this.conf.query_timeout > 0) {
        sleep(this.conf.query_timeout)
          .then(() => { timedout = true; rej(new Error('Query read timeout')) })
      }
      const client = await this.nextClient(false)
      if (timedout || !client) { return }
      client.query(query, args, true).then(res).catch(rej)
    })
  }

  end() {
    process.off('exit', this.exitHandle)
    this.removeAllListeners('booted')
    this.removeAllListeners('error')
    this.removeAllListeners('info')
    this.removeAllListeners('retry')
    if (this.driver) { this.driver.kill() }
    if (!this.ended) { this.emit('end') }
    this.removeAllListeners('end')
    this.ended = true
    return Promise.resolve()
  }
}

class Client {
  constructor(pool, conn) {
    this.pool = pool
    this.conf = pool.conf
    this.driver = pool.driver
    this.pending = Promise.resolve()
    this.connecting = false
    this.connected = false
    this.inflight = 0
    this.busy = false
    this.conn = conn
    this.qid = 0
  }

  async connect() {
    let timeout = this.conf.connectionTimeoutMillis
    if (timeout > 0) { timeout = sleep(timeout) }
    else { timeout = null }
    const cmd = `${this.conn},connect`
    const ack = awaitData(this.driver, cmd).then(() => this.connected = true)
    this.driver.stdin.push(`${cmd}\n`)
    if (!timeout) { return ack }
    const result = await Promise.race([timeout, ack])
    if (result?.timeout === true) { throw new Error('timeout exceeded when trying to connect') }
    return this
  }

  async reconnect() {
    if (this.connected) { return }
    if (this.connecting) { return }
    if (this.pool.ended) { return }
    this.connecting = this.connect().catch((err) => {
      this.pool.emitInfo(`connection ${this.conn} reconnect failed: ${err.message}`)
      return false
    })
    this.connecting = await this.connecting
    // todo: maybe support reconnect delay
    if (this.connecting === false) { return this.reconnect() }
    this.connecting = false
  }

  query(query, args=[], pool=false) {
    if (!pool) {
      this.pending = this.pending.catch(noop).then(() => this.__query(query, args))
    } else {
      this.pending = this.__query(query, args)
    }
    if (pool || this.conf.query_timeout <= 0) {
      return this.pending
    }
    const timeout = sleep(this.conf.query_timeout)
    return Promise.race([timeout, this.pending]).then((res) => {
      if (res?.timeout === true) { return Promise.reject(new Error('Query read timeout')) }
      return res
    })
  }

  __cmd(cmd) {
    cmd = `${this.conn},${cmd}`
    const ack = awaitData(this.driver, cmd)
    this.driver.stdin.push(`${cmd}\n`)
    return ack
  }

  __query(query, args=[], again=0) {
    ++this.inflight
    const retry = this.conf.retry

    // connecting
    if (this.connecting) {
      const res = this.connecting.then((ok) => {
        if (ok) { return this.__query(query, args, again) }
        if (!retry || again >= retry) { return Promise.reject(new Error('client reconnect failed')) }
        this.pool.emitRetry(new Error('client reconnect failed'))
        return sleep(this.conf.retryDelayMillis)
          .then(() => this.__query(query, args, again + 1))
      })
      res.catch(noop).finally(() => --this.inflight)
      return res
    }

    // cmd
    let cmd = query.toLowerCase()
    cmd = cmd.replaceAll(';', '').trim()
    const cmds = ['begin', 'commit', 'rollback']
    cmd = cmds.indexOf(cmd)
    if (cmd >= 0) {
      const res = this.__cmd(cmds[cmd]).catch((err) => {
        if (err.message.includes('Query read timeout')) { return Promise.reject(err) }
        if (!retry || again >= retry) { return Promise.reject(err) }
        this.pool.emitRetry(err)
        return sleep(this.conf.retryDelayMillis)
          .then(() => this.__query(query, args, again + 1))
      })
      res.catch(noop).finally(() => --this.inflight)
      return res
    }

    // query
    let qid = this.qid++
    qid = `${this.conn}:${qid}`
    return new Promise((res, rej) => {
      let [count1, count2, cols, rows] = [null, null, null, null]
      this.pool.sub(qid, rej, async (data) => {
        if (count2 !== null) {
          rows.push(data)
          if (count2 === rows.length) {
            rows = await readRows(cols, rows).catch(rej)
            res({rowCount: count1, rows})
          }
          return
        }

        const parts = data.split(',')
        if (parts.length < 2) {
          rej(new Error(`driver replied to query incorrectly: ${data}`))
          return
        }

        if (parts.length === 2) {
          count1 = parseInt(parts[0])
          res({rowCount: count1, rows: []})
          return
        }

        rows = []
        count1 = parseInt(parts[0])
        count2 = parseInt(parts[1])
        cols = parts.slice(2)
        if (count2 <= 0) { res({rowCount: count1, rows}) }
      })
      let qidd = qid.split(':')[1]
      let cmd = `${this.conn},query,${qidd},${encode(query)}`
      if (args.length > 0) { cmd = `${cmd},${encodeArr(args)}` }
      this.driver.stdin.push(`${cmd}\n`)
    }).then((data) => {
      this.inflight--
      this.pool.unsub(qid)
      return data
    }).catch((err) => {
      this.inflight--
      this.pool.unsub(qid)
      if (err.message.includes('Query read timeout')) { return Promise.reject(err) }
      if (!retry || again >= retry) { return Promise.reject(err) }
      this.pool.emitRetry(err)
      return sleep(this.conf.retryDelayMillis)
        .then(() => this.__query(query, args, again + 1))
    })
  }

  release() {
    this.connected = this.busy = false
    this.pending = Promise.resolve()
    this.inflight = 0
    this.driver.stdin.push(`${this.conn},close\n`)
    this.reconnect()
  }
}

module.exports = {
  Pool,
}
