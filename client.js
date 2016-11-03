// 'use strict'
const EventEmitter = require('events');
const net = require('net')
const assert = require('assert')
const parseBuffer  = require('./pg').parseBuffer
const createQueryMessage  = require('./pg').createQueryMessage
const payloadParser  = require('./pg').payloadParser
const test  = require('./pg').test


const initialStartupMessage = "0000 0008 04d2 162f"

const clientConfigMessage = "0000 0041 0003 0000 " +
"7573 6572 0061 6c65 7863 7573 6163 6b00 " +
"6461 7461 6261 7365 0070 6f73 7467 7265 " +
"7300 6170 706c 6963 6174 696f 6e5f 6e61 " +
"6d65 0070 7371 6c00 00"

const fullStartupMessage = initialStartupMessage.concat(clientConfigMessage)


class QueryResult extends EventEmitter {
  constructor(Client, sql) {
    super()
    this.client = Client
    this.connection = Client.client // read/write socket connection
    this.sql = sql                  // sql statement for this query
    this.rowDescriptions = []
    this.buffer = Buffer.from([])
  }

  run() {
    this.client._changeStatus('querying')
    this.connection.on('data', this.handleResultData.bind(this))
    this.connection.write(createQueryMessage(this.sql))
  }

  handleResultData(data) {
    let message = {payload: true} // initilize to true for first iteration
    this.buffer = Buffer.concat([this.buffer, data], this.buffer.length + data.length)
    while (message.payload) {
      const t = parseBuffer(this.buffer)
      message = t.shift()
      this.buffer = t.shift()
      if (message.payload) this.handleMessage(message)
    }
  }

  handleMessage(message) {
    switch (message.header) {
      case 'I': // empty query response
      case 'C':
        this.rowDescriptions = []
        this.emit('commandComplete', commandComplete(message.payload))
        break;
      case 'D':
        const row = parseDataRow(message.payload, this.rowDescriptions)
        this.emit('resultRow', row)
        break;
      case 'E':
        console.log('error', message.payload.toString())
        this.emit('error', message.payload)
        break;
      case 'T':
        this.rowDescriptions = parseRowDescription(message.payload)
        break;
      case 'Z':
        this.emit('done')
        this.client.client.removeAllListeners('data')
        this.client._changeStatus('readyForQuery')
        break;
    }
  }
}


class Client extends EventEmitter {
  constructor(options) {
    super()
    this.status = 'initilized'
    this.options = options
    this.serverConfig = {}
    this.authenticated
    this.processID
    this.secretKey
    this.queryQueue = []
    this._changeStatus('connecting')
    this.buffer = Buffer.from([])
    this.client = net.connect(options, (err) => {
      if (err) throw err
      this._changeStatus('connected')
      this._changeStatus('authenticating')
      this.client.write(Buffer.from(fullStartupMessage.replace(/ /g, ''), 'hex'))
      this.client.on('data', this._handleAuth.bind(this))
    })

    this.client.on('error', (e) => this.emit('error', e))
    this.client.on('close', () => this._changeStatus('disconnected'))
    this.client.on('disconnect', () => this._changeStatus('disconnected'))

    this.on('readyForQuery', () => {
      if (this.queryQueue.length) {
        const query = this.queryQueue.shift();
        query.run()
      }
    })
  }

  query(sql) {
    const query = new QueryResult(this, sql)
    this.queryQueue.push(query)
    return query
  }

  _handleAuth(data) {
    let message = {payload: true} // initilize to true for first iteration
    this.buffer = Buffer.concat([this.buffer, data], this.buffer.length + data.length)
    while (message.payload) {
      const t = parseBuffer(this.buffer)
      message = t.shift()
      this.buffer = t.shift()
      switch (message.header) {
        case 'K':
          this.processID = message.payload.readInt32BE(0)
          this.secretKey = message.payload.readInt32BE(4)
          break;
        case 'N':
          break;
        case 'R':
          this.authenticated = updateAuthStatus(message.payload)
          break;
        case 'S':
          this.serverConfig = Object.assign({}, this.serverConfig, parseConfig(message.payload))
          break;
        case 'Z':
          this.client.removeAllListeners('data') // this is more aggressive than it needs to be
          this._changeStatus('readyForQuery')
          break;
        default:
          message.header ?
            console.log('missed header', message.header, (message.payload || '').toString()) :
            null // do nothing
          break;
      }
    }
  }

  _changeStatus(newStatus) {
    this.emit('statusChange', this.status, newStatus)
    this.status = newStatus
    this.emit(newStatus)
  }
}

function parseConfig(payload) {
  const asArray = payload.toString().split('\00')
  return asArray.reduce((map, _, index) => {
    if (index % 2 !== 0) {
      map[asArray[index - 1]] = asArray[index]
    }
    return map
  }, {})
}


function readBytes(data) {
  // fires on data event, continues to parse messages while full messages are found
  let t;
  let message;
  this.buffer = Buffer.concat([this.buffer, data], this.buffer.length + data.length)
  t = parseBuffer(this.buffer)
  message = t.shift()
  this.buffer = t.shift()
  while(message.payload) {
    messageHandlers[message.header](message.payload, this)
    t = parseBuffer(this.buffer)
    message = t.shift()
    this.buffer = t.shift()
  }
}

function updateAuthStatus(payload) {
  return payload.readInt32BE(0) === 0
}

function parseRowDescription(messagePayload) {
  // mutates client.rowDescription via self (arg)
  // adds an object of attributes for each value in the to be received rows
  const fieldsPerRow = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2) // remove first two bytes
  const descriptions = []
  for (let i = 0; i < fieldsPerRow; ++i) {
    const t = columnName(messagePayload)
    const fieldName = t.shift()
    messagePayload = t.shift()
    descriptions.push({
      fieldName: fieldName,
      tableID: messagePayload.readInt32BE(0),
      attributeNumber: messagePayload.readInt16BE(4),
      fieldDataTypeID: messagePayload.readInt32BE(6),
      fieldTypeSize: messagePayload.readInt16BE(10),
      typeModifier: messagePayload.readInt32BE(12),
      formatCode: messagePayload.readInt16BE(16), // 0 = text; 1 = binary;
    })
  }
  return descriptions
}

function commandComplete(payload) {
  return payload.toString('utf8')
}

function columnName(buffer) {
  // interpret columnName for a row by reading until the null byte
  // return the name and buffer with the used bytes + null byte sliced
  // off
  let str = ''
  let i = 0;
  while (Buffer.from([buffer[i]]).toString('utf8') !== '\00') {
    str = str.concat(Buffer.from([buffer[i]]).toString('utf8'))
    ++i
  }
  return [str, buffer.slice(i + 1)]
}

function parseDataRow(messagePayload, rowDescriptions) {
  const numberOfColumns = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2)                    // remove initial 16bit int indicating # of columns
  const row = {}
  for (let i = 0; i < numberOfColumns; ++i) {
    const columnLength = messagePayload.readInt32BE()
    const fieldName    = rowDescriptions[i].fieldName
    const formatCode   = rowDescriptions[i].formatCode
    const value        = columnLength > -1 ?                 // -1 if null value, no follow on bytes
                          messagePayload.slice(4, 4 + columnLength) :
                          null
    if (value) {
      row[fieldName] = formatCode === 0 ? value.toString('utf8') : value
      messagePayload = messagePayload.slice(4 + columnLength)
    } else {
      // length was -1 so no value
      row[fieldName] = null;
      messagePayload = messagePayload.slice(1)
    }
  }

  return row
}

function createClient(options) {
  return new Client(options)
}


// TESTS
test('commandComplete', () => {
  const payload = Buffer.from('53454c454354203100', 'hex')
  const expected = 'SELECT 1\000'
  const actual = commandComplete(Buffer.from('53454c454354203100', 'hex'))
  assert.equal(actual, expected)
})

test('parse config', () => {
  const payload = Buffer.from('54696d655a6f6e650055532f5061636966696300', 'hex')
  const expected = {TimeZone: 'US/Pacific'}
  const actual = parseConfig(payload)
  assert.deepEqual(actual, expected)
})

test('columnName parsing', () => {
  const payload = Buffer.from('3f636f6c756d6e3f00000000000000000000170004ffffffff0000', 'hex')
  const actual = columnName(payload)
  const expectedRemaingBuff = Buffer.from('000000000000000000170004ffffffff0000', 'hex')
  const expectedString = '?column?'
  assert.equal(actual[0], expectedString)
  assert.equal(actual[1].equals(expectedRemaingBuff), true)
})

test('parseRowDescription', () => {
  const payload = Buffer.from('00013f636f6c756d6e3f00000000000000000000170004ffffffff0000', 'hex')
  const expected = [{
    fieldName: '?column?',
    tableID: 0,
    attributeNumber: 0,
    fieldDataTypeID: 23,
    fieldTypeSize: 4,
    typeModifier: -1,
    formatCode: 0
  }]
  const actual = parseRowDescription(payload)
  assert.deepEqual(actual, expected)
})


test('parseDataRow', () => {
  const payload = Buffer.from('00010000000131', 'hex')
  const rowDescriptions = [{
    fieldName: '?column?',
    tableID: 0,
    attributeNumber: 0,
    fieldDataTypeID: 23,
    fieldTypeSize: 4,
    typeModifier: -1,
    formatCode: 0
  }]
  const expected = {'?column?': '1'}
  const actual = parseDataRow(payload, rowDescriptions)
  assert.deepEqual(actual, expected)
})

// some setup exported for use in the CLI, uncomment for cli use
const client = createClient({port: 5433})
client.on('statusChange', (oldStatus, newStatus) => {
  console.log('status changed:', oldStatus, '->', newStatus)
})
const rows = client.query('select 1')
const rows2 = client.query('select 9999')
const rows3 = client.query('select 555')
const rows4 = client.query('select 8888')

module.exports = {
  createClient: createClient,
  client: client
}

