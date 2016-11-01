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

class Client extends EventEmitter {
  constructor(options) {
    super()
    this.queryQueue = []
    this.rowDescriptions = [] // enum populated when a row description message ('T') is received. one value for each cell in the row. emptied on 'C'
    this.transactionBlock
    this.authentication
    this.processID
    this.secretKey
    this.options = options
    this.connected = false
    this.readyForQuery = false
    this.client = net.connect(options, (err) => {
      if (err) throw err
      this.connected = true
      this.client.write(Buffer.from(fullStartupMessage.replace(/ /g, ''), 'hex'))
      while (this.queryQueue.length) {
        this.query(this.queryQueue.shift())
      }
    })
    this.buffer = Buffer.from([])
    this.client.on('error', (e) => this.emit('error', e))
    this.client.on('close', () => {this.connected = false})
    this.client.on('disconnect', () => {this.connected = false})
    this.client.on('data', readBytes.bind(this))
  }

  query(sql) {
    if (this.connected) {
      this.client.write(createQueryMessage(sql))
    } else {
      this.queryQueue.push(sql)
    }
  }
}

const messageHandlers = {
  C: commandComplete,                          // command complete
  D: parseDataRow,                             // data row
  E: (payload, self) => {console.log('got error'); self.emit('error', payload)},
  I: (payload, self) => {},                    // EmptyQueryResponse or command complete
  K: parseCancellationKey,                     // cancellation key data
  R: updateAuthStatus,                         // authentication request. 0 for success
  S: (payload, self) => {},
  T: parseRowDescription,                      // row description
  Z: readyForQuery,                            // ready for query
}

function readBytes(data) {
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

function parseCancellationKey(messagePayload, self) {
  self.processID = messagePayload.readInt32BE(0)
  self.secretKey = messagePayload.readInt32BE(4)
}

function updateAuthStatus(payload, self) {
  self.authentication = payload.readInt32BE(0) === 0 ?
    'sucess' :
    'failed'
}

function readyForQuery(payload, self) {
  self.readyForQuery = true
  self.transactionBlock = payload.toString() // I | T | E
}

function parseRowDescription(messagePayload, self) {
  // one entry for each value in a row
  const fieldsPerRow = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2) // remove first two bytes
  for (let i = 0; i < fieldsPerRow; ++i) {
    const t = columnName(messagePayload)
    const fieldName = t.shift()
    messagePayload = t.shift()
    self.rowDescriptions.push({
      fieldName: fieldName,
      tableID: messagePayload.readInt32BE(0),
      attributeNumber: messagePayload.readInt16BE(4),
      fieldDataTypeID: messagePayload.readInt32BE(6),
      fieldTypeSize: messagePayload.readInt16BE(10),
      typeModifier: messagePayload.readInt32BE(12),
      formatCode: messagePayload.readInt16BE(16), // 0 = text; 1 = binary;
    })
  }
}

function commandComplete(payload, self) {
  // <Buffer 53 45 4c 45 43 54 20 31 00>
  console.log('command complete', payload)
  self.rowDescriptions = []; // reset row descriptions state
  const completedCommand = payload.toString('utf8')
  // console.log('finished', completedCommand) emit?
}

function columnName(buffer) {
  let str = ''
  let i = 0;
  while (Buffer.from([buffer[i]]).toString('utf8') !== '\00') {
    str = str.concat(Buffer.from([buffer[i]]).toString('utf8'))
    ++i
  }
  return [str, buffer.slice(i + 1)]
}

function parseDataRow(messagePayload, self, testing) {
  const numberOfColumns = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2)                    // remove initial 16bit int indicating # of columns
  let rowValues = [];
  for (let i = 1; i <= numberOfColumns; ++i) {
    const columnLength = messagePayload.readInt32BE()         // -1 if null value, no follow on bytes
    if (columnLength === -1) {
      rowValues.push(null)
      messagePayload = messagePayload.slice(1)
    } else {
      // push value
      rowValues.push(messagePayload.slice(4, 4 + columnLength))
      // remove value, skipping first 4 length bytes
      messagePayload = messagePayload.slice(4 + columnLength)
    }
  }
  const fullRow = rowValues.reduce((row, value, index) => {
    const formatCode = self.rowDescriptions[index].formatCode
    const fieldName = self.rowDescriptions[index].fieldName
    value = formatCode === 0 ? value.toString('utf8') : value
    row[fieldName] = value;
    return row
  }, {})
  if (testing) {
    return fullRow
  } else {
    self.emit('resultsRow', fullRow)
  }
}

function getClient(options) {
  return new Client(options)
}


// runner
const client = getClient({port: 5433})
// client.on('error', (e) => {console.log('got e', e)})
// client.on('resultsRow', (d) => {console.log('got resultsRow:', JSON.stringify(d))})
// client.query(createQueryMessage('select 5'))

module.exports = {
  getClient: getClient,
  client: client
}


test('commandComplete', () => {
  const payload = Buffer.from('53454c454354203100', 'hex')
  const sampleClient = {
    rowDescriptions: ['non-empty']
  }
  commandComplete(Buffer.from('53454c454354203100', 'hex'), sampleClient)
  assert.equal(sampleClient.rowDescriptions.length, 0)
})

// <Buffer>
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
  const expectedSelf = {
    rowDescriptions: [{
      fieldName: '?column?',
      tableID: 0,
      attributeNumber: 0,
      fieldDataTypeID: 23,
      fieldTypeSize: 4,
      typeModifier: -1,
      formatCode: 0
    }]
  }
  const actualSelf = {rowDescriptions: []}
  // mutatates actualSelf
  parseRowDescription(payload, actualSelf)
  assert.deepEqual(expectedSelf, actualSelf)
})


// test('parseDataRow', () => {
//   <Buffer 00 01 00 00 00 01 31>
// emitting { '?column?': '1' }
// emitting [ { fieldName: '?column?',
//     tableID: 0,
//     attributeNumber: 0,
//     fieldDataTypeID: 23,
//     fieldTypeSize: 4,
//     typeModifier: -1,
//     formatCode: 0 } ]
// })