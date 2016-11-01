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

const messageHandlers = {
  C: commandComplete,                          // command complete
  D: parseDataRow,                             // data row
  E: (payload, self) => {console.log('got error'); self.emit('error', payload)}, // todo!
  I: (payload, self) => {},                    // EmptyQueryResponse or command complete
  K: parseCancellationKey,                     // cancellation key data
  R: updateAuthStatus,                         // authentication request. 0 for success
  S: (payload, self) => {},                    // status messages, save these on client
  T: parseRowDescription,                      // row description
  Z: readyForQuery,                            // ready for query
}

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

function parseCancellationKey(messagePayload, self) {
  // stores processID and secretKey which are used when sending a cancellation
  // request to the server
  self.processID = messagePayload.readInt32BE(0)
  self.secretKey = messagePayload.readInt32BE(4)
}

function updateAuthStatus(payload, self) {
  // updates Client authentication status to success or failed
  self.authentication = payload.readInt32BE(0) === 0 ?
    'sucess' :
    'failed'
}

function readyForQuery(payload, self) {
  // updates client ready status and transaction block status to...
  // I = idle
  // T = server is in transaction block
  // E = error occured in the transaction
  self.readyForQuery = true
  self.transactionBlock = payload.toString()
}

function parseRowDescription(messagePayload, self) {
  // mutates client.rowDescription via self (arg)
  // adds an object of attributes for each value in the to be received rows
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
  // command complete, reset rowDescription to empty
  self.rowDescriptions = []; // reset row descriptions state
  const completedCommand = payload.toString('utf8')
  // console.log('finished', completedCommand) emit?
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

function parseDataRow(messagePayload, self, testing) {
  const numberOfColumns = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2)                    // remove initial 16bit int indicating # of columns
  const row = {}
  for (let i = 0; i < numberOfColumns; ++i) {
    const columnLength = messagePayload.readInt32BE()
    const fieldName    = self.rowDescriptions[i].fieldName
    const formatCode   = self.rowDescriptions[i].formatCode
    const value        = columnLength > -1 ?                // -1 if null value, no follow on bytes
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

  if (testing) {
    return row
  } else {
    self.emit('resultsRow', row)
  }
}

function getClient(options) {
  return new Client(options)
}


// some setup exported for use in the CLI, uncomment for cli use
// const client = getClient({port: 5433})
// client.on('error', (e) => {console.log('got e', e)})
// client.on('resultsRow', (d) => {console.log('got resultsRow:', JSON.stringify(d))})

// module.exports = {
//   getClient: getClient,
//   client: client
// }

// TESTS
test('commandComplete', () => {
  const payload = Buffer.from('53454c454354203100', 'hex')
  const sampleClient = {
    rowDescriptions: ['non-empty']
  }
  commandComplete(Buffer.from('53454c454354203100', 'hex'), sampleClient)
  assert.equal(sampleClient.rowDescriptions.length, 0)
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
  // mutates `actualSelf`
  parseRowDescription(payload, actualSelf)
  assert.deepEqual(expectedSelf, actualSelf)
})


test('parseDataRow', () => {
  const payload = Buffer.from('00010000000131', 'hex')
  const self = { // sample state of client going into parseDataRow function
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
  const expected = {'?column?': '1'}
  const actual = parseDataRow(payload, self, 'testing')
  assert.deepEqual(actual, expected)
})