// 'use strict'
const EventEmitter = require('events');
const net = require('net')
const parseBuffer  = require('./pg').parseBuffer
const createQueryMessage  = require('./pg').createQueryMessage
const payloadParser  = require('./pg').payloadParser

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
      this.client.write(sql)
    } else {
      this.queryQueue.push(sql)
    }
  }
}

function readBytes(data) {
  let message;
  let t;
  this.buffer = Buffer.concat([this.buffer, data], this.buffer.length + data.length)
  t = parseBuffer(this.buffer)
  message = t.shift()
  this.buffer = t.shift()
  while(message.payload) {
    switch (message.header) {
      case 'C':                                   // command complete
        break;
      case 'D':                                   // data row
        dataRow(message.payload, this)
      case 'I':                                   // EmptyQueryResponse or command complete
        break;
      case 'K':                                   // cancellation key data
        parseCancellationKey(message.payload, this)
        break;
      case 'R':                                   // authentication request. 0 for success
        this.authentication = 'success'
        break;
      case 'T':                                   // row description
        rowDescription(message.payload, this)
        break;
      case 'Z':                                   // ready for query
        this.readyForQuery = true
        this.transactionBlock = message.payload.toString() // I | T | E
        break;
      default:
        break;
    }
    t = parseBuffer(this.buffer)
    message = t.shift()
    this.buffer = t.shift()
  }
}

function parseCancellationKey(messagePayload, self) {
  self.processID = messagePayload.readInt32BE(0)
  self.secretKey = messagePayload.readInt32BE(4)
}

function rowDescription(messagePayload, self) {
  // how do we know low long the column name is? nullBytes?
  const fieldsPerRow = messagePayload.readInt16BE(0)
}

function dataRow(messagePayload, self) {
  const numberOfColumns = messagePayload.readInt16BE(0)
  messagePayload = messagePayload.slice(2) // remove initial 16bit int indicating # of columns
  let values = [];
  for (let i = 1; i <= numberOfColumns; ++i) {
    // length of the column value, in bytes
    // -1 if null value, no follow on bytes
    const length = messagePayload.readInt32BE()
    if (length === -1) {
      vaules.push(null)
      messagePayload = messagePayload.slice(1)
    } else {
      // push the column value on the values array, format is specified elsewhere
      values.push(messagePayload.slice(4, 4 + length))
      // remove bytes and loop
      messagePayload = messagePayload.slice(4 + length)
    }
  }
}

function getClient(options) {
  return new Client(options)
}

module.exports = {
  getClient: getClient
}

// runner
const client = getClient({port: 5433})
client.on('error', (e) => {console.log('got e', e)})
client.on('resultsRow', (d) => {console.log('got resultsRow:', JSON.stringify(d))})
client.query(createQueryMessage('select 5'))
