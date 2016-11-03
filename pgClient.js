// 'use strict'
const EventEmitter        = require('events');
const net                 = require('net')
const assert              = require('assert')
const pgUtils             = require('./pgUtils')
const parseBuffer         = pgUtils.parseBuffer
const createQueryMessage  = pgUtils.createQueryMessage

const QueryResult = require('./queryResult').QueryResult

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

  end() {
    this.client.end()
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


function createClient(options) {
  return new Client(options)
}

exports.createClient = createClient
exports.parseConfig = parseConfig

