// 'use strict'
const EventEmitter        = require('events');
const net                 = require('net')
const assert              = require('assert')
const pgUtils             = require('./pgUtils')
const parseBuffer         = pgUtils.parseBuffer
const createQueryMessage  = pgUtils.createQueryMessage

const QueryResult = require('./queryResult').QueryResult

class Client extends EventEmitter {
  constructor(options, config) {
    super()
    this.status = 'initilized'
    this.clientConfig = config
    this.options = options
    this.serverConfig = {}
    this.authenticated
    this.processID
    this.secretKey
    this.queryQueue = []
    this.buffer = Buffer.from([])
    this.connect(options)

    this.on('readyForQuery', () => {
      if (this.queryQueue.length) {
        const query = this.queryQueue.shift();
        query.run()
      }
    })
  }

  connect(options) {
    this._changeStatus('connecting')
    options = options || this.options
    this.client = net.connect(options, (err) => {
      if (err) throw err
      this._changeStatus('connected')
      this._changeStatus('authenticating')
      this.client.write(Buffer.from("0000000804d2162f", 'hex')) // TBD myster startup message
      this.client.write(encodeConfig(this.clientConfig))
      this.client.on('data', this._handleAuth.bind(this))
      this.client.on('error', (e) => this.emit('error', e))
      this.client.on('close', () => this._changeStatus('disconnected'))
      this.client.on('disconnect', () => this._changeStatus('disconnected'))
    })
  }

  _changeStatus(newStatus) {
    this.emit('statusChange', this.status, newStatus)
    this.status = newStatus
    this.emit(newStatus)
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

  end() {
    this.client.end()
  }

  query(sql) {
    if (this.status === 'disconnected') throw 'Client is no longer connected'
    const query = new QueryResult(this, sql)
    this.queryQueue.push(query)
    return query
  }
}

function encodeConfig(config) {
  const numberOfKeys = Object.keys(config).length
  const asString = Object.keys(config).reduce((string, key) => {
    return string.concat(key, '\00', config[key], '\00')
  }, '')
  const buff = Buffer.alloc(4 + 2 + 2 + asString.length + 1) // 32bit int, 16 bit int, 2 mystery bytes, trailing null
  buff.writeInt32BE(buff.length, 0) // subtract trailing null byte
  buff.writeInt16BE(numberOfKeys, 4)
  buff.write(asString.concat('\00'), 6 + 2) // 2 myster bytes that 00
  return buff
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


function createClient(options, config) {
  return new Client(options, config)
}

exports.createClient = createClient
exports.parseConfig = parseConfig
exports.encodeConfig = encodeConfig

