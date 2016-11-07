const EventEmitter        = require('events');
const net                 = require('net')
const assert              = require('assert')
const pgUtils             = require('./pgUtils')
const parseBuffer         = pgUtils.parseBuffer
const createQueryMessage  = pgUtils.createQueryMessage

const QueryResult = require('./queryResult').QueryResult

class Client extends EventEmitter {
  constructor(config) {
    super()
    this.status = 'initialized'
    this.clientConfig = config
    this.serverConfig = {}
    this.authenticated
    this.processID
    this.secretKey
    this.queryQueue = []
    this.buffer = Buffer.from([])

    this.on('readyForQuery', () => {
      if (this.queryQueue.length) {
        const query = this.queryQueue.shift();
        query.run()
      }
    })
  }

  connect() {
    this._changeStatus('connecting')
    this.client = net.connect({port: this.clientConfig.port}, (err) => {
      if (err) {
        this.emit('error', err)
        return
      }
      this.client.on('data', this._handleAuth.bind(this))
      this.client.on('close', () => this._changeStatus('disconnected'))
      this.client.on('disconnect', () => this._changeStatus('disconnected'))
      this._changeStatus('connected')
      this.client.write(Buffer.from("0000000804d2162f", 'hex')) // mystery startup message
      this.client.write(encodeConfig(this.clientConfig))
    })
    this.client.on('error', (e) => this.emit('error', e))
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
      [message, this.buffer] = parseBuffer(this.buffer)
      switch (message.header) {
        case 'K':
          this.processID = message.payload.readInt32BE(0)
          this.secretKey = message.payload.readInt32BE(4)
          break;
        case 'N':
          this._changeStatus('authenticating')
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
    if (this.status === 'disconnected') {
      this.emit('error', 'client is no longer connected')
      return;
    }
    const query = new QueryResult(this, sql)
    this.queryQueue.push(query)
    return query
  }
}

function encodeConfig(config) {
  config = Object.keys(config).reduce((c, k) => {
    // filter connection related params out
    if (['port', 'host'].indexOf(k) > -1) return c
    c[k] = config[k]
    return c
  }, {})
  const asString = Object.keys(config).reduce((string, key) => {
    return string.concat(key, '\00', config[key], '\00')
  }, '')
  const buff = Buffer.alloc(4 + 2 + 2 + asString.length + 1) // 32bit int, 16 bit int, 2 mystery bytes, trailing null
  buff.writeInt32BE(buff.length, 0)
  buff.writeInt16BE(Object.keys(config).length, 4)
  buff.write(asString.concat('\00'), 6 + 2) // 2 mystery bytes
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

function updateAuthStatus(payload) {
  return payload.readInt32BE(0) === 0
}


function createClient(options, config) {
  return new Client(options, config)
}

exports.createClient = createClient
exports.parseConfig = parseConfig
exports.encodeConfig = encodeConfig

