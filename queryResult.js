const EventEmitter = require('events');
const parseBuffer  = require('./pgUtils').parseBuffer

const NULL_BYTE_LENGTH = 1
const INT_32_LENGTH = 4

exports.QueryResult = class QueryResult extends EventEmitter {
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
        this.emit('complete', commandComplete(message.payload))
        break;
      case 'D':
        const row = parseDataRow(message.payload, this.rowDescriptions)
        this.emit('row', row)
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

function createQueryMessage(payload) {
  // allocate a new buffer with space for the payload, a type byte, and 4 message length
  // bytes + 1 nullByte. Initialize the buffer to be 0s
  const buff = Buffer.alloc(payload.length + INT_32_LENGTH + NULL_BYTE_LENGTH + NULL_BYTE_LENGTH, 0)
  buff.write('Q')
  buff.writeInt32BE(payload.length + INT_32_LENGTH + NULL_BYTE_LENGTH, 1) // these bytes + null terminator
  buff.write(payload, 5) // write payload at offset of 5
  buff.write('\00', 1 + 4 + payload.length) // write null byte
  return buff
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

exports.commandComplete = commandComplete
exports.columnName = columnName
exports.parseRowDescription = parseRowDescription
exports.parseDataRow = parseDataRow
exports.createQueryMessage = createQueryMessage
