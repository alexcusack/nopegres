const net    = require('net')
const assert = require('assert')

function parseBuffer(buffer) {
  if (buffer.length === 0) return [{}, buffer]
  const header = new Buffer([buffer[0]]).toString()
  if (buffer.length === 1 && header === 'N') {
    // initial acknowledgement byte
    return [{header: header, length: 1, payload: null}, buffer.slice(1)]
  }
  const messageLength = buffer.readInt32BE(1)
  const fullMessage = buffer.length >= messageLength
  const message = {
    header: header,
    length: messageLength, // returning as meta-data for sanity checks
    payload: fullMessage ? buffer.slice(5, messageLength + 1) : null, // messageLength + 1 to inclusive of final byte
  }
  return [
    message,
    fullMessage ? buffer.slice(messageLength + 1) : buffer
  ]
}

function hexToBuffer(hexString) {
  return new Buffer(hexString.replace(/ /g, ''), 'hex')
}


module.exports = {
  parseBuffer: parseBuffer,
  hexToBuffer: hexToBuffer
}

