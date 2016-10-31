const net    = require('net')
const assert = require('assert')

const NULL_BYTE_LENGTH = 1
const INT_32_LENGTH = 4
const PGPORT = 5433

const configuration = {
  username: 'alexcusack',
  database: 'postgres',
  application_name: 'psql'
}


function parseMsg(buffer) {
  if (buffer.length === 0) return [{}, buffer]
  const header = new Buffer([buffer[0]]).toString()
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

function getReponse(buffer) {
  while (buffer.length) {
    let message;
    [message, buffer] = parseMsg(buffer)
    if (message.payload) {
      // parsing all as S right now so some are non-conforming
      // console.log(payloadParser.S(message.payload))
    }
  }
}

const payloadParser = {
  S: (buffer) => {
    // convert buffer to map based on `key<nullByte>value`
    const asArray = buffer.toString()
                          .split('\00')
    return asArray.reduce((map, _, index) => {
        if (index % 2 !== 0) {
          map[asArray[index - 1]] = asArray[index]
        }
        return map
      }, {})
    }
}

// ////// ////// write messages ///// ////// ////// ////// ////

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

function getClient(port) {
  return new Promise((resolve, reject) => {
    client = net.connect({port: port}, () => {
      console.log(`Successful socket connection to ${port}`)
      return resolve(client)
    })
  })
}

const initialStartupMessage = "0000 0008 04d2 162f"

const clientConfigMessage = "0000 0041 0003 0000 " +
"7573 6572 0061 6c65 7863 7573 6163 6b00 " +
"6461 7461 6261 7365 0070 6f73 7467 7265 " +
"7300 6170 706c 6963 6174 696f 6e5f 6e61 " +
"6d65 0070 7371 6c00 00"

const fullStartupMessage = initialStartupMessage.concat(clientConfigMessage)

getClient(PGPORT)
.then((client) => {
  // attach startup listener and write client startup messages
  console.log('got client, attaching listeners')
  client.on('data', (data) => console.log('receieved Data:', data.toString()))
  client.on('error', (err) => console.log('ERROR:', err))
  client.on('end', () => console.log('disconnected from server'));
  client.on('close', (had_error) => console.log('client connection closed', had_error));
  return client
})
.then((client) => {
  console.log('writing startup messages')
  // client.write(startConnectionMessage)
  client.write(Buffer.from(fullStartupMessage.replace(/ /g, ''), 'hex'))
  // client.write(Buffer.concat([startConnectionMessage, configMessage]), totalLength)
  // const query = createQueryMessage('select 1')
  // client.write(querys, query.length)
  return client
})


// READ TESTS
function hexToBuffer(hexString) {
  return new Buffer(hexString.replace(/ /g, ''), 'hex')
}
assert.equal(hexToBuffer("5300 0000 1a61").length, 6, 'hexToBuffer broken')
console.log('hexToBuffer: okay')

const fullServerSMessage = hexToBuffer("5300 0000 1a61 7070 6c69 6361 7469 6f6e 5f6e 616d 6500 7073 716c 00")
assert.equal(parseMsg(fullServerSMessage)[0].header, 'S', 'Broke message type parsing')
assert.equal(parseMsg(fullServerSMessage)[0].length, 26, 'Broke length parsing')
assert.deepEqual(payloadParser.S(parseMsg(fullServerSMessage)[0].payload), {application_name: 'psql'}, '\'S\' parser broken')
console.log('fullServerSMessage: okay')

const partialServerSMessage = hexToBuffer("5300 0000 1a61 7070 6c69 6361 7469")
assert.equal(parseMsg(partialServerSMessage)[0].header, 'S', 'Broke message type parsing')
assert.equal(parseMsg(partialServerSMessage)[0].length, 26, 'Broke length parsing')
assert.equal(parseMsg(partialServerSMessage)[0].payload, null, 'Partial messages should not return payloads')
assert.equal(parseMsg(partialServerSMessage)[1].length, 14, 'Partial buffers should not be mutated')
console.log('partialServerSMessage: okay')

assert.deepEqual(parseMsg(new Buffer([])), [{}, new Buffer([])], 'broke parsing empty buffer')
console.log('emptyServerMessage: okay')

const fullServerMessageSet = hexToBuffer("5300 0000 1a61 7070 6c69 6361 7469 6f6e 5f6e 616d 6500 7073 716c 0053 0000 0019 636c 6965 6e74 5f65 6e63 6f64 696e 6700 5554 4638 0053 0000 0017 4461 7465 5374 796c 6500 4953 4f2c 204d 4459 0053 0000 0019 696e 7465 6765 725f 6461 7465 7469 6d65 7300 6f6e 0053 0000 001b 496e 7465 7276 616c 5374 796c 6500 706f 7374 6772 6573 0053 0000 0014 6973 5f73 7570 6572 7573 6572 006f 6e00 5300 0000 1973 6572 7665 725f 656e 636f 6469 6e67 0055 5446 3800 5300 0000 1973 6572 7665 725f 7665 7273 696f 6e00 392e 342e 3500 5300 0000 2573 6573 7369 6f6e 5f61 7574 686f 7269 7a61 7469 6f6e 0061 6c65 7863 7573 6163 6b00 5300 0000 2373 7461 6e64 6172 645f 636f 6e66 6f72 6d69 6e67 5f73 7472 696e 6773 006f 6e00 5300 0000 1854 696d 655a 6f6e 6500 5553 2f50 6163 6966 6963 004b 0000 000c 0000 a1f6 6efa 2558 5a00 0000 0549 5400 0000 2100 013f 636f 6c75 6d6e 3f00 0000 0000 0000 0000 0017 0004 ffff ffff 0000 4400 0000 0b00 0100 0000 0131 4300 0000 0d53 454c 4543 5420 3100 5a00 0000 0549")

getReponse(fullServerMessageSet)
console.log('getReponse (fullServerMessageSet): okay')

// WRITE TESTS
assert.equal(createQueryMessage('select 1').equals(Buffer.from("510000000d73656c656374203100", 'hex')), true)
assert.equal(Buffer.from([createQueryMessage('')[0]]), 'Q')
assert.equal(createQueryMessage('').readInt32BE(1), '5', 'Failed: Empty querys should have length of 5')
assert.equal(Buffer.from([createQueryMessage('')[createQueryMessage('').length - 1]]), '\00', 'Failed: Messages should end with nullbyte')
console.log('createMessageSimpleQuery: okay')


// async tests
