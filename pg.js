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

function getReponse(buffer) {
  while (buffer.length) {
    let message;
    [message, buffer] = parseBuffer(buffer)
    if (message.payload) {
      // parsing all as S right now so some are non-conforming
      console.log(payloadParser.S(message.payload))
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
function hexToBuffer(hexString) {
  return new Buffer(hexString.replace(/ /g, ''), 'hex')
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

const initialStartupMessage = "0000 0008 04d2 162f"

const clientConfigMessage = "0000 0041 0003 0000 " +
"7573 6572 0061 6c65 7863 7573 6163 6b00 " +
"6461 7461 6261 7365 0070 6f73 7467 7265 " +
"7300 6170 706c 6963 6174 696f 6e5f 6e61 " +
"6d65 0070 7371 6c00 00"

const fullStartupMessage = initialStartupMessage.concat(clientConfigMessage)

function getClient(options) {
  return new Promise((resolve, reject) => {
    client = net.connect(options, () => {
      console.log(`Successful socket connection to ${JSON.stringify(options)}`)
      return resolve(client)
    })
  })
}
getClient.documentation = `takes an options object and returns a Promise of a client. Throws and error if unable to connect`


// getClient({port: PGPORT})
// .then((client) => {
//   // attach startup listener and write client startup messages
//   console.log('got client, attaching listeners')
//   client.on('data', (data) => {
//     console.log('got data!', data)
//     // getReponse(data)
//   })
//   client.on('error', (err) => console.log('ERROR:', err))
//   client.on('end', () => console.log('disconnected from server'));
//   client.on('close', (had_error) => console.log('client connection closed', had_error));
//   return client
// })
// .then((client) => {
//   console.log('writing startup messages')
//   client.write(Buffer.from(fullStartupMessage.replace(/ /g, ''), 'hex'))
//   return client
// })
// .then((client) => {
//   console.log('trying a sample query')
//   // const query = createQueryMessage('select 1')
//   client.write(query)
// })

module.exports = {
  parseBuffer: parseBuffer,
  payloadParser: payloadParser,
  createQueryMessage: createQueryMessage,
  test: test
}

// Test
function test(name, callback) {
  try {
    callback()
  } catch (e) {
    console.error(`Failed: ${name}`)
    console.error(e)
    process.exit(1)
  }
  console.log(`Passed: ${name}`)
}

// test('hexToBuffer', () => {
//   const actual = hexToBuffer("5300 0000 1a61").length
//   const expected = 6
//   assert.deepEqual(actual, expected)
// })

// test('createQueryMessage \'select 1\'', () => {
//   const actual = createQueryMessage('select 1')
//   const expected = Buffer.from("510000000d73656c656374203100", 'hex')
//   assert.equal(actual.equals(expected), true)
// })
// test('createQueryMessage query type code', () => {
//   const actual = Buffer.from([createQueryMessage('')[0]])
//   const expected = 'Q'
//   assert.equal(actual, expected)
// })
// test('createQueryMessage length', () => {
//   const actual = createQueryMessage('').readInt32BE(1)
//   const expected = '5'
//   assert.equal(actual, expected)
// })
// test('createQueryMessage end with nullByte', () => {
//   const queryBuffer = createQueryMessage('')
//   const actual = Buffer.from([queryBuffer[queryBuffer.length - 1]])
//   const expected = '\00'
//   assert.equal(actual, expected)
// })

// test('parseBuffer: full', () => {
//   const fullServerSMessage = hexToBuffer(
//     "5300 0000 1a61 7070 6c69 6361 7469 6f6e 5f6e 616d 6500 7073 716c 00"
//   )
//   const parsedMessage = parseBuffer(fullServerSMessage);
//   assert.equal(parsedMessage[0].header, 'S', 'Message type')
//   assert.equal(parsedMessage[0].length, 26, 'Message length')
// })

// test('parseBuffer: partial', () => {
//   const partialServerSMessage = hexToBuffer("5300 0000 1a61 7070 6c69 6361 7469")
//   const parsedPartialMessage = parseBuffer(partialServerSMessage)
//   assert.equal(parsedPartialMessage[0].header, 'S')
//   assert.equal(parsedPartialMessage[0].length, 26)
//   assert.equal(parsedPartialMessage[0].payload, null)
//   assert.equal(parsedPartialMessage[1].length, 14)
// })

// test('parseBuffer: empty buffer', () => {
//   const actual = parseBuffer(new Buffer([]))
//   const expected = [{}, new Buffer([])]
//   assert.deepEqual(actual, expected)
// })

// test('payloadParser: \'S\'', () => {
//   const fullServerSMessage = hexToBuffer(
//     "5300 0000 1a61 7070 6c69 6361 7469 6f6e 5f6e 616d 6500 7073 716c 00"
//   )
//   const payload =parseBuffer(fullServerSMessage)[0].payload
//   const actual = payloadParser.S(payload)
//   const expected = {application_name: 'psql'}
//   assert.deepEqual(actual, expected)
// })
