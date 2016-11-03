const assert      = require('assert')
const pgUtils     = require('./pgUtils')
const parseBuffer = pgUtils.parseBuffer
const hexToBuffer = pgUtils.hexToBuffer

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

test('hexToBuffer', () => {
  const actual = hexToBuffer("5300 0000 1a61").length
  const expected = 6
  assert.deepEqual(actual, expected)
})

test('parseBuffer: full', () => {
  const fullServerSMessage = hexToBuffer(
    "5300 0000 1a61 7070 6c69 6361 7469 6f6e 5f6e 616d 6500 7073 716c 00"
  )
  const parsedMessage = parseBuffer(fullServerSMessage);
  assert.equal(parsedMessage[0].header, 'S', 'Message type')
  assert.equal(parsedMessage[0].length, 26, 'Message length')
})

test('parseBuffer: partial', () => {
  const partialServerSMessage = hexToBuffer("5300 0000 1a61 7070 6c69 6361 7469")
  const parsedPartialMessage = parseBuffer(partialServerSMessage)
  assert.equal(parsedPartialMessage[0].header, 'S')
  assert.equal(parsedPartialMessage[0].length, 26)
  assert.equal(parsedPartialMessage[0].payload, null)
  assert.equal(parsedPartialMessage[1].length, 14)
})

test('parseBuffer: empty buffer', () => {
  const actual = parseBuffer(new Buffer([]))
  const expected = [{}, new Buffer([])]
  assert.deepEqual(actual, expected)
})
