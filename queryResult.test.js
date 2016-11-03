const assert              = require('assert')
const queryResult         = require('./queryResult')
const commandComplete     = queryResult.commandComplete
const columnName          = queryResult.columnName
const parseRowDescription = queryResult.parseRowDescription
const parseDataRow        = queryResult.parseDataRow
const createQueryMessage  = queryResult.createQueryMessage

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


test('commandComplete', () => {
  const payload = Buffer.from('53454c454354203100', 'hex')
  const expected = 'SELECT 1\000'
  const actual = commandComplete(Buffer.from('53454c454354203100', 'hex'))
  assert.equal(actual, expected)
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
  const expected = [{
    fieldName: '?column?',
    tableID: 0,
    attributeNumber: 0,
    fieldDataTypeID: 23,
    fieldTypeSize: 4,
    typeModifier: -1,
    formatCode: 0
  }]
  const actual = parseRowDescription(payload)
  assert.deepEqual(actual, expected)
})

test('parseDataRow', () => {
  const payload = Buffer.from('00010000000131', 'hex')
  const rowDescriptions = [{
    fieldName: '?column?',
    tableID: 0,
    attributeNumber: 0,
    fieldDataTypeID: 23,
    fieldTypeSize: 4,
    typeModifier: -1,
    formatCode: 0
  }]
  const expected = {'?column?': '1'}
  const actual = parseDataRow(payload, rowDescriptions)
  assert.deepEqual(actual, expected)
})

test('createQueryMessage \'select 1\'', () => {
  const actual = createQueryMessage('select 1')
  const expected = Buffer.from("510000000d73656c656374203100", 'hex')
  assert.equal(actual.equals(expected), true)
})
test('createQueryMessage query type code', () => {
  const actual = Buffer.from([createQueryMessage('')[0]])
  const expected = 'Q'
  assert.equal(actual, expected)
})
test('createQueryMessage length', () => {
  const actual = createQueryMessage('').readInt32BE(1)
  const expected = '5'
  assert.equal(actual, expected)
})
test('createQueryMessage end with nullByte', () => {
  const queryBuffer = createQueryMessage('')
  const actual = Buffer.from([queryBuffer[queryBuffer.length - 1]])
  const expected = '\00'
  assert.equal(actual, expected)
})