const assert        = require('assert')
const pgClient      = require('./pgClient')
const parseConfig   = pgClient.parseConfig
const createClient  = pgClient.createClient

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


test('parseConfig', () => {
  const payload = Buffer.from('54696d655a6f6e650055532f5061636966696300', 'hex')
  const expected = {TimeZone: 'US/Pacific'}
  const actual = parseConfig(payload)
  assert.deepEqual(actual, expected)
})


const client = createClient({port: 5433})
client.on('statusChange', (oldStatus, newStatus) => {
  console.log('status changed:', oldStatus, '->', newStatus)
})
const rows = client.query('select 1')
rows.on('row', (row) => {
  // console.log('Row: ', row)
  assert.deepEqual(row, {'?column?': '1'})
})
rows.on('complete', (res) => {
  // console.log(`got ${res} rows`)
  assert.equal(res, 'SELECT 1\u0000')
})
rows.on('done', () => {
  // console.log('done with \'rows\', removeing all listeners')
  rows.removeAllListeners();
})
const rows2 = client.query('select (1, 2, 3)')
rows2.on('row', (row) => {
  // console.log('Row: ', row)
  assert.deepEqual(row, {row: '(1,2,3)'})
})
rows2.on('complete', (res) => {
  // console.log(`Got ${count} rows`)
  assert.equal(res, 'SELECT 1\u0000')
})
rows2.on('done', () => {
  // console.log('done with \'rows2\', removeing all listeners')
  rows2.removeAllListeners();
  client.end()
})
