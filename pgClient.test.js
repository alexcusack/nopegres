const assert        = require('assert')
const pgClient      = require('./pgClient')
const parseConfig   = pgClient.parseConfig
const createClient  = pgClient.createClient
const encodeConfig  = pgClient.encodeConfig

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

test('encodeConfig', () => {
  const clientConfigMessage = "0000 0041 0003 0000 " +
  "7573 6572 0061 6c65 7863 7573 6163 6b00 " +
  "6461 7461 6261 7365 0070 6f73 7467 7265 " +
  "7300 6170 706c 6963 6174 696f 6e5f 6e61 " +
  "6d65 0070 7371 6c00 00"
  const expected = Buffer.from(clientConfigMessage.replace(/ /g, ''), 'hex')
  const actual = encodeConfig({
    user: 'alexcusack',
    database: 'postgres',
    application_name: 'psql'
  })
  assert(actual.equals(expected), true)
})

pgClientConfig = {
  user: 'alexcusack',
  database: 'postgres',
  application_name: 'psql',
}

const client = createClient({port: 5433}, pgClientConfig)
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
