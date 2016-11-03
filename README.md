# nopegres
zero dependency Node/Javascript driver for Postgres


####Why is this called `nopegres`?
* Because the answer to 'does this do x', is likely 'nope'
* Client pooling? Nope.
* Known third-party test framework? Nope.
* Any dependencies at all? Nope.

Wait, then why did you make this?
* For fun.
* ..But also to experience implementing a binary protocol and better understanding of the Postgres API specifically

####Test
`npm test`

####Use
* Open a console and enter the following commands
```
$ cd /tmp
$ initdb pgfiles
$ postgres -D pgfiles -h localhost -p 5433
```

* In a separate session, `cd` to the project root directory and open a Node REPL (v6.x.x^)
```
$ pg = require('./index')

// { createClient: [Function: createClient],
//   parseConfig: [Function: parseConfig],
//   encodeConfig: [Function: encodeConfig] }

$ client = pg.createClient({port: 5433}, {user: '<username>', database: 'postgres', application_name: 'psql'})

// Client {....}
// Returns a Client Event Emitter that wraps a socket

$ client.query('select 1')

// QueryResult {...}
// Returns a QueryResult Event Emitter that will emit 'row' events has rows are returned from the
// Postgres server

$ client.end()
// closes socket

$ client.status
'disconnected'
```

###Client (Event Emitter)
#####Events
* `statusChange` - emitted every time `Client.status` changes
* - statuses: `connecting | connected | authenticating | readyForQuery | querying | disconnected`


###QueryResult (Event Emitter)
#####Events
* `row` - emits a result row from the query
* `complete` - query has completed
* `done` - no more messages will be received on this Emitter, safe to drop all listeners
* `error` - emitted on server error, emits an error message string
