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
* Experiment with this library with a temporary postgres instance so any mistakes won't mess up your data
```
$ cd /tmp
$ initdb pgfiles
$ postgres -D pgfiles -h localhost -p 5433
```

* In a separate session:
```
$ cd path/to/nopegres
$ node
> pg = require('./index')
> conf = { port: 5433, user: process.env.USER, database: 'postgres', application_name: 'psql' }
> client = pg.createClient(conf)
Client {...}
> res = client.query('select x from values (1)')
QueryResult {...}
> res.on("row", (row) => console.log(row))
{ x: 1 }
> client.end()
> client.status
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
