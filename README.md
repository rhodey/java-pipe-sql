# java-pipe-sql
This library was created as an attempt to work around [issues found with](https://github.com/brianc/node-postgres/issues/3174) the popular node.js postgresql library pg. The root of these issues is decidedly server-side however the pg client lib is crashing as a result of protocol errors instead of emitting an error event and/or resolving the query callback with an error. Java was chosen for this because the `org.postgresql` java library is the most battle-tested psql client.

## Summary
  + The Java source code in the repo is packaged into a jar
  + The jar is spawned as a child process by Node java-pipe-psql
  + java-pipe-psql communicates with the jar over stdin, stderr, and stdout

## About these protocol errors
This library was developed and then deployed to approx 70% of our production stack. The node.js component of this library has [the same API as pg](https://github.com/rhodey/java-pipe-sql/blob/master/benchmark.js) and so basically no changes were made to our prod services meaning importantly the services exhibited the same query patterns as when using pg. What happens is that roughly every 3-4 weeks we have a number of different services across two availability zones begin to crash every few hours and continue to crash for 24-48 hours on average. Even after deploying this java+node library to 70% of our stack the problematic behavior continued in both 70% and 30% of the stack.

## Node stack traces
The stack traces from pg look something like this:
```
TypeError: Cannot read properties of null (reading 'name')
at Client._handleParseComplete (/app/node_modules/pg/lib/client.js:380:26)
at Connection.emit (node:events:517:28)
at Connection.emit (node:domain:489:12)
at /app/node_modules/pg/lib/connection.js:116:12
at Parser.parse (/app/node_modules/pg-protocol/dist/parser.js:40:17)
at Socket.<anonymous> (/app/node_modules/pg-protocol/dist/index.js:11:42)
at Socket.emit (node:events:517:28)
at Socket.emit (node:domain:489:12)
at addChunk (node:internal/streams/readable:335:12)
at readableAddChunk (node:internal/streams/readable:308:9)
```

This does not tell us a lot about what went wrong only that the library thought it was safe to access a variable and it turned out it was not because the variable was null. To help identify what is going wrong a facility was added to the java+node lib to log all stack traces and to fail the query instead of crashing and to automatically [retry failed queries](https://github.com/rhodey/java-pipe-sql/blob/master/index.js#L515) up to two times. The retry policy ended up working aka re-submitting the failed queries again with no changes succeeds and this is another reason we are confident something server-side or something in between such as pgproxy or pgbouncer is at fault.

## Java stack traces
The 70% of services which got the new java+node library in prod reliably produced two failure modes, the first failure mode is a NullPointerException when trying to parse `java.sql.ParameterMetaData` after `java.sql.Connection.prepareStatement()` succeeds. Specifically `ParameterMetaData.getParameterTypeName()` is returning null which is not normal behavior. In code the null is returned from [this line specifically](https://github.com/rhodey/java-pipe-sql/blob/master/src/main/java/app/velodata/Queries.java#L227)

The second failure mode is a `org.postgresql.util.PSQLException` with message "No results were returned by the query". When using a `java.sql.Connection` if you submit an INSERT or UPDATE query you are supposed to call `PreparedStatement.executeUpdate()` instead of `PreparedStatement.executeQuery()`. If you call `executeQuery()` on an INSERT or UPDATE this is the exception and message you get from the driver. Note that this exception does not simply mean zero rows were returned as is with e.g. `SELECT 1 WHERE 1=2` this is something else. In code the exception is thrown from [this line specifically](https://github.com/rhodey/java-pipe-sql/blob/master/src/main/java/app/velodata/Queries.java#L243)

## Develop
This is how to build the jar and how to run the [test suite](https://github.com/rhodey/java-pipe-sql/blob/master/tests.js)
```
cp example.env .env
export $(cat .env | xargs)
docker-compose up -d postgres
mvn package
npm run test
```

## Next steps
The protocol errors causing our issues with node pg cannot be reproduced on demand but we hope that with the added information we gathered via the java stack traces we have enough to go on to add some defensive guards to the pg client lib.

## License
Copyright 2024 - Velo Data
