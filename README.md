Just a test for fetching postgres logical replication data, 
and push it to kafka. 
Not anything close to production ready!

This is a testing for forwarding pglogical messages to kafka fast.
Messages are not serialized here, and pushed as it is.
Only if message is too large, it is split and sent to kafka in pieces.

PGLOGICAL_QUERY_INTERVALS_MILLISEC can be used to control query interval to fetch from logical replication slot.
To small interval will result in message duplication in subsequent chunks if there are a lot 
of operations happening at the same time (i.e. bulk updates/insrts/deletes)

kafka docs:
https://kafka.apache.org/quickstart

postgres wal2json docs:
https://www.postgresql.org/docs/9.4/static/logicaldecoding-example.html

https://github.com/eulerto/wal2json


Docs for go-kafka package:
https://github.com/confluentinc/confluent-kafka-go

The kafka client for Go depends on librdkafka v0.11.0 or later,
you either need to install librdkafka through your
OS/distributions package manager, or download and build
it from source.

building from source:
git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
./configure --prefix /usr
make
sudo make install

Postgres logical replication will NOT work out-of-the box
after installation. Explicit change of configuration files required.
Check details in models/initdb.go file

Make sure that replication connections are permitted for user postgres in pg_hba.conf:

    host    replication     <<_user_>>        127.0.0.1/32            trust

and reload the server configuration.

You also need to set wal_level=logical and max_wal_senders, max_replication_slots to value greater than zero in postgresql.conf
(these changes require a server restart). Create a database psycopg2_test.

in order to launch:
modify constants in main.go (connection args), then

$$ go run mail.go test  

(test - kafka chanel)

