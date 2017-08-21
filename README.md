Just a test for fetching postgres logical replication data, 
and push it to kafka. 

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