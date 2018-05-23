# InfluxDB Schema Updater [![Build Status](https://travis-ci.org/open-ch/influxdb-schema-updater.svg?branch=master)](https://travis-ci.org/open-ch/influxdb-schema-updater)

The InfluxDB schema updater is a small DevOps tool to manage the schema of an [InfluxDB](https://github.com/influxdata/influxdb) instance with a set of configuration files. 

## SYNOPSIS

`influxdb-schema-updater [--help] [--dryrun] [--diff] [--force] [--config <schema_location>] [--url <url>]`

## OPTIONS

- **--help**

    Print a help message and exit.

- **--dryrun**

    Print the changes which would be applied in normal mode.

- **--diff**

    Print the InfluxQL queries instead of executing them.

- **--force**

    Apply the changes which were prevented in normal mode.

- **--config**

    The directory where the schema files are located. Default is /etc/influxdb/schema/.

- **--url**

    The url where the InfluxDB HTTP API is reachable. Default is localhost:8086.

## DESCRIPTION

This tool compares the databases, retention policies (RPs) and continuous queries (CQs) found in the `<schema_location>` directory to the ones in the InfluxDB instance reachable at `<url>`. If there is a difference, InfluxDB will be updated. Some changes like deleting a database are skipped when the `--force` flag is not set.

The exit code is 0 if and only if every required update has been executed successfully.

The `<schema_location>` directory should have the following structure:

```
db/
    <db_file1>
    <db_file2>
    ...
cq/
    <cq_file1>
    <cq_file2>
        ...
```

The files in `db/` contain `CREATE` queries for databases followed by their their RPs, for example:

```
CREATE DATABASE test WITH DURATION 100d REPLICATION 1 SHARD DURATION 2w NAME rp1;
CREATE RETENTION POLICY rp2 ON test DURATION 260w REPLICATION 1 SHARD DURATION 12w;

CREATE DATABASE test2;
CREATE RETENTION POLICY rp1 ON test2 DURATION 100d REPLICATION 1 SHARD DURATION 2w;
CREATE RETENTION POLICY rp2 ON test2 DURATION 260w REPLICATION 1 SHARD DURATION 12w;
CREATE RETENTION POLICY rp3 ON test2 DURATION INF REPLICATION 1 SHARD DURATION 260w;
```

The files in `cq/` contain CQs usually corresponding to the databases declared in the file with the same name in `db/`, for example:

```
CREATE CONTINUOUS QUERY cq1 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;

CREATE CONTINUOUS QUERY cq1 ON test2 RESAMPLE EVERY 30m FOR 1h BEGIN SELECT LAST(a) AS b, c INTO test2.rp2.m FROM test2.rp1.m GROUP BY time(30m) END;
CREATE CONTINUOUS QUERY cq2 ON test2 RESAMPLE EVERY 1d FOR 2d BEGIN SELECT LAST(a) AS b, c INTO test2.rp3.m FROM test2.rp2.m GROUP BY time(1d) END;
```
