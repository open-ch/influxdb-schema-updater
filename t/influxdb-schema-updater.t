#!/usr/bin/env perl

use 5.010;
use strict;
use warnings;

use Test::More;

use File::Temp;
use File::Slurper qw(write_text);
use IPC::Run qw(run);
use File::Spec;
use File::Basename;

sub test {
    my $curdir = get_directory_of_this_file();
    my $schemas_dir = "$curdir/data";

    my $tmpdir_handle = File::Temp->newdir(CLEANUP => 1);
    my $tmpdir = $tmpdir_handle->dirname();
    my $port = 17755;

    my $conf = get_test_conf($tmpdir, $port);
    write_text("$tmpdir/influx.conf", $conf);

    # check if influxd is found before forking
    eval {
        run_cmd('influxd', 'version');
    };
    plan(skip_all => 'influxd not found in PATH') if $@;

    my $pid;
    defined($pid = fork()) or die "unable to fork: $!\n";
    if ($pid == 0) {
        exec("influxd -config $tmpdir/influx.conf");
        warn "unable to exec 'influxd -config $tmpdir/influx.conf': $!\n";
        exit 1;
    }
    sleep 1; # wait for influxdb to start

    # empty config
    is run_updater($curdir, "$schemas_dir/test00", $port, '--diff'), ''         => 'Empty config';

    # only database
    is run_updater($curdir, "$schemas_dir/test01", $port, '--diff'), "CREATE DATABASE test;\n"
                                                                                => 'New database is detected';
    is run_updater($curdir, "$schemas_dir/test01", $port, '--diff'), "CREATE DATABASE test;\n"
                                                                                => '--diff mode doesn\'t update InfluxDB';
    run_updater($curdir, "$schemas_dir/test01", $port);
    is run_updater($curdir, "$schemas_dir/test01", $port, '--diff'), ''         => 'Database is added';

    # add a retention policy
    is run_updater($curdir, "$schemas_dir/test02", $port, '--diff'), "CREATE RETENTION POLICY \"rp1\" ON test DURATION 90d REPLICATION 1 SHARD DURATION 2w;\n"
                                                                                => 'New RP is detected';
    run_updater($curdir, "$schemas_dir/test02", $port);
    is run_updater($curdir, "$schemas_dir/test02", $port, '--diff'), ''         => 'RP is added';

    # change a retention policy
    is run_updater($curdir, "$schemas_dir/test03", $port, '--diff'), "ALTER RETENTION POLICY \"rp1\" ON test DURATION 100d REPLICATION 1 SHARD DURATION 2w;\n"
                                                                                => 'RP change is detected';
    run_updater($curdir, "$schemas_dir/test03", $port);
    is run_updater($curdir, "$schemas_dir/test03", $port, '--diff'), ''         => 'RP is updated';

    # create a retention policy on the same line as the database
    is run_updater($curdir, "$schemas_dir/test04", $port, '--diff'), "CREATE RETENTION POLICY \"rp2\" ON test DURATION 260w REPLICATION 1 SHARD DURATION 12w DEFAULT;\n"
                                                                                => 'RP on same line as create database is detected';

    run_updater($curdir, "$schemas_dir/test04", $port, '--force');
    cmp_ok $? >> 8, '==', 0                                                     => 'Exit code 0 when InfluxDB is up to date';
    is run_updater($curdir, "$schemas_dir/test04", $port, '--diff'), ''         => 'RP deleted with --force';


    # add some continuous queries
    is run_updater($curdir, "$schemas_dir/test05", $port, '--diff'), "CREATE CONTINUOUS QUERY cq1 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\nCREATE CONTINUOUS QUERY cq2 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\n"
                                                                                => 'New CQs are detected';
    run_updater($curdir, "$schemas_dir/test05", $port);
    is run_updater($curdir, "$schemas_dir/test05", $port, '--diff'), ''         => 'CQs are added';

    # change a continuous query
    is run_updater($curdir, "$schemas_dir/test06", $port, '--diff'), "DROP CONTINUOUS QUERY cq2 ON test; CREATE CONTINUOUS QUERY cq2 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT MAX(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\n"
                                                                                => 'CQ change is detected';
    run_updater($curdir, "$schemas_dir/test06", $port);
    is run_updater($curdir, "$schemas_dir/test06", $port, '--diff'), ''         => 'CQ is updated';

    # check that fill(null) is ignored
    run_updater($curdir, "$schemas_dir/test06.2", $port, '--force');
    is run_updater($curdir, "$schemas_dir/test06.2", $port, '--diff'), ''       => 'fill(null) in CQ is ignored';
    run_updater($curdir, "$schemas_dir/test06", $port, '--force'); # reset

    # remove a continuous query
    is run_updater($curdir, "$schemas_dir/test07", $port, '--diff'), "-- DROP CONTINUOUS QUERY cq2 ON test;\n"
                                                                                => 'CQ removal is detected';
    run_updater($curdir, "$schemas_dir/test07", $port);
    is run_updater($curdir, "$schemas_dir/test07", $port, '--diff'), "-- DROP CONTINUOUS QUERY cq2 ON test;\n"
                                                                                => 'CQ is not deleted without --force';
    # don't execute a delete action be default - return exit code 1
    run_updater($curdir, "$schemas_dir/test07", $port);
    cmp_ok $? >> 8, '==', 1                                                     => 'Exit code 1 when some changes are not applied';

    run_updater($curdir, "$schemas_dir/test07", $port, '--force');
    is run_updater($curdir, "$schemas_dir/test07", $port, '--diff'), ''         => 'CQ is deleted with --force';

    # test the order of updates
    is run_updater($curdir, "$schemas_dir/test08", $port, '--diff', '--force'), "DROP CONTINUOUS QUERY cq1 ON test;\nDROP DATABASE test;\nCREATE DATABASE test2;\nCREATE RETENTION POLICY \"rp1\" ON test2 DURATION 100d REPLICATION 1 SHARD DURATION 2w;\nCREATE RETENTION POLICY \"rp2\" ON test2 DURATION 260w REPLICATION 1 SHARD DURATION 12w DEFAULT;\nCREATE CONTINUOUS QUERY cq1 ON test2 RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test2.rp2.m FROM test2.rp1.m GROUP BY time(5m) END;\n"

                                                                                => 'Updates applied in the right order';

    is run_updater($curdir, "$schemas_dir/test00", $port, '--diff'), "-- DROP CONTINUOUS QUERY cq1 ON test;\n-- DROP DATABASE test;\n"
                                                                                => 'Old database is detected';
    run_updater($curdir, "$schemas_dir/test00", $port);
    is run_updater($curdir, "$schemas_dir/test00", $port, '--diff'), "-- DROP CONTINUOUS QUERY cq1 ON test;\n-- DROP DATABASE test;\n"
                                                                                => 'Database is not deleted without --force';
    run_updater($curdir, "$schemas_dir/test00", $port, '--force');
    is run_updater($curdir, "$schemas_dir/test00", $port, '--diff'), ''         => 'Database is deleted with --force';

    run_updater($curdir, "$schemas_dir/test10", $port, '--diff');
    cmp_ok $? >> 8, '==', 255                                                   => 'Exit with error when a database is created a second time';

    run_updater($curdir, "$schemas_dir/test02", $port);
    is run_updater($curdir, "$schemas_dir/test02", $port, '--diff'), ''         => 'Running the updater a second time for the same config does nothing (regression LAKE-338)';
    
    clean_db_state($curdir, $schemas_dir, $port);
    # create db and retention policy
    run_updater($curdir, "$schemas_dir/test11", $port);
    # try to delete the policy created above, should not be executed without --force
    run_updater($curdir, "$schemas_dir/test12", $port);
    is run_updater($curdir, "$schemas_dir/test12", $port, '--diff'), "-- DROP RETENTION POLICY \"rp11\" ON test11;\n"
                                                                                => 'Retention policy is not deleted without --force';

    done_testing();

    kill 'KILL', $pid;
}


#
# Deletes all databases and retention policies from InfluxDB, to get a clean state for a test.
#
# Arguments:
#     $curdir string: the current directory from where the script is ran
#     $schemas_dir string: the name of the directory where the config files are
#     $port string: the port where InfluxDB is running
#
# Returns:
#
sub clean_db_state {
    my ($curdir, $schemas_dir, $port) = @_;

    run_updater($curdir, "$schemas_dir/test00", $port, '--force');
}


sub run_updater {
    my ($curdir, $schema_dir, $port, @flags) = @_;
    return run_cmd("$curdir/../influxdb-schema-updater", '--config', $schema_dir, '--url', "localhost:$port", @flags);
}

sub run_cmd {
    my @cmd = @_;
    my $out_and_err;
    run(\@cmd, '>&', \$out_and_err);

    return $out_and_err;
}

sub get_directory_of_this_file {
    my (undef, $filename) = caller;
    return dirname(File::Spec->rel2abs( $filename ));
}

# ------------------------------------------------------------------------------

sub get_test_conf {
    my ($tmpdir, $port) = @_;
    return <<"END";
reporting-disabled = true

[logging]
  level = "warn"
  suppress-logo = true

[meta]
  dir = "$tmpdir/meta"
  # don't create the autogen policy
  retention-autocreate = false
  logging-enabled = true

[data]
  dir = "$tmpdir/data"
  engine = "tsm1"
  wal-dir = "$tmpdir/wal"
  wal-logging-enabled = true
  query-log-enabled = true
  cache-max-memory-size = 0
  max-points-per-block = 0
  max-series-per-database = 0
  max-values-per-tag = 0
  data-logging-enabled = true
  index-version = "tsi1"

[coordinator]
  write-timeout = "10s"
  max-concurrent-queries = 0
  query-timeout = "0s"
  log-queries-after = "0s"
  max-select-point = 0
  max-select-series = 0
  max-select-buckets = 0

[retention]
  enabled = true
  check-interval = "30m0s"

[shard-precreation]
  enabled = true
  check-interval = "10m0s"
  advance-period = "30m0s"

[admin]
  enabled = false

[monitor]
  store-enabled = true
  store-database = "_internal"
  store-interval = "10s"

[subscriber]
  enabled = true
  http-timeout = "30s"

[http]
  enabled = true
  bind-address = ":$port"
  auth-enabled = false
  log-enabled = false
  write-tracing = false
  https-enabled = false
  max-row-limit = 0
  max-connection-limit = 0
  shared-secret = ""
  realm = "InfluxDB"

[[graphite]]
  enabled = false

[[collectd]]
  enabled = false

[[opentsdb]]
  enabled = false

[[udp]]
  enabled = false

[continuous_queries]
  log-enabled = true
  enabled = true
  run-interval = "1s"
  query-stats-enabled = true
END
}

# ------------------------------------------------------------------------------

test();
