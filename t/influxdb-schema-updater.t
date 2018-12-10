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

my $port = 17755;
my $curdir = get_directory_of_this_file();
my $schemas_dir = "$curdir/data";

# check if influxd is found before forking
eval {
    run_cmd('influxd', 'version');
};
plan(skip_all => 'influxd not found in PATH') if $@;


sub test {

    my ($pid, $tmpdir_handle) = start_db();

    # empty config
    is run_updater($curdir, "$schemas_dir/test00", $port, 0, '--diff'), ''         => 'Empty config';

    # only database
    is run_updater($curdir, "$schemas_dir/test01", $port, 0, '--diff'), qq{CREATE DATABASE "test";\n}
                                                                                => 'New database is detected';
    is run_updater($curdir, "$schemas_dir/test01", $port, 0, '--diff'), qq{CREATE DATABASE "test";\n}
                                                                                => '--diff mode doesn\'t update InfluxDB';
    run_updater($curdir, "$schemas_dir/test01", $port, 0);
    is run_updater($curdir, "$schemas_dir/test01", $port, 0, '--diff'), ''         => 'Database is added';

    # add a retention policy
    is run_updater($curdir, "$schemas_dir/test02", $port, 0, '--diff'), qq{CREATE RETENTION POLICY "rp1" ON "test" DURATION 90d REPLICATION 1 SHARD DURATION 2w;\n}
                                                                                => 'New RP is detected';
    run_updater($curdir, "$schemas_dir/test02", $port, 0);
    is run_updater($curdir, "$schemas_dir/test02", $port, 0, '--diff'), ''         => 'RP is added';

    # change a retention policy
    is run_updater($curdir, "$schemas_dir/test03", $port, 0, '--diff'), qq{ALTER RETENTION POLICY "rp1" ON "test" DURATION 100d REPLICATION 1 SHARD DURATION 2w;\n}
                                                                                => 'RP change is detected';
    run_updater($curdir, "$schemas_dir/test03", $port, 0);
    is run_updater($curdir, "$schemas_dir/test03", $port, 0, '--diff'), ''         => 'RP is updated';

    # create a retention policy on the same line as the database
    is run_updater($curdir, "$schemas_dir/test04", $port, 0, '--diff'), qq{CREATE RETENTION POLICY "rp2" ON "test" DURATION 260w REPLICATION 1 SHARD DURATION 12w DEFAULT;\n}
                                                                                => 'RP on same line as create database is detected';

    run_updater($curdir, "$schemas_dir/test04", $port, 0, '--force');
    is run_updater($curdir, "$schemas_dir/test04", $port, 0, '--diff'), ''         => 'RP deleted with --force';


    # add some continuous queries
    is run_updater($curdir, "$schemas_dir/test05", $port, 0, '--diff'), "CREATE CONTINUOUS QUERY cq1 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\nCREATE CONTINUOUS QUERY cq2 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\n"
                                                                                => 'New CQs are detected';
    run_updater($curdir, "$schemas_dir/test05", $port, 0);
    is run_updater($curdir, "$schemas_dir/test05", $port, 0, '--diff'), ''         => 'CQs are added';

    # change a continuous query
    is run_updater($curdir, "$schemas_dir/test06", $port, 0, '--diff'), qq{DROP CONTINUOUS QUERY "cq2" ON "test"; CREATE CONTINUOUS QUERY cq2 ON test RESAMPLE EVERY 5m FOR 10m BEGIN SELECT MAX(a) AS b, c INTO test.rp2.m FROM test.rp1.m GROUP BY time(5m) END;\n}
                                                                                => 'CQ change is detected';
    run_updater($curdir, "$schemas_dir/test06", $port, 0);
    is run_updater($curdir, "$schemas_dir/test06", $port, 0, '--diff'), ''         => 'CQ is updated';

    # check that fill(null) is ignored
    run_updater($curdir, "$schemas_dir/test06.2", $port, 0, '--force');
    is run_updater($curdir, "$schemas_dir/test06.2", $port, 0, '--diff'), ''       => 'fill(null) in CQ is ignored';
    run_updater($curdir, "$schemas_dir/test06", $port, 0, '--force'); # reset

    # remove a continuous query
    is run_updater($curdir, "$schemas_dir/test07", $port, 0, '--diff'), qq{-- DROP CONTINUOUS QUERY "cq2" ON "test";\n}
                                                                                => 'CQ removal is detected';
    run_updater($curdir, "$schemas_dir/test07", $port, 1);
    is run_updater($curdir, "$schemas_dir/test07", $port, 0, '--diff'), qq{-- DROP CONTINUOUS QUERY "cq2" ON "test";\n}
                                                                                => 'CQ is not deleted without --force';
    # don't execute a delete action be default - return exit code 1 when some changes are not applied
    is run_updater($curdir, "$schemas_dir/test07", $port, 1), "[!] skipped: delete continuous query cq2 on database test\n"               => "Don't execute delete statements without --force";

    run_updater($curdir, "$schemas_dir/test07", $port, 0, '--force');
    is run_updater($curdir, "$schemas_dir/test07", $port, 0, '--diff'), ''         => 'CQ is deleted with --force';

    # test the order of updates
    is run_updater($curdir, "$schemas_dir/test08", $port, 0, '--diff', '--force'), qq{DROP CONTINUOUS QUERY "cq1" ON "test";\nDROP DATABASE "test";\nCREATE DATABASE "test2";\nCREATE RETENTION POLICY "rp1" ON "test2" DURATION 100d REPLICATION 1 SHARD DURATION 2w;\nCREATE RETENTION POLICY "rp2" ON "test2" DURATION 260w REPLICATION 1 SHARD DURATION 12w DEFAULT;\nCREATE CONTINUOUS QUERY cq1 ON test2 RESAMPLE EVERY 5m FOR 10m BEGIN SELECT LAST(a) AS b, c INTO test2.rp2.m FROM test2.rp1.m GROUP BY time(5m) END;\n}

                                                                                => 'Updates applied in the right order';

    is run_updater($curdir, "$schemas_dir/test00", $port, 0, '--diff'), qq{-- DROP CONTINUOUS QUERY "cq1" ON "test";\n-- DROP DATABASE "test";\n}
                                                                                => 'Old database is detected';
    run_updater($curdir, "$schemas_dir/test00", $port, 1);
    is run_updater($curdir, "$schemas_dir/test00", $port, 0, '--diff'), qq{-- DROP CONTINUOUS QUERY "cq1" ON "test";\n-- DROP DATABASE "test";\n}
                                                                                => 'Database is not deleted without --force';

    run_updater($curdir, "$schemas_dir/test00", $port, 0, '--force');
    is run_updater($curdir, "$schemas_dir/test00", $port, 0, '--diff'), ''         => 'Database is deleted with --force';


    # Exit with error when a database is created a second time
    run_updater($curdir, "$schemas_dir/test10", $port, 255, '--diff');

    ($pid, $tmpdir_handle) = restart_db($pid);
    is run_updater($curdir, "$schemas_dir/test12", $port, 0, '--diff'), ''         => 'Comments are ignored';

    is run_updater($curdir, "$schemas_dir/test13", $port, 0, '--diff'), qq{CREATE DATABASE "test1";\nCREATE DATABASE "test2";\nCREATE DATABASE "test3";\n}      => 'Multiple config files are handled properly';

    run_updater($curdir, "$schemas_dir/test02", $port, 0);
    is run_updater($curdir, "$schemas_dir/test02", $port, 0, '--diff'), ''         => 'Running the updater a second time for the same config does nothing (regression LAKE-338)';

    # Test for handling of name with dots (bugfix while working on MON-2086)
    ($pid, $tmpdir_handle) = restart_db($pid);
    is run_updater($curdir, "$schemas_dir/test_name_with_dot", $port, 0, '--diff'), qq{CREATE DATABASE "db.test";\nCREATE RETENTION POLICY "rp.test" ON "db.test" DURATION 260w REPLICATION 1 SHARD DURATION 12w DEFAULT;\n}
                                                                                => 'CQ change is detected';
    run_updater($curdir, "$schemas_dir/test_name_with_dot", $port, 0);
    is run_updater($curdir, "$schemas_dir/test_name_with_dot", $port, 0, '--diff'), ''         => 'CQ is updated';


    done_testing();

    kill 'KILL', $pid;
}


#
# Starts an InfluxDB instance, used to run the tests against it.
#
# Arguments:
#     -
#
# Returns:
#     $pid int: the pid of the InfluxDB process started
#     $tmpdir_handle: a file handle pointing to a tmp directory where the DB config file is saved.
#     This is returned to make sure that there is always a reference to it, otherwise the GC might delete it.
#
sub start_db {
    my $tmpdir_handle = File::Temp->newdir(CLEANUP => 1);
    my $tmpdir = $tmpdir_handle->dirname();
    my $conf = get_test_conf($tmpdir, $port);
    write_text("$tmpdir/influx.conf", $conf);

    my $pid;
    defined($pid = fork()) or die "unable to fork: $!\n";
    if ($pid == 0) {
        exec("influxd -config $tmpdir/influx.conf");
        warn "unable to exec 'influxd -config $tmpdir/influx.conf': $!\n";
        exit 1;
    }

    sleep 1; # wait for influxdb to start
    return ($pid, $tmpdir_handle);
}


#
# Kills the running InfluxDB instance and start a new. In this way a test can start with a clean state of InfluxDB.
# Note that the pid must be updated in the caller, so that it can be killed when the tests are finished (i.e. the pid returned from this function)
#
# Arguments:
#     $old_pid int: the pid of the currently running InfluxDB
#
# Returns:
#     $pid int: the pid of the new InfluxDB
#     $tmpdir_handle: the file handler for the Influx config directory. We return it so that the file has always a reference to it, otherwise GC might delete it.
#
sub restart_db {
    my ($old_pid) = @_;

    kill 'KILL', $old_pid;
    my ($pid, $tmpdir_handle) = start_db();
    return ($pid, $tmpdir_handle);
}


#
# Runs the update script and optionally tests that the exit code is as expected.
# If the exit code argument is 0 then it does not test it.
# In this way, we distinguish the cases that test the std out and the cases that test the exit code.
# Without this, extra tests would appear in 'prove', showing that we test the exit code in each existing test (where we actually test the std out).
#
# Arguments:
#     $curdir string: the current directory from where the script is run
#     $schema_dir string: the name of the directory with the config files
#     $port string: the port where Influx is running
#     $exit_code int: the expected exit code of the update script
#     @flags strings: the flags to be passed in the updater call
#
sub run_updater {
    my ($curdir, $schema_dir, $port, $exit_code, @flags) = @_;
    my @cmd = ("$curdir/../influxdb-schema-updater", '--config', $schema_dir, '--url', "localhost:$port", @flags);
    my $output = run_cmd(@cmd);
    is $? >> 8, $exit_code, "expected exit code for @cmd";

    return $output

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
