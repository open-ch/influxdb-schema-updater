requires 'InfluxDB::HTTP', '>=0.02';
requires 'Getopt::Long', '2.00';
requires 'Pod::Usage', '1.69';
requires 'IPC::Run', '0.99';
requires 'JSON::MaybeXS', '1.0';
requires 'File::Slurper', '0.012';
requires 'List::Util', '1.50';
requires 'Method::Signatures', '20170211';
requires 'Object::Result', '0.000003';
requires 'LWP::UserAgent', '6.25';
requires 'Any::Moose', '== 0.26';

on 'test' => sub {
    requires 'Test::More', '>= 0.96';
    requires 'File::Temp', '0.2304';
    requires 'File::Spec', '3.60';
}
