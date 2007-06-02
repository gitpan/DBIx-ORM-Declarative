#! /usr/bin/perl

# For use on systems with Module::Build, but not make

use Module::Build;

Module::Build->new
(
    module_name        => 'DBIx::ORM::Declarative',
    license            => 'perl',
    requires           =>
    {
        'DBI'          => 1.30,
        'Scalar::Util' => 1,
    },
)->create_build_script;
