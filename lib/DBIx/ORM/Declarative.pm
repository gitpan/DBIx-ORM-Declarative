package DBIx::ORM::Declarative;

use strict;
use Carp;

use vars qw($VERSION);
$VERSION = '0.10';

# How this works:
# 1)  There are three subclasses - DBIx::ORM::Declarative::Schema,
#     DBIx::ORM::Declarative::Table, and DBIx::ORM::Declarative::Row.  Thanks
#     to the miracle of Perl inheritence, the class heirarchy is
#     DBIx::ORM::Declarative::Row is-a DBIx::ORM::Declarative::Table is-a
#     DBIx::ORM::Declarative::Schema is-a DBIx::ORM::Declarative
# 2)  A particular schema also forms its own class -
#     DBIx::ORM::Declarative::Schema::$schema.  Similar remarks apply for the
#     other classes.  However, they are based on the schema class directly
#     For example, a schema named Schema1 with a table named Table1
#     with a column named Column1 will have the following class heirarchy:
#     DBIx::ORM::Declarative::Schema::Schema1::Table1::Row is-a
#     DBIx::ORM::Declarative::Row and-a
#     DBIx::ORM::Declarative::Schema::Schema1::Table1 is-a
#     DBIx::ORM::Declarative::Table and-a 
#     DBIx::ORM::Declarative::Schema::Schema1 is-a
#     DBIx::ORM::Declarative::Schema
#     The first parent lets it find the object-specific methods.
#     The second parent lets it find its schema-specific data methods.

@DBIx::ORM::Declarative::Schema::ISA = qw(DBIx::ORM::Declarative);
@DBIx::ORM::Declarative::Table::ISA = qw(DBIx::ORM::Declarative::Schema);
@DBIx::ORM::Declarative::Row::ISA = qw(DBIx::ORM::Declarative::Table);

use constant BASE_CLASS   => 'DBIx::ORM::Declarative';
use constant SCHEMA_CLASS => 'DBIx::ORM::Declarative::Schema';
use constant TABLE_CLASS  => 'DBIx::ORM::Declarative::Table';
use constant ROW_CLASS    => 'DBIx::ORM::Declarative::Row';

# This applies a method by name - necessary for perl < 5.6
sub apply_method
{
    my ($obj, $method, $wantarray, @args) = @_;
    my $res = UNIVERSAL::can($obj, $method);
    if($res)
    {
        return $wantarray?($res->($obj, @args)):scalar($res->($obj, @args));
    }
    $res = UNIVERSAL::can($obj, 'AUTOLOAD');
    if($res)
    {
        # We can't directly use the result in $res, because we need to know
        # which AUTOLOAD it found.  Just use eval for now.  *sigh*.
        my @rv;
        if($wantarray)
        {
            eval "\@rv = \$obj->$method(\@args)";
        }
        else
        {
            eval "\$rv[0] = \$obj->$method(\@args)";
        }
        carp $@ if $@;
        return $wantarray?@rv:$rv[0];
    }
    my $class = ref $obj || $obj;
    carp qq(Can't locate object method "$method" via package "$class");
}

# Create a new DBIx::ORM::Declarative object
# Accepts args as a hash
# Recognized args are "handle" and "debug"
# Unrecognized args are ignored
# If used as an object method, duplicates the underlying object
sub new
{
    my ($self, %args) = @_;
    my $class = ref $self || $self;
    my $handle = delete $args{handle} || $self->handle;
    my $debug = delete $args{debug} || $self->debug_level || 0;
    my $rv = bless { __handle => $handle, __debug_level => $debug }, $class;
    return $rv;
}

# Use this to supress warnings
use constant w__noop => sub { };

# Custom import method to create schemas during the "use" clause.
sub import
{
    my ($package, @args) = @_;
    unless(ref $args[0])
    {
        $package->schema(@args);
        return;
    }
    for my $arg (@args)
    {
        unless(ref $arg)
        {
            carp "Can't import '$arg' in '$package'";
            next;
        }
        $package->schema(%$arg);
    }
}

# Get or set the DBI handle
sub handle
{
    my $self = shift;
    return unless ref $self;
    if(@_)
    {
        delete $self->{__handle};
        $self->{__handle} = $_[0] if $_[0];
        return $self;
    }
    return unless exists $self->{__handle};
    return $self->{__handle};
}

# Get or set the debug level
sub debug_level
{
    my $self = shift;
    return unless ref $self;
    if(@_)
    {
        $self->{__debug_level} = $_[0] || 0;
        return $self;
    }
    return $self->{__debug_level};
}

# Get the current schema name, or switch to a new schema, or create a
# new schema class.
sub schema
{
    my ($self, @args) = @_;
    unless(@args>1)
    {
        return unless ref $self;
        if(@args==1)
        {
            my $schema = shift @args;
            return $self->apply_method($schema,wantarray) if $schema;
            return $self;
        }
        my $schema;
        eval { $schema = $self->_schema; };
        return $schema;
    }
    my %args = @args;
    my $schema = delete $args{schema};
    my $tables = delete $args{tables};
    my $limit_clause = delete $args{limit_clause} || "LIMIT %offset%,%count%";
    carp "missing schema argument" and return unless $schema;
    my $schema_class = $self->SCHEMA_CLASS . "::$schema";
    {
        no strict 'refs';
        unless(@{$schema_class . '::ISA'})
        {
            @{$schema_class . '::ISA'} = ($self->SCHEMA_CLASS);
            *{$schema_class . '::_schema' } = sub { $schema; };
            *{$schema_class . '::_class' } = sub { $schema_class; };
            *{$schema_class . '::_limit_clause' } = sub { $limit_clause; };
            *{$self->BASE_CLASS . "::$schema"} = sub
            {
                my ($self) = @_;
                my $rv = $self->new(schema => $schema);
                bless $rv, $schema_class unless $rv->isa($schema_class);
                return $rv;
            } ;
        }
        for my $tab (@$tables)
        {
            $schema_class->table(%$tab);
        }
        return &{$self->BASE_CLASS . "::$schema"}($self);
    }
}

package DBIx::ORM::Declarative::Schema;
use Carp;

# Get the current table name, or switch to a new table, or create a new
# table class
sub table
{
    my ($self, @args) = @_;
    unless(@args>1)
    {
        return unless ref $self;
        if(@args==1)
        {
            my $table = shift @args;
            return $self->apply_method($table, wantarray) if $table;
            return $self;
        }
        my $table;
        eval { $table = $self->_table; };
        return $table;
    }
    my %args = @args;
    my $table = delete $args{table};
    carp "missing table argument" and return unless $table;
    my $name = delete $args{alias} || $table;
    my $primary = delete $args{primary};
    my $unique = delete $args{unique};
    my $columns = delete $args{columns};
    carp "missing column definitions" and return unless
        $primary or $unique or $columns;
    my $onpnull = delete $args{for_null_primary};
    my $selonpnull = delete $args{select_null_primary};
    my $join = delete $args{join_clause};
    my $group_by = delete $args{group_by};
    my $super = $self->_class;
    my $table_class = $super . "::$name";
    my $row_class = $table_class . "::Rows";
    my $schema = $self->_schema;
    {
        no strict 'refs';
        @{$table_class . '::ISA'} = ($super, $self->TABLE_CLASS);
        @{$row_class . '::ISA'} = ($self->ROW_CLASS, $table_class);
        *{$table_class . '::_class'} = sub { $table_class; };
        *{$table_class . '::_row_class'} = sub { $row_class; };
        *{$table_class . '::_table'} = sub { $name; };
        *{$table_class . '::_sql_name'} = sub { $table; };
        *{$table_class . '::_for_null_primary'} = sub { $onpnull; };
        *{$table_class . '::_select_null_primary'} = sub { $selonpnull; };
        *{$table_class . '::_join_clause'} = sub { $join; };
        if($group_by)
        {
            my @p = @$group_by;
            *{$table_class . '::_group_by' } = sub { @p; };
        }
        else
        {
            *{$table_class . '::_group_by' } = sub { };
        }
        *{$table_class} = sub
        {
            my ($self) = @_;
            my $rv = $self->new(schema => $schema);
            bless $rv, $table_class unless $rv->isa($table_class);
            return $rv;
        } ;
        my %seen_keys;
        my @newcolumns;
        my @p;
        @p = @$primary if $primary;
        *{$table_class . '::_primary_key'} = sub { @p; };
        $seen_keys{$_} =
            { sql_name => $_, name => $_, constraint => 'isstring' }
            foreach @p;
        my %pk = map {($_,1);} @p;
        @newcolumns = @p;
        my @uniqs = ([@p]);
        for my $un (@$unique)
        {
            my %kv = map {($_,1)} @$un;
            delete @kv{@p};
            next if not %kv and scalar(@p) == scalar(@$un);
            push @uniqs, [ @$un ];
            for my $k (@$un)
            {
                next if $seen_keys{$k};
                $seen_keys{$k} = { sql_name => $k, name => $k,
                    constraint => 'isnullablestring' };
                push @newcolumns, $k;
            }
        }
        *{$table_class . '::_unique_keys' } = sub { @uniqs; };
        my @coldefs;
        my %colmap;
        for my $col (@$columns)
        {
            my %cdef = %$col;
            my $sql_name = delete $cdef{name};
            my $name = delete $cdef{alias} || $sql_name;
            $colmap{$name} = $sql_name;
            delete $seen_keys{$sql_name};
            my $constraint = delete $cdef{constraint};
            my $match = delete $cdef{matches};
            my $type = delete $cdef{type};
            unless($constraint)
            {
                $constraint = 'isnullablestring';
                if($match)
                {
                    $constraint = sub
                    {
                        my ($self, $value) = @_;
                        $value =~ /$match/;
                    };
                }
                elsif($type)
                {
                    if($type eq 'number') { $constraint = 'isnumber'; }
                    elsif($type eq 'string') { $constraint = 'isstring'; }
                    elsif($type eq 'nullablenumber')
                    {
                        $constraint = 'isnullablenumber';
                    }
                }
            }
            push @coldefs,
            {
                sql_name => $sql_name,
                name => $name,
                constraint => $constraint,

            };
            *{$row_class . "::$name"} = $self->__create_column_accessor(
                $sql_name, $pk{$sql_name});
        }
        for my $col (@newcolumns)
        {
            my $def = delete $seen_keys{$col};
            next unless $def;
            push @coldefs, $def;
            $colmap{$col} = $col;
            *{$row_class . "::$col"} = $self->__create_column_accessor(
                $col, $pk{$col});
        }
        *{$table_class . '::_columns' } = sub { @coldefs; } ;
        *{$table_class . '::_column_map' } = sub { %colmap; } ;
        return &{$table_class}($self);
    }
}

sub __create_column_accessor
{
    my ($self, $name, $flag) = @_;
    if($flag)
    {
        return sub
        {
            my $self = shift;
            carp "$name is not a class method" and return unless ref $self;
            carp "$name is part of the primary key" and return $self if @_;
            return $self->{__data}{$name};
        };
    }
    return sub
    {
        my $self = shift;
        carp "$name is not a class method" and return unless ref $self;
        my $val = $self->{__data}{$name};
        if(@_)
        {
            my $nval = $_[0];
            if(not defined $val or $nval ne $val)
            {
                delete $self->{__data}{$name};
                $self->{__data}{$name} = $nval if $nval;
                $self->{__dirty} = 1;
            }
            return $self;
        }
        return $val;
    };
}

package DBIx::ORM::Declarative::Table;
use Carp;

sub isnumber
{
    my ($self, $value) = @_;
    return unless defined $value;
    $value =~ /^(?:\.\d+)|(?:\d+(?:\.\d+)?)$/;
}

sub isstring
{
    my ($self, $value) = @_;
    defined $value and length $value;
}

sub isnullablenumber
{
    my ($self, $value) = @_;
    return defined $value?$self->isnumber($value):1;
}

sub isnullablestring { 1; }

my %criteriamap =
(
    eq => [ '=', 1 ],
    ne => [ '!=', 1 ],
    gt => [ '>', 1 ],
    lt => [ '<', 1 ],
    ge => [ '>=', 1 ],
    le => [ '<=', 1 ],
    isnull => [ 'IS NULL', 0 ],
    notnull => [ 'IS NOT NULL', 0 ],
    in => [ undef, 'IN' ],
    notin => [ undef, 'NOT IN' ],
    like => [ 'LIKE', 1 ],
    notlike => [ 'NOT LIKE', 1 ],
) ;

my %critexceptions =
(
    'limit by' => 2,
    'order by' => 1,
);

sub __find_limit
{
    my ($self, @criteria) = @_;
    for my $crit (@criteria)
    {
        my @subcrit = @$crit;
        while(@subcrit)
        {
            my $col = shift @subcrit;
            if($col eq 'limit by')
            {
                my ($offset, $count) = @subcrit[0..1];
                my $lc = $self->_limit_clause;
                $lc =~ s/%offset%/$offset/g;
                $lc =~ s/%count%/$count/g;
                return $lc;
            }
            my $cnt = $critexceptions{$col};
            if($cnt)
            {
                while($cnt) { shift @subcrit; $cnt--; }
                next;
            }
            my $op = shift @subcrit;
            shift @subcrit if $criteriamap{$op}[1];
        }
    }
    return '';
}

sub __find_orderby
{
    my ($self, @criteria) = @_;
    for my $crit (@criteria)
    {
        my @subcrit = @$crit;
        while(@subcrit)
        {
            my $col = shift @subcrit;
            if($col eq 'order by')
            {
                return "ORDER BY " . join(',', @{$subcrit[0]});
            }
            my $cnt = $critexceptions{$col};
            if($cnt)
            {
                while($cnt) { shift @subcrit; $cnt--; }
                next;
            }
            my $op = shift @subcrit;
            shift @subcrit if $criteriamap{$op}[1];
        }
    }
    return '';
}

sub __create_where
{
    my ($self, @criteria) = @_;
    my @clauses;
    my @binds;
    my %map = $self->_column_map;
    for my $crit (@criteria)
    {
        my @sclause = ();
        my @subcrit = @$crit;
        while(@subcrit)
        {
            my $col = shift @subcrit;
            my $cnt = $critexceptions{$col};
            if($cnt)
            {
                while($cnt) { shift @subcrit; --$cnt; }
                next;
            }
            if($col eq 'limit')
            {
                shift @subcrit;
                shift @subcrit;
                next;
            }
            my $op = shift @subcrit;
            my $test;
            carp "No such column $col" and return unless $map{$col};
            if(defined $criteriamap{$op}[0])
            {
                if($criteriamap{$op}[1])
                {
                    $test = "$col " . $criteriamap{$op}[0] . ' ';
                    my $val = shift @subcrit;
                    if(ref $val)
                    {
                        $test .= $$val;
                    }
                    else
                    {
                        $test .= '?';
                        push @binds, $val;
                    }
                }
                else
                {
                    $test = "$col " . $criteriamap{$op}[0];
                }
            }
            else
            {
                $test = "$col " . $criteriamap{$op}[1] . ' (';
                my $val = shift @subcrit;
                $test .= join(',',('?')x@$val);
                $test .= ')';
                push @binds, @$val;
            }
            push @sclause, $test;
        }
        push @clauses, join(' AND ', @sclause) if @sclause;
    }
    my $where = join(' OR ', @clauses);
    return ($where, @binds);
}

# Creates one or more items
# Does not return row objects
# Does not validate the input
# Returns an array of undef or 1 values (depending on reported success)
sub create_only
{
    my ($self, @data) = @_;
    my $handle = $self->handle;
    carp "can't create without a database handle" and return unless $handle;
    carp "can't create a row in a JOIN" and return if $self->_join_clause;
    my $table = $self->table;
    carp "can't create a row without a table" and return unless $table;
    my @cols = map { $_->{name}; } $self->_columns;
    my %name2sql = $self->_column_map;
    my @rv = ();
    # We really don't want any warnings...
    local ($SIG{__WARN__}) = $self->w__noop;
    for my $row (@data)
    {
        my @use_cols = grep { $row->{$_}; } @cols;
        my $sql = "INSERT INTO $table (" . join(',', @name2sql{@use_cols})
            . ') VALUES (' . join(',', ('?') x @use_cols) . ')';
        my $sth = $handle->prepare_cached($sql);
        push @rv, undef and next unless $sth;
        my $rc = $sth->execute(@{$row}{@use_cols});
        push @rv, $rc?1:undef;
    }
    $handle->commit;
    return @rv;
}

# Check parameters against the declared constraints and create
# a row in a table, returning the corresponding row object.
sub create
{
    my ($self, %params) = @_;
    my $handle = $self->handle;
    carp "can't create without a database handle" and return unless $handle;
    carp "can't create a row in a JOIN" and return if $self->_join_clause;
    my @cols = $self->_columns;
    my %name2sql = $self->_column_map;
    my %fetch;
    my @pk = $self->_primary_key;
    my (@icols, @ivals, @binds);
    my $npk = 0;
    my %col_cons = map {($_->{name}, $_->{constraint})} @cols;
    if(@pk)
    {
        for my $k (@pk)
        {
            my $v = delete $params{$k};
            $fetch{$k} = $v;
            if(defined $v)
            {
                my $cons = $col_cons{$k};
                unless($self->apply_method($cons, wantarray, $v))
                {
                    carp "column $k constraint failed";
                    return;
                }
                push @icols, $name2sql{$k};
                push @ivals, '?';
                push @binds, $v;
            }
            else
            {
                my $fnp = $self->_for_null_primary;
                if(defined $fnp)
                {
                    push @ivals, $fnp;
                    push @icols, $name2sql{$k};
                }
                $npk = 1;
            }
        }
    }
    else
    {
        my @un = $self->_unique_keys;
        if(@un)
        {
            $fetch{$_} = $params{$_} foreach @{$un[0]};
        }
        else
        {
            %fetch = %params;
        }
    }
    for my $k (grep { exists $params{$_} } map { $_->{name} } @cols)
    {
        my $v = delete $params{$k};
        push @icols, $name2sql{$k};
        my $cons = $col_cons{$k};
        unless($self->apply_method($cons, wantarray, $v))
        {
            carp "column $k constraint failed";
            return;
        }
        if(defined $v)
        {
            push @ivals, '?';
            push @binds, $v;
        }
        else
        {
            push @ivals, 'NULL';
        }
    }
    my $sql = 'INSERT INTO ' . $self->_table . ' (' . join(',', @icols) .
        ') VALUES (' . join(',', @ivals) . ')';
    my $res;
    if(@binds)
    {
        $res = $handle->do($sql, undef, @binds);
    }
    else
    {
        $res = $handle->do($sql);
    }
    carp "Database error: ", $handle->errstr and return unless $res;
    if(@pk and $npk)
    {
        my $np = $self->_select_null_primary;
        if($np)
        {
            my $data = $handle->selectall_arrayref($np);
            unless($data)
            {
                carp "Database error: ", $handle->errstr;
                local ($SIG{__WARN__}) = $self->w__noop;
                $handle->rollback;
                return;
            }
            my $val = $data->[0][0];
            if(defined $val)
            {
                $fetch{$_} = $val
                    foreach
                    grep { !defined $fetch{$_} }
                    keys %fetch;
            }
        }
    }
    # This is in a block because we only want to turn off warnings on commit
    {
        local ($SIG{__WARN__}) = $self->w__noop;
        $handle->commit;
    }
    my @res = $self->search([
        map {($_, defined($fetch{$_})?(eq => $fetch{$_}):('isnull'))}
        keys %fetch ]);
    carp "Can't find inserted data" and return unless @res;
    carp "Dataset is underdetermined - too many matches" and return if @res>1;
    return $res[0];
}

# Delete stuff from the database
sub delete
{
    my ($self, @criteria) = @_;
    my $handle = $self->handle;
    carp "can't delete without a database handle" and return unless $handle;
    carp "can't delete from a JOIN" and return if $self->_join_clause;
    my ($where, @binds) = $self->__create_where(@criteria);
    my $sql = "DELETE FROM " . $self->_sql_name;
    $sql .= " WHERE $where" if $where;
    my $res;
    if(@binds)
    {
        $res = $handle->do($sql, undef, @binds);
    }
    else
    {
        $res = $handle->do($sql);
    }
    unless($res)
    {
        carp "Database error " . $handle->errstr;
        local ($SIG{__WARN__}) = $self->w__noop;
        $handle->rollback;
        return;
    }
    local ($SIG{__WARN__}) = $self->w__noop;
    $handle->commit;
    return $self;
}

# Search the database, return a row object per returned item
sub search
{
    my ($self, @criteria) = @_;
    my $handle = $self->handle;
    carp "can't search without a database handle" and return unless $handle;
    my $table = $self->_sql_name;
    my @columns = $self->_columns;
    my $sql = "SELECT " . join(',',
        map { $_->{sql_name} . ' AS "' . $_->{name} . '"'; } @columns) .
            " FROM $table";
    my $join = $self->_join_clause;
    $sql .= " $join" if $join;
    my ($where, @binds) = $self->__create_where(@criteria);
    $sql .= " WHERE $where" if $where;
    my @g = $self->_group_by;
    $sql .= " GROUP BY " . join(',',@g) if @g;
    my $ord = $self->__find_orderby(@criteria);
    $sql .= " $ord" if $ord;
    my @rv;
    my $limit = $self->__find_limit(@criteria);
    if($limit)
    {
        $sql .= " $limit";
    }
    my $data;
    if(@binds)
    {
        $data = $handle->selectall_arrayref($sql, undef, @binds);
    }
    else
    {
        $data = $handle->selectall_arrayref($sql);
    }
    carp "Database error " . $handle->errstr and return unless $data;
    my @res;
    my $rclass = $self->_row_class;
    $rclass = ref $self if $self->isa($rclass);
    for my $row (@$data)
    {
        my $robj = bless $self->new, $rclass;
        @{$robj->{__data}}{map {$_->{sql_name}} @columns} = @$row;
        push @res, $robj;
    }
    return @res;
}

sub size
{
    my ($self, @criteria) = @_;
    my $handle = $self->handle;
    carp "can't find table size without a database handle" and return
        unless $handle;
    my $table = $self->_sql_name;
    my $sql = "SELECT COUNT(*) FROM $table";
    my $join = $self->_join_clause;
    $sql .= " $join" if $join;
    my ($where, @binds) = $self->__create_where(@criteria);
    $sql .= " WHERE $where" if $where;
    my $data = $handle->selectall_arrayref($sql);
    carp "Database error " . $handle->errstr and return unless $data;
    return $data->[0][0];
}

package DBIx::ORM::Declarative::Row;
use Carp;

sub delete
{
    my ($self) = @_;
    carp "delete: not a class method" and return unless ref $self;
    $self->{__deleted} = 1;
    return $self;
}

sub __create_where
{
    my ($self) = @_;
    carp "not a class method" and return unless ref $self;
    my  @cols = $self->_columns;
    my %name2sql = map {($_->{name}, $_->{sql_name})} @cols;
    my @binds;
    my @wclause;
    my @pkey = $self->_primary_key;
    if(@pkey)
    {
        for my $k (@pkey)
        {
            push @wclause, $name2sql{$k} . "=?";
            push @binds, $self->{__data}{$k};
        }
    }
    else
    {
        my @un = $self->_unique_keys;
        my @colv;
        if(@un)
        {
            @colv = @{$un[0]};
        }
        else
        {
            @colv = map {$_->{name}} @cols;
        }
        for my $k (@colv)
        {
            if(defined($self->{__data}{$k}))
            {
                push @wclause, $name2sql{$k} . "=?";
                push @binds, $self->{__data}{$k};
            }
            else
            {
                push @wclause, $name2sql{$k} . " IS NULL";
            }
        }
    }
    return join(" AND ", @wclause), @binds;
}

sub commit
{
    my ($self) = @_;
    carp "commit: not a class method" and return unless ref $self;
    my $handle = $self->handle;
    carp "can't commit without a database handle" and return unless $handle;
    return $self unless $self->{__deleted} or $self->{__dirty};
    my ($where, @binds) = $self->__create_where;
    my $sql;
    if($self->{__deleted})
    {
        $sql = "DELETE FROM " . $self->_sql_name . " WHERE $where";
    }
    else
    {
        my @keys = $self->_primary_key;
        unless(@keys)
        {
            @keys = @{${$self->_unique_keys}[0]};
        }
        carp "commit: no unique keys" and return unless @keys;
        my @cols = $self->_columns;
        my %name2sql = map {($_->{name}, $_->{sql_name})} @cols;
        $sql = "UPDATE " . $self->_sql_name . " SET ";
        my @terms;
        my @pbinds;
        my %seenkeys = map {($_,1)} @keys;
        for my $k (map {$_->{name}} @cols)
        {
            next if $seenkeys{$k};
            if(defined($self->{__data}{$k}))
            {
                push @terms, $name2sql{$k} . "=?";
                push @pbinds, $self->{__data}{$k};
            }
            else
            {
                push @terms, $name2sql{$k} . "=NULL";
            }
        }
        $sql .= join(',', @terms) . " WHERE $where";
        @binds = (@pbinds, @binds);
    }
    unless($handle->do($sql, undef, @binds))
    {
        carp "Database error: ", $handle->errstr;
        local ($SIG{__WARN__}) = $self->w__noop;
        $handle->rollback;
        return;
    }
    undef $self->{__dirty};
    if($self->{__deleted})
    {
        undef $self->{__data};
        undef $self->{__deleted};
        bless $self, $self->_class;
    }
    local ($SIG{__WARN__}) = $self->w__noop;
    $handle->commit;
    return $self;
}

1;
__END__

=head1 NAME

DBIx::ORM::Declarative - Perl extension for object-oriented database access

=head1 SYNOPSIS

  use DBIx::ORM::Declarative
  (
    {
      schema => $name,
      limit_clause => 'LIMIT %count% OFFSET %offset%',
      tables =>
      [
        {
          table => $tabname,
          primary => [ $key, ... ],
          unique => [ [ $key, ...], ... ],
          columns =>
          [
            {
              name => $key,
              type => $type,
              matches => $re,
              constraint => $sub,
            }, ...
          ],
        }, ...
      ],
    }, ...
  );
  my $db = DBIx::ORM::Declarative->new(handle => $dbh)->schema($schema);
  $db->table(table => $name, primary => [ $key, ... ], ...);
  my $tab = $db->table_name;
  my @res = $tab->search([$key, $op, $data, ...], ... );
  my $ent = $tab->create($key => $value, ...);
  $ent->column_name($value);
  $ent->commit;
  $ent->delete;
  $tab->delete([$key, $op, $data, ...], ...);
  my $len = $tab->size;
  print "Table $name now has $len rows\n";

=head1 ABSTRACT

B<DBIx::ORM::Declarative> encapsulates the creation of database table
classes at compile time.  You declare the properties of table and row
classes with data structures passed to the B<DBIx::ORM::Declarative>
module when the module is used.  You can also add table classes on the
fly.  Best of all, you don't need to know a thing about SQL to create or
use the classes.

=head1 DESCRIPTION

The B<DBIx::ORM::Declarative> class encapsulates common database operations
on tables and provides an object-oriented interface to them.  It provides a
simple way of constructing an object-oriented database framework.  In
particular, the user doesn't need to create base classes and subclasses -
the B<DBIx::ORM::Declarative> class takes care of all that for you.
For most common operations, no SQL knowledge is needed, and even joins and
sequences don't require a lot of SQL to implement.

The class is customized at compile time by presenting a list of schema
declarations in the B<use> declaration for the module.  The schema
declarations are hash references, each of which must have the keys C<schema>
(which declares the name to be used to bind a particular database declaration
to an object) and C<tables> (which declares the details of the tables
themselves).  The key C<limit_clause>, if present, declares a substitution
pattern for use with limiting searches.  By default, it's
C<LIMIT %offset%,%count%>, which is suitable for MySQL.  The example above
is suitable for PostgreSQL.

The value corresponding to the C<tables> key is a reference to an array
of hash references, each of which describes the particulars of a single
table.  The table hashes each support the keys C<table> (which declares
the name of the table, as used by SQL), C<alias> (which declares the
name of the table as used by Perl, if needed), C<primary> (which declares
the components of the primary key), C<unique> (which declares the components
of other unique keys), and C<columns> (which declares the name and
constraints of columns).  Additionally, if your table has a primary key
consisting of a single column, you can provide a stand-in for the primary
key in the case where you provide a null value for the primary key on
creation.  The table hash keys C<for_null_primary> and C<select_null_primary>
control this.  You can also create a virtual table as join between
two or more tables using the C<join_clause> hash key, and you can search
on aggregates by providing a C<group_by> hash key.

If the table name is not a valid Perl identifier, an alias should be given.
This will let you use derived methods to access the table.  You can provide
an alias for table name even if the SQL table name is a valid Perl identifier.
If an alias is provided, the table will be accessed with that name.

The primary key declaration is a reference to an array of strings, each
of which is the SQL name of a column that makes up part of the primary key.
This portion of the table declaration is optional.

The unique keys declaration is a reference to an array of array refs, each
of which contains a list of strings that declare the SQL names of the columns
that make up the unique key.  It is not necessary to replicate the primary
key declaration in the unique keys declaration - it will be copied over as
needed.

The C<for_null_primary> and C<select_null_primary> allow you to use somewhat
arcane features of various databases to derive primary key values.  The
value corresponding to the C<for_null_primary> key is the literal expression
to be used in place of a NULL for a missing or undefined primary key column.
For example, if you are using an Oracle database, and using the sequence
I<SEQKEY> for your primary key value, you would use C<SEQKEY.NEXTVAL>.  If
this key is not present, NULL primary key values won't be included in affected
INSERT statements.

The value of the C<select_null_primary> key provides a literal SQL SELECT
command to fetch the primary key value generated by the C<for_null_primary>.
For the Oracle example, the appropriate value would be:

  SELECT SEQKEY.CURRVAL FROM DUAL

If you are using MySQL with an auto increment primary key, you would not need
to set a value for C<for_null_primary>, and you would use the following
for C<select_null_primary>:

  SELECT LAST_INSERT_ID()

The C<join_clause> key lets you define derived tables by performing a
join.  The value is a literal string that pastes two or more tables together.
For example, if you have tables named C<EMPLOYEE>, C<ADDRESS>, and C<PAYROLL>,
where C<EMPLOYEE> has an C<ADDRESS_ID> column corresponding to C<ADDRESS>'s
primary key, and C<PAYROLL> has an C<EMPLOYEE_ID> column, the appropriate join
clause, table clause, and alias clause would be something like this:

  table => 'PAYROLL',
  alias => 'PAYROLL_EMPLOYEE_ADDRESS',
  join_clause => 'JOIN EMPLOYEE USING (EMPLOYEE_ID) JOIN ADDRESS ' .
                 'USING (ADDRESS_ID)',

This construction requires the use of aliases for table and column names.

The columns declaration is a reference to an array of hash refs, each of
which declares a single column.  The valid keys are C<name> (which provides
the SQL name for the column), C<alias> (which provides the Perl name for
the column, if necessary), C<type> (which is one of I<number>, I<string>,
I<nullablenumber>, or I<nullablestring>), C<matches> (which is a regular
expression that should match whatever is in the column), and C<constraint>
(which is a reference to a function or the name of a method used to validate
the value of the column).

If the column name is not a valid Perl identifier, an alias should be given,
so that derived methods can be used on returned row objects.

Type checking upon setting a column's value will be done in the order
constraint, matches, type - if the C<constraint> key is present, the
corresponding code reference or method will be called to validate any new
column value, or else if the C<matches> key is present, the corresponding
regular expression will be used to vallidate any new column value, or else
if the C<type> key is present, the type will be validated.  Note that a
type of C<nullablestring> implies no validation at all.

The C<constraint> function is called as a method on the table or row object,
and it's given the proposed new value and column names as arguments.  For
example, if you're attempting to set the C<col1> column on a row to the
value C<no surrender>, the validation code would be run as:

  $ent->$validate('no surrender', 'col1');

Any columns in a primary key declaration that aren't in a columns declaration
are added to the end of the columns declaration in the order found in the
primary key declaration, with a type of C<string>.  Any columns in a unique
key declaration that aren't in a columns or primary key declaration are
added after that, with a type of C<nullablestring>.

The C<group_by> key lets you define searches on tables using aggregated
results.  For example, if you have a PAYROLL table with an EMPLOYEE_ID,
CHECK_DT, and CHECK_AMT table, and you want to be able to get the total
amount an employee has been paid in a given period of time, you'd need a
table declaration like this:

  table => 'PAYROLL',
  alias => 'payroll_total_pay',
  group_by => 'EMPLOYEE_ID',
  columns =>
  [
    { name => 'EMPLOYEE_ID', },
    { name => 'SUM(CHECK_AMT)', alias 'total_pay', },
    { name => 'CHECK_DT', },
  ],

=head1 METHODS

=head2 DBIx::ORM::Declarative->new

The new() method creates a new B<DBIx::ORM::Declarative> object.  It takes a hash
of arguments, and expects values for the key C<handle> (which must be a
DBI-compatible object capable of at least performing the selectall_arrayref(),
do(), and commit() methods).

If called as an object method, it will return a copy of the current object,
with suitable overrides depending on its arguments.  Calling new() without
a handle could be used to create an object factory that uses database handles
that are created as needed.

=head2 DBIx::ORM::Declarative->schema

The schema() method can be used to add a new schema at runtime.  It can
be called as either a class or object method.  If called as a class method,
it returns a new object bound to the new schema.  If called as an object
method, it returns a new object bound to the new schema, and using the handle
from the object (if available).  The arguments to the schema() method
are a hash similar to a schema stanza from the B<use DBIx::ORM::Declarative>
method.

=head2 $db->handle()

The handle() method is an accessor/mutator to get or set the database handle.
The accessor variant takes no arguments, and returns the database handle
associated with the schema.  The mutator method takes a single argument,
and returns the schema object itself.  This will let you add/change the
database handle and then immediately create and use a table object.  For
example:

  my @ents = $schema->handle($dbh)->table1->search($searchspec);

For the mutator variant, if the argument is defined, it must be compatible
with a DBI handle.  If the argument is undefined, the current handle is
deleted.

=head1 AUTOGENERATED METHODS

Every schema, table, and column definition corresponds to an autogenerated
method.  The schema methods are valid on any object, the table methods are
valid on associated schema, table and row objects, the column methods are
valid on row objects.  Using a schema method will return a
B<DBIx::ORM::Declarative> subclass object bound to the schema (a "schema
object").  Using a table method will return a B<DBIx::ORM::Declarative> subclass
object bound to the table (a "table object").  Searching or adding data via
a table object returns a list of B<DBIx::ORM::Declarative> subclass objects
bound to a particular row of the corresponding table (the "row objects").

For example, if you have a schema named "schema1" with a table named "table1"
that has a column named "column1", the following are all legal (based on
a properly created B<DBIx::ORM::Declarative> object in C<$db>):

  my $schema = $db->schema1;
  my $table = $schema->table1;
  my $row = $table->create(column1 => $value);
  $row->column1($newvalue);

If any definition provides an alias, only the alias is used for the method
name.  For example, if you have an table named C<table1> with an alias
of C<alias1>, B<DBIx::ORM::Declarative> will only create an alias1() method
(not a table1() method).

=head1 SCHEMA OBJECT METHODS

Schema object methods are also valid for table and row objects.

=head2 $schema->schema()

The schema() method is an accessor/mutator to get or set the schema for
the object.  The accessor method takes no argument and returns the current
schema name.  The mutator method takes a single argument, which is the name
of a schema, and returns the schema object.  Note that the object may need
to be re-bless`ed.

If schema() is passed more than one argument, or the argument is a hash
reference, the class method version is called.  If the argument is undefined,
the current schema name is returned, if available.

=head2 $schema->table()

The table() method lets you add a table definition to a schema at run time.
It takes a hash of arguments, which is similar to a table definition in the
B<use DBIx::ORM::Declarative> stanza.  It returns a table object bound to the
newly defined table type.  The schema object is not changed.

If the table() method is passed a single argument, a new table object bound
to the specified table is returned (if possible).  If the table() method is
passed no arguments, the table bound to the object (if any) is returned.

=head1 TABLE OBJECT METHODS

Table methods may also be used on row objects, but not schema objects.

=head2 $table->search()

The search() method allows you to search for data in a table.  The method
takes as its arguments a list of references to arrays, where each array
contains a series of search criteria.  The criteria consist of a column name
or alias, an operation, and possibly a parameter.  The criteria are put in
the list one after the other.  The conditions in a single criteria array must
all be met for a given row to be returned.  However, if the conditions of any
single set of criteria are met, the row will be returned.  In other words, the
conditions are ANDed within a single array, but ORed between arrays.

Single-value parameters can either be scalars (which are taken to be the
literal value in question) or scalar references (where the referenced scalar
is taken to be an SQL expression).  For example, if you want to look for
a record where the value of C<col1> is twice the value of C<col2>, your
criteria would be:

  col1 => eq => \'2*col2'

The operations are:

=over 4

=item eq

The column must match the value of the parameter (a single value).

=item ne

The column must not match the value of the parameter (a single value).

=item gt

The column must be greater than the value of the parameter (a single value).

=item lt

The column must be less than the value of the parameter (a single value).

=item ge

The column must be greater than or equal to the value of the parameter (a
single value).

=item le

The column must be less than or equal to the value of the parameter (a
single value).

=item isnull

The column must be null.  There is no parameter.

=item notnull

The column must not be null.  There is no parameter.

=item in

The column must have a value that is one of those in the array pointed to
by the reference which is provided as the parameter.

=item notin

The column must have a value that is not one of those in the array pointed to
by the reference which is provided as the parameter.

=item like

The column must have a value that matches the SQL wildcard pattern provided
by the parameter (which can only be a scalar, not a reference).

=item notlike

The column must not have a value that matches the SQL wildcard pattern provided
by the parameter (which can only be a scalar, not a reference).

=back

In addition to search criteria, you can also pass limits for the search by
using the pseudo column name C<limit by>.  The next two items in the list are
the offset and row count for the limit.

In addition to limiting your search to a subset of the results, you can also
fetch the results in a sorted order by using the pseudo column name
C<order by>.  It expects an array reference of column or alias names.

The search() method returns an array of row objects (one per matching row).

=head2 $table->size()

The size() method returns the number of rows in the table (or join).
It accepts most of the same criteria as the search() method, except for
grouping and limit criteria (which are ignored).

=head2 delete()

The delete() method searches for and deletes records from the database based
on the provided search criteria.  The syntax and format is identical to the
search() method.  B<WARNING> - this method autocommits changes; be careful.

=head2 create()

The create() method creates a new database entry, and returns a row object
on success (or nothing on failure).  The method expects a list of column
name - value pairs.  B<WARNING> - this method autocommits changes; be careful.

=head2 create_only()

The create_only() method creates one or more database methods, without
validating its input against the row constraints for the table, and without
returning row objects.  It expects a list of hash references, where the keys
are column names or aliases and the values are the values to be inserted into
the database.  It returns a list of flags, where a true value indicates that
the corresponding hash reference was successfully inserted into the table.

B<WARNING> - this method autocommits changes, and it bypasses row constraint
checking.

=head1 ROW OBJECT METHODS

=head2 delete()

The delete() method marks this entry as to be deleted.  It doesn't immediately
delete the entry from the database.

=head2 commit()

The commit() method writes any changes on the object to the database.  If the
object has been marked for deletion, it will be promoted to an associated
table object by this method.  There is no corresponding rollback() method -
just let the object go out of scope if you don't want to write the changes
out to the database.


=head1 EXAMPLES

First example:  loading B<DBIx::ORM::Declarative> and create a schema:

  use DBIx::ORM::Declarative
  (
    {
      schema => 'Example1',
      tables =>
      [
        {
          table => 'table1',
          primary => [ qw(id) ],
          unique => [ [ qw(val) ] ],
          columns =>
          [
            {
              name => 'id',
              type => 'number',
            },
            {
              name => 'val',
              type => 'string',
            },
          ],
        },
        {
          table => 'table2',
          primary => [ qw(id) ],
          unique => [ [ qw(val) ] ],
          columns =>
          [
            {
              name => 'id',
              type => 'number',
            },
            {
              name => 'val',
              type => 'string',
            },
          ],
        },
        {
          table => 'table1',
          alias => 'table1_table2',
          join_clause => 'JOIN table2 USING (id)',
          columns =>
          [
            { name => 'table1.val', alias => 'table1_val' },
            { name => 'table2.val', alias => 'table2_val' },
          ]
        },
      ],
    },
  );

Second example:  create a table object

  my $db = new DBIx::ORM::Declarative schema => 'Example1', handle => $dbh;
  my $table = $db->table1;

Third example:  search for rows with an C<id> between 17 and 24, and where the
C<val> starts with the string C<closure>:

  my @rows = $table->search([ id => ge => 17, id => le => 24,
                              val => like => 'closure%' ]);

Fourth example:  change the string on the first returned item to "closure is
a myth" and commit the change:

  $rows[0]->val("Closure is a myth")->commit;

=head1 HOW DO I...

=head2 How do I use a view?

A view appears identical to a table from the point of view of a client.
Just describe the columns as usual.

=head2 How do I perform a join?

If your database supports the B<JOIN> operator, you can add a virtual table
with a C<join_clause> to  your schema.  The restrictions on this are that
the table name must correspond to the first table in the join (so you'll
need to use an alias if you want to access that table by itself - either in
the virtual table definition, or in the definition for the table), and
the join can't be implemented via a C<WHERE> clause.  You also can't update,
insert, or delete into or from a join.

Remember that the C<name> keys for the column definitions must be what
SQL expects, so if you have columns with the same name in multiple
different tables, you'll need to provide a complete column specification
(something like C<table.column>), and you'll need to provide an alias if
you want to access it.

If your database doesn't support the B<JOIN> operator, you can emulate by
fetching all of the relevant data and stitching it up yourself.  For
example, if you have two tables like the following:

  +---------------+  +---------------+
  | Table names   |  | Table owing   |
  +------+--------+  +------+--------+
  | Name | Type   |  | Name | Type   |
  +------+--------+  +------+--------+
  | id   | number |  | id   | number |
  +------+--------+  +------+--------+
  | name | string |  | owes | number |
  +------+--------+  +------+--------+

and you want to find the name of everyone who owes more than 25, you'd 
do something like this:

  my @owers = $owing->search([owes => gt => 25]);
  my @pers = $names->search([id => in => [ map { $_->id } @owers ]);
  my @names = map { $_->name } @pers;

More complex joins would be similar, they would just need more steps.
Be aware that a search will return B<every> matching record, so searching
a large table could return a large list.  For efficiency, you want to
arrange your search operations to return the smallest data sets possible.

=head1 CAVEATS

Schema names must be valid Perl identifiers.

The B<DBIx::ORM::Declarative::Schema> namespace and all of its dependent name
spaces can be used by this module.  The rules are that each schema gets a new
namespace dependent on B<DBIx::ORM::Declarative::Schema>, and each table gets
b<two> name spaces dependent on the schema's namespace.

Both commit() and rollback() methods can be called on the handle passed
to a B<DBIx::ORM::Declarative> constructor.  This may disrupt transactions
if you use the handle outside of this class.

The B<DBIx::ORM::Declarative> class will fetch and return ALL of the results
for a search, unless you use a limit clause - queries that return large data
sets will take a long time, and may run you out of memory.

You can't insert, update, or delete using a virtual table created with
a C<join_clause> declaration.  If you need to do that, create a view in
the database and make your changes there.

C<order by> clauses to searches, and the ability to pass in an arbitrary
SQL expression in a search by using a scalar reference, could leave your
application vulnerable to SQL injection attacks - make sure your application
checks external parameters B<before> you pass them to the search() method.

=head1 TESTING

I have elected not to distribute my test suite with this module, since
testing this module depends on having access to a database, and writing
a custom script for that particular database.

To date, the class has only been tested against a MySQL 5.0.17 database.
The generated SQL is fairly generic, however, and I have no reason to
suspect the class won't work with any other SQL database.

=head1 SEE ALSO

L<DBI(3)>

=head1 AUTHOR

Jim Schneider, E<lt>perl@jrcsdevelopment.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Jim Schneider

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.6.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
