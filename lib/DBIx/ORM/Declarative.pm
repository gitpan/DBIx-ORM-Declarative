package DBIx::ORM::Declarative;

use strict;
use Carp;
use DBIx::ORM::Declarative::Schema;
use DBIx::ORM::Declarative::Table;
use DBIx::ORM::Declarative::Join;
use DBIx::ORM::Declarative::Row;
use DBIx::ORM::Declarative::JRow;

use vars qw($VERSION);
$VERSION = '0.14';

use constant BASE_CLASS   => 'DBIx::ORM::Declarative';
use constant SCHEMA_CLASS => 'DBIx::ORM::Declarative::Schema';
use constant TABLE_CLASS  => 'DBIx::ORM::Declarative::Table';
use constant JOIN_CLASS   => 'DBIx::ORM::Declarative::Join';
use constant ROW_CLASS    => 'DBIx::ORM::Declarative::Row';
use constant JROW_CLASS   => 'DBIx::ORM::Declarative::JRow';

# Use this to /really/ supress warnings
use constant w__noop => sub { };

# The error we return when we have an embarassment of riches
use constant E_TOOMANYROWS => 'Database error: underdetermined data set';

# The error we return when we've lost the row we just inserted
use constant E_NOROWSFOUND => 'Database error: inserted data not found';

# This applies a method by name - necessary for perl < 5.6
sub apply_method
{
    my ($obj, $method, $wantarray, @args) = @_;
    # Check to see if we can apply it directly
    my @rv;
    eval
    {
        # We don't need any warnings here
        local $SIG{__WARN__} = __PACKAGE__->w__noop;
        if($wantarray)
        {
            @rv = $obj->$method(@args);
        }
        else
        {
            $rv[0] = $obj->$method(@args);
        }
    } ;
    if(not $@)
    {
        return $wantarray?@rv:$rv[0];
    }
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
# If used as an object method, copy the handle and debug status, if available
sub new
{
    my ($self, %args) = @_;
    my $class = ref $self || $self;
    my $handle = delete $args{handle} || $self->handle;
    my $debug = delete $args{debug} || $self->debug_level || 0;
    my $rv = bless { __handle => $handle, __debug_level => $debug }, $class;
    return $rv;
}

# Custom import method to create schemas during the "use" clause.
sub import
{
    my ($package, @args) = @_;
    if(not ref $args[0])
    {
        $package->schema(@args);
        return;
    }
    for my $arg (@args)
    {
        if(not ref $arg)
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
    return 0 unless ref $self;
    if(@_)
    {
        $self->{__debug_level} = $_[0] || 0;
        return $self;
    }
    return $self->{__debug_level} || 0;
}

# Get the current schema name, or switch to a new schema, or create a
# new schema class.
sub schema
{
    my ($self, @args) = @_;
    if(@args<2)
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

    # Creating/updating a schema class - process the args
    my %args = @args;
    my $schema = delete $args{schema};
    my $from_dual = delete $args{from_dual};
    my $limit_clause = delete $args{limit_clause} || 'LIMIT %offset%,%count%';

    carp "missing schema argument" and return unless $schema;
    my $schema_class = $self->SCHEMA_CLASS . "::$schema";

    # The meat of the declarations
    my $tables = delete $args{tables} || [ ];
    my $joins = delete $args{joins} || [ ];
    my $aliases = delete $args{table_aliases} || { };

    # We're gonna do a whole mess of symbolic references...
    no strict 'refs';
    my $schema_method_name = $self->BASE_CLASS . "::$schema";
    if(not @{$schema_class . '::ISA'})
    {
        # Create the class heirarchy
        @{$schema_class . '::ISA'} = ($self->SCHEMA_CLASS);

        # Information methods
        *{$schema_class . '::_schema' } = sub { $schema; };
        *{$schema_class . '::_schema_class' } =
        *{$schema_class . '::_class' } = sub { $schema_class; };
        *{$schema_class . '::_limit_clause' } = sub { $limit_clause; };
        *{$schema_class . '::_from_dual' } = sub { $from_dual; };

        # A constructor/mutator
        *{$schema_method_name} = sub
        {
            my ($self) = @_;
            my $rv = $self->new(schema => $schema);
            bless $rv, $schema_class unless $rv->isa($schema_class);
            return $rv;
        } ;
    }

    # Create the tables
    $schema_class->table(%$_) foreach @$tables;

    # Create the aliases, if we have any
    for my $alias (keys %$aliases)
    {
        # Get the alias name
        my $table = $aliases->{$alias};

        # Create the class names
        my $alias_class = $schema_class . "::$alias";
        my $table_class = $schema_class . "::$table";

        # Set up the heirarchy
        if(not @{$alias_class . '::ISA'})
        {
            @{$alias_class . '::ISA'} = ($table_class);
            *{$alias_class . '::_class'} = sub { $alias_class; };
            *{$alias_class . '::_table'} = sub { $alias; };
            *{$alias_class} = sub
            {
                my ($self) = @_;
                my $rv = $self->new(schema => $schema);
                bless $rv, $alias_class unless $rv->isa($alias_class);
                return $rv;
            } ;

            # Make sure row objects promote themselves to the alias class
            my $row_class = $alias_class . '::Rows';
            @{$row_class . '::ISA'} = ($self->ROW_CLASS, $alias_class);
            *{$alias_class . '::_row_class'} = sub { $row_class; };
        }
    }
    
    # Create any joins we might have
    $schema_class->join(%$_) foreach @$joins;

    return &{$schema_method_name}($self);
}

1;
