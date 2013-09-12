package AnyEvent::PacketForwarder;

use strict;
use warnings;

our $VERSION = '0.01';

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(packet_forwarder);

use AnyEvent;
use AnyEvent::PacketReader;
use Errno qw(EPIPE EMSGSIZE EINTR EAGAIN EWOULDBLOCK ENODATA);
use Carp;
our @CARP_NOT = qw(AnyEvent::PacketReader);

our $QUEUE_SIZE = 10;

sub packet_forwarder {
    my $cb = pop;
    my ($in, $out, $queue_size, $templ, $max_load_length) = @_;
    $queue_size ||= $QUEUE_SIZE;

    # data is:   0:reader, 1:out, 2:queue_size, 3:queue, 4:cb, 5:out_watcher
    my $data = [ undef   , $out , $queue_size , []     , $cb , undef         ];
    $data->[0] = packet_reader $in, $templ, $max_load_length, sub { _packet($_[0], $data) };

    my $obj = \\$data;
    bless $obj;
}

sub _packet {
    my $data = shift;
    if (defined $_[0]) {
        if ($data->[4]->($_[0])) {
            my $queue = $data->[3];
            push @$queue, $_[0];
            $data->[0]->pause if @$queue >= $data->[2];
            $data->[5] ||= AE::io $data->[1], 1, sub { _write($data) };
        }
        return;
    }
    $data->[4]->();
    _fatal_write($data, ENODATA) unless defined $data->[5];
    undef $data->[0];
}

sub _write {
    my $data = shift;
    my $queue = $data->[3];
    while (@$queue) {
        unless (length $queue->[0]) {
            $data->[0]->resume if @$queue == $data->[2];
            shift @$queue;
            next;
        }

        my $bytes = syswrite($data->[1], $queue->[0]);
        if ($bytes) {
            substr($queue->[0], 0, $bytes, '');
        }
        elsif (defined $bytes) {
            _fatal_write($data, EPIPE);
        }
        else {
            $! == $_ and return for (EINTR, EAGAIN, EWOULDBLOCK);
            _fatal_write($data);
        }
        return;
    }
    unless (defined $data->[0]) {
        return _fatal_write($data, ENODATA);
    }
    undef $data->[5];
}

sub _fatal_write {
    my $data = shift;
    local $! = shift if @_;
    $data->[4]->(undef, 1);
}

1;
__END__


=head1 NAME

AnyEvent::PacketForwarder - Perl extension for blah blah blah

=head1 SYNOPSIS

  use AnyEvent::PacketForwarder;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for AnyEvent::PacketForwarder, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Salvador Fandino, E<lt>salva@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013 by Salvador Fandino

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.14.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
