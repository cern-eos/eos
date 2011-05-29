#!/usr/bin/perl
print "[COMMANDS]\n";
open FIN, "eos help |";
while ( <FIN> ) {
    chomp $_;
    my @args = split (" ", $_, 2);
    printf ".in 10\n.B $args[0]\n.in 30\n- $args[1] ( man \n.I eos::$args[0] ) \n.br\n";
}

close IN;
