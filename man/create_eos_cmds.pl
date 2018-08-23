#!/usr/bin/perl

my @excluded_commands = @ARGV;

print "[COMMANDS]\n";
open FIN, "eos help |";

while ( <FIN> ) {
    chomp $_;
    my @args = split (" ", $_, 2);
    printf ".in 10\n.B $args[0]\n.in 30\n- $args[1] ";
    if ( !($args[0] ~~ @excluded_commands) ) {
        printf "( man \n.I eos-$args[0] ) ";
    }
    printf "\n.br\n";
}

close IN;
