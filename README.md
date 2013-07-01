PLA
===

PLA (Personal Learning Analytics) is a system to log the occurrence of events
within a Linux environment used by students. Events can be anything from
executing a program from the command line to compiling a C program.

PLA is meant for students who work with Subversion as their repository system,
although this might change in the future. Event logs are sent as additional
files when the user commits his/her code.

Installation
------------
1. Place the PLA directory in the home directory with name .pladata.
2. Add at the end of .bashrc: source ~/.pladata/bin/plugpla ~/.pladata/bin

