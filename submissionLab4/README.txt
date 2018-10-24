# MapReduce
Submission for ENSC 351 Lab 4

Group members:
	Nicolas Klaassen
	Galen Elfert
	Diane Wolf

to build, run `make` from this directory.

the output is a binary called "wordcount", which runs the MapReduce wordcount algorithm from Lab 3, with Moby-Dick.txt as input.

The MapReduce implementation is in MapReducer.hpp, it has been updated to use OpenMP rather than the C++11 threading library.
