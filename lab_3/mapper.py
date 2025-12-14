#!/usr/bin/env python3
import sys

# Input comes from standard input STDIN
for line in sys.stdin:
    # Remove leading and trailing whitespaces
    line = line.strip()
    
    # Split the line into words
    words = line.split()
    
    # Output tuples [word, 1] in tab-delimited format
    for word in words:
        # Write the results to standard output STDOUT
        print('%s\t%s' % (word, 1))