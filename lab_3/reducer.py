#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Input comes from STDIN
for line in sys.stdin:
    # Remove whitespace
    line = line.strip()

    # Parse the input we got from mapper.py
    try:
        word, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        # Ignore lines that are not formatted correctly
        continue

    # This IF-switch works because Hadoop sorts map output by key (word)
    # before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Write result to STDOUT
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# Output the last word if needed
if current_word == word:
    print('%s\t%s' % (current_word, current_count))