#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

def get_key_value(line):
    words = line.split()

    # 3. Get the key and the value and return them
    key = words[0]
    value = words[1]
    return key, value

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    dict = {}
    for line in input_stream:
        word = line.split()
        if word[0][:2] in languages and (len(word[0]) == 2 or word[0][2] == "."):
            if word[0] not in dict:
                dict[word[0]] = []
                for x in range(0, num_top_entries):
                    dict[word[0]].append(("empty", 0))
            if int(dict[word[0]][-1][1]) < int(word[2]):
                dict[word[0]][-1] = (word[1], word[2])
                dict[word[0]].sort(key=lambda x: int(x[1]), reverse=True)

    for d in dict:
        for tup in dict[d]:
            if int(tup[1]) != 0:
                output_stream.write(d + "\t(" + tup[0] + ", " + tup[1] + ")\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
