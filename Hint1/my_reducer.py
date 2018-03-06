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
import time



# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    temp = [line.split() for line in input_stream]
    #unique = set(x[0] for x in temp)
    unique, num = ([] for x in range(2))
    for x in range (0, len(temp)):
        if temp[x][0] not in unique:
            unique.append(temp[x][0])
            num.append(x)
    num.append(len(temp))
    #for word in unique:
    for x in range(1, len(num)):
        #ls = sorted([y for y in temp if word == y[0]], key=lambda lines: int(lines[2][:-1]), reverse=True)
        ls = sorted(temp[num[x-1]:num[x]], key=lambda j: int(j[2][:-1]),reverse=True)
        for y in range(min(len(ls), num_top_entries)):
            output_stream.write(ls[y][0]+"\t"+ls[y][1]+ls[y][2]+"\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, num_top_entries):
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
    my_reduce(my_input_stream, num_top_entries, my_output_stream)

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

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    num_top_entries = 5
    s = time.time()
    my_main(debug, i_file_name, o_file_name, num_top_entries)
    print(time.time() - s)