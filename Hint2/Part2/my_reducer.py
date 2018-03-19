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
from collections import OrderedDict
# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
	map_results = dict()
	map_percent = dict()

	for line in input_stream:
		parts = line.split('\t')
		origin = parts[0]
		count = int(parts[1])
		if origin in map_results:
			map_results[origin] += count
		else:
			map_results[origin] = count

	for key,value in map_results.items():
		map_percent[key] = (float(value) / total_petitions * 100)
	map_results = OrderedDict(sorted(map_results.items(), key=lambda x: x[1], reverse=True))
	for key,val in map_results.items():
		output_stream.write(key + '\t(' + str(val) +', '+ str(map_percent[key]) + '% )\n')
	return


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
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
    my_reduce(my_input_stream, total_petitions, my_output_stream)

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

    # This variable must be computed in the first stage
    total_petitions = 21996621

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
