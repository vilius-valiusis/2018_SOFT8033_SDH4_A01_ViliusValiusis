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

import codecs
import sys

def build_output_file(mapOrigin,mapResult,output_stream):
    for key,value in mapResult.items():
        origin = mapOrigin[key]
        output_stream.write(origin + "\t(" +  key + " , " + str(value) + ")\n")
    return


# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream):
    mapOrigin = dict()
    mapResult = dict()
    for line in my_input_stream:
        parts = line.split()
        origin = parts[0]
        title = parts[1]
        try:
            total_views = int(parts[2])
        except (NameError,UnicodeEncodeError):
            print('Failed value convertion to decimal. Skipping entry.')
            continue

        if title in mapResult:
            mapResult[title] += total_views
        else:
            mapResult[title] = total_views
        mapOrigin[title] = origin

    build_output_file(mapOrigin,mapResult,my_output_stream)
    return

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name):
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
    my_map(my_input_stream, my_output_stream)

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

    i_file_name = "pageviews-20180219-100000_1.txt"
    o_file_name = "mapResult.txt"

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name)
