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
import re
import operator
# ------------------------------------------
# FUNCTION sort_by_views
# ------------------------------------------
def sort_by_views(map_reduced_results):
    #Sorts the map_reduced_results dictionary by value in a descending order.
    return sorted(map_reduced_results.items(), key=operator.itemgetter(1), reverse=True)
# ------------------------------------------
# FUNCTION extract_information_from_line
# ------------------------------------------
def extract_information_from_line(line):
    ent = line.split('\t(')
    origin = ent[0]
    keyval = ent[1].split(' , ')
    title = keyval[0]
    total_views = re.findall(r'\d+',keyval[1])[0]
    return (origin,title,int(total_views))
# ------------------------------------------
# FUNCTION build_output_file
# ------------------------------------------
def build_output_file(map_origin,list_sorted_results,num_top_entries,output_stream):
    entries = 0
    for key,value in list_sorted_results:
        if entries < 5:
            origin = map_origin[key]
            output_stream.write(origin + "\t(" +  key + "," + str(value) + ")\n")
            entries += 1
    return


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    map_reduced_results = dict()
    map_origin = dict()
    for line in input_stream:
        origin,title,total_views = extract_information_from_line(line)
        if title in map_reduced_results:
            map_reduced_results[title] += total_views
        else:
            map_reduced_results[title] = total_views
            map_origin[title] = origin
    list_sorted_results = sort_by_views(map_reduced_results)
    build_output_file(map_origin,list_sorted_results,num_top_entries,output_stream)
    pass

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

    my_main(debug, i_file_name, o_file_name, num_top_entries)
