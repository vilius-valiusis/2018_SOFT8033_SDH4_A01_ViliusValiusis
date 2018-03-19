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
    parts = line.split('\t')
    origin = parts[0]
    res = parts[1].split(' : ')
    title = res[0]
    total_views = res[1]

    return origin,title,total_views

# ------------------------------------------
# FUNCTION build_top_entries_dict
# ------------------------------------------
def build_top_entries_dict(list_sorted_results,num_top_entries):
    top_entries = dict()
    count_entries = dict()

    for key,value in list_sorted_results:
        if key[0] not in count_entries:
            count_entries[key[0]] = 1
            top_entries[key[0],key[1]] = value
        elif count_entries[key[0]] < num_top_entries:
            count_entries[key[0]] += 1
            top_entries[key[0],key[1]] = value


    return top_entries
# ------------------------------------------
# FUNCTION build_output_file
# ------------------------------------------
def build_output_file(top_entries,output_stream):
    for key,value in top_entries:
        res = key[0] + "\t(" +  key[1] + "," + str(value) + ")\n"
        output_stream.write(res)
    return
# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, num_top_entries, output_stream):
    map_results = dict()
    for line in input_stream:
        origin,title,total_views = extract_information_from_line(line)
        if (origin,title) in map_results:
            map_results[orgin,title] += int(total_views)
        else:
            map_results[origin,title] = int(total_views)

    list_sorted_results = sort_by_views(map_results)
    top_entries = build_top_entries_dict(list_sorted_results,num_top_entries)
    top_entries = sort_by_views(top_entries)
    print(len(top_entries))
    build_output_file(top_entries,output_stream)
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
