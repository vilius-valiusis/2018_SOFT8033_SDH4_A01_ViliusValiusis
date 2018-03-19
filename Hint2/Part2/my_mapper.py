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

def build_output_file(map_results,output_stream):
    total = 0;
    for key,value in map_results.items():
        total += value
        output_stream.write(key + "\t" + str(value) + "\n")
    return

def extract_information_from_line(line):
    parts = line.split(' ')
    indentifiers = parts[0].split('.')
    language = indentifiers[0]
    try:
        total_views = int(parts[2])
    except (NameError,UnicodeEncodeError):
        print('Failed value convertion to decimal. Skipping entry.')

    if len(indentifiers) > 1:
        project = indentifiers[1]
    else:
        project = 'wikipedia'
    return (language,project,total_views)

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    map_results = dict()

    for line in input_stream:
        language,project,total_views = extract_information_from_line(line)
        if per_language_or_project:
            if language in map_results:
                map_results[language] += total_views
            else:
                map_results[language] = total_views
        else:
            if project in map_results :
                map_results[project] += total_views
            else:
                map_results[project] = total_views
    build_output_file(map_results,output_stream)
    pass

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)

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

    per_language_or_project = False # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
