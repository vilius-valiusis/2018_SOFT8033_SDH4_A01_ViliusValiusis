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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------

def sort_list(x,num_top_entries):
  return sorted(list(x), key=lambda val:val[1],reverse=True)[:num_top_entries]
  
def format_line(line):
  pts = line.split(' ')
  return pts[0],(pts[1],int(pts[2]))

def filter_by_language(line,languages):
  return any(language in line[0][:2] for language in languages)

def flatten(x): return x

def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    inputRDD = sc.textFile(dataset_dir)
    inputRDD.persist()
    # 2. Format all lines to orgin,(title,views) format.
    # 3. Remove all entries outside of the provided languages.
    # 4. Group entries all entries by origin.
    # 5. Sort entries by origin a-z.
    # 6. Map and sort all values and only return the top five results.
    # 7. Flatten lists approriate to keys e.g (en,[(a,5),(c,3))) becomes (en,(a,5)),(en,(c,3)).
    # 8. Save file to the specified directory
    t = inputRDD.map(format_line) \
                .filter(lambda x: filter_by_language(x,languages)) \
                .groupByKey() \
                .sortByKey() \
                .mapValues(lambda x: sort_list(x,num_top_entries)) \
                .flatMapValues(flatten) \
                .saveAsTextFile(o_file_dir)
            
# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"
    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)

