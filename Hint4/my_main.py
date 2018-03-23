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
  
def format_line(line):
  pts = line.split(' ')
  return pts[0],int(pts[2])

def filter_by_type(line,per_language_or_project):
  pts = line[0].split('.')
  if per_language_or_project:
      return pts[0],line[1]
  else:
      if len(pts) < 1:
        return pts[1],line[1]
      else:
        return 'wikipedia',line[1]

def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    inputRDD = sc.textFile(dataset_dir)
    inputRDD.persist()
    # 2. Format all lines to orgin,views format.
    allValuesRDD = inputRDD.map(format_line) 
    allValuesRDD.persist()
    # 3. Take the sum of all the views
    total = allValuesRDD.values().sum()
    # 4. Alter the key based on the value of filter_by_type. True == language, False == Project
    filteredValuesRDD = allValuesRDD.map(lambda x: filter_by_type(x,per_language_or_project))
    # 5. Group all values of the same key
    groupedKeysRDD = filteredValuesRDD.groupByKey()
    # 6. Find and map the sum of values in each key
    mappedValuesRDD = groupedKeysRDD.mapValues(lambda x: sum(list(x)))
    # 7. Find and map the perecentage to each of the keys
    percentageTotalsRDD = mappedValuesRDD.mapValues(lambda x: (x,(float(x) / total * 100)))
    # 8. Perform a descending order sort based on the percentage
    sortedRDD = percentageTotalsRDD.sortBy(lambda x: x[1],False)
    # 9. Save the values to a text file
    sortedRDD.saveAsTextFile(o_file_dir)

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

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)

