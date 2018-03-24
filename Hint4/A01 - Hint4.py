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


def process(line, l_p):
    word = line.split()
    key = ()
    temp = word[0];
    if '.' in temp:
        temp = word[0].split('.')[1]
        if per_language_or_project:
            temp = word[0].split('.')[0]
    elif not per_language_or_project:
        temp = "wikipedia"
    try:
        key = (temp, int(word[-2]))
    except:
        key = (temp, int(word[1]))
    return key


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    inputRDD = sc.textFile(dataset_dir)
    inputRDD.persist()
    total = inputRDD.map(lambda x: int(x.split()[-2])).sum()
    mapRDD = inputRDD.map(lambda x: process(x, per_language_or_project))
    eachRDD = mapRDD.combineByKey(lambda value: (value, 1),
                                  lambda x, value: (x[0] + value, x[1] + 1),
                                  lambda x, y: (x[0] + y[0], x[1] + y[1]))
    solutionRDD = eachRDD.map(lambda x: (x[0], (x[1][0], x[1][0] / total * 100)))
    solutionRDD.saveAsTextFile(o_file_dir)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/my_dataset/"
    o_file_dir = "/FileStore/tables/my_result/"

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
