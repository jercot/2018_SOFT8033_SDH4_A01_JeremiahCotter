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
import heapq


# Got this function from https://ragrawal.wordpress.com/2015/08/25/pyspark-top-n-records-in-each-group/
def take_ordered_by_key(self, num, sortValue=None, reverse=False):
    def init(a):
        return [a]

    def combine(agg, a):
        agg.append(a)
        return agg

    def merge(a, b):
        agg = a + b
        return getTopN(agg)

    def getTopN(agg):
        if reverse:
            return heapq.nlargest(num, agg, sortValue)
        else:
            return heapq.nsmallest(num, agg, sortValue)

    return self.combineByKey(init, combine, merge)


def filter_func(line, lang):
    if line.split()[0].split('.')[0] not in lang:
        return False
    return True


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    from pyspark.rdd import RDD
    RDD.takeOrderedByKey = take_ordered_by_key

    inputRDD = sc.textFile("/FileStore/tables/my_dataset/")
    filterRDD = inputRDD.filter(lambda x: filter_func(x, languages))
    mapRDD = filterRDD.map(lambda x: (x.split()[0], (x.split()[0], x.split()[1], x.split()[2])))
    topRDD = mapRDD.takeOrderedByKey(num_top_entries, sortValue=lambda x: int(x[2]), reverse=True).flatMap(
        lambda x: x[1])
    solutionRDD = topRDD.map(lambda x: (x[0], (x[1], x[2]))).sortByKey(ascending=True)
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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
