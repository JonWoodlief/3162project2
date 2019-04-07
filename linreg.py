# linreg.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv
#
#

import sys
import numpy as np

from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])

  a = yxinputFile.map(lambda line: line.split(',')).map(lambda r:float(r[1])*float(r[1]))
  b = yxinputFile.map(lambda line: line.split(',')).map(lambda r:float(r[0])*float(r[1]))

  aSum = a.reduce(lambda a, b:a+b)
  bSum = b.reduce(lambda a, b:a+b)

  result = aSum**-1 * bSum

  print "xxSum"
  print aSum
  print "xySum"
  print bSum
  print "beta"
  print result

  # dummy floating point array for beta to illustrate desired output format

  #
  # Add your code here to compute the array of 
  # linear regression coefficients beta.
  # You may also modify the above code if needed.
  #
 
 #xy = sc.textFile(sys.argv[1]).map(lambda line: line.split(','))

  # print the linear regression coefficients in desired output format print "beta: "

  sc.stop()
