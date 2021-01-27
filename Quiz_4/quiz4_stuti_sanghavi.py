# Importing the required packages
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from pyspark.sql import functions as sf

#Creating a spark session 
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#Reducing the verbosity of SPARK
sc.setLogLevel("WARN")

#Displaying the spark submit inputs
print ("###################################################################")
print ("This is the name of the script: ", sys.argv[0])
print ("N: " , str(sys.argv[1]))

#Saving the N
N = int(sys.argv[1])

#Get the input path
input_path = sys.argv[2]
print("input_path: ", input_path)

#Importing raw data into a Dataframe
df_whitehouse = spark.read.csv(input_path, header=True, inferSchema=True)

#Checking the imported data
print ("###################################################################")
print("Checking the input file records")
print(df_whitehouse.show(1))

#Data Clean up
print ("###################################################################")
print("Data Cleaning")

#Step 1 : Keep only the Required Columns - NAMEFIRST, NAMELAST, visitee_namefirst, visitee_namelast
print ("###################################################################")
print("Getting only the required columns from input file")
df_req = df_whitehouse.select('NAMEFIRST', 'NAMELAST', 'visitee_namefirst', 'visitee_namelast')
df_req.show(2)

# Step 2: Dropping the rows with null NAMELAST or visitee_namelast
df_non_null = df_req.filter(df_req.NAMELAST.isNotNull() & df_req.visitee_namelast.isNotNull())


# Step 3: Converting all data and column names to lower case 

#Creating a new dataframe
df_lower = df_non_null

#Getting all the column names
cols = df_lower.columns

#Creating a for loop to convert all the columns to lower case
for col_name in cols:
        df_lower = df_lower.withColumn(col_name, lower(col(col_name)))

#Display data
print ("###################################################################")
print("Printing lowercase data")
df_lower.show(2)

######Step 4: If a record is empty, then drop it
df_no_na = df_lower.na.drop(how='all')

#Rename df_lower
df = df_no_na

#Finding the most frequent visitors
print ("###################################################################")
print("Part A : The " + str(N) + " most frequent visitors to the White House")

#Creating a new column called visitor having the format (NAMELAST, NAMEFIRST)
df = df.withColumn('visitor', sf.concat(sf.col('NAMELAST'),sf.lit(', '), sf.col('NAMEFIRST')))

#Grouping the newly created visitor column to find the frequency of visitors in descending order 
df.select('visitor').groupby('visitor').\
count().orderBy('count', ascending=False).show(N)

#The most frequently visited people in the White House.(visitee_namelast, visitee_namefirst) 
print ("###################################################################")
print("Part B : The " + str(N) + " most frequently visited people in the White House")

#Creating a new column called visitee having the format (visitee_namelast, visitee_namefirst)
df = df.withColumn('visitee', sf.concat(sf.col('visitee_namelast'),sf.lit(', '), sf.col('visitee_namefirst')))

#Grouping the newly created visitee column to find the frequency of visitee in descending order 
df.select('visitee').groupby('visitee').\
count().orderBy('count', ascending=False).show(N)

#The most frequent visitor-visitee  combinations.
print ("###################################################################")
print("Part C : The " + str(N) + " most frequent visitor-visitee combinations")
# Grouping by visitor-visitee combination to find the frequency
df.groupby('visitor','visitee').\
count().orderBy('count', ascending=False).show(N)

# Getting the count for number of initial and final records
initial_records = df_req.count()
final_records = df.count()

#The number of records dropped (due to filtering)
print ("###################################################################")
print("Part D : The number of records dropped (due to filtering)")
print('Number of records dropped = ' + str(initial_records-final_records))

#The number of records processed
print ("###################################################################")
print("Part E : The number of records processed")
print('Number of records processed = ' + str(final_records))