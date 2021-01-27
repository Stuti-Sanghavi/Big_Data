# Importing the required packages
import sys
from pyspark.sql import SparkSession
from math import sqrt

#Creating a spark session 
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#Reducing the verbosity of SPARK
sc.setLogLevel("WARN")


#Displaying the spark submit inputs
print ("###################################################################")
print ("This is the name of the script: ", sys.argv[0])
print ("Number of arguments: ", len(sys.argv))
print ("The arguments are: " , str(sys.argv))


#Get the input path
input_path = sys.argv[1]
print("input_path: ", input_path)

#Importing raw data into an RDD
rdd = sc.textFile(input_path)

#Checking the imported data
print ("###################################################################")
print("Checking the input file records")
print(rdd.take(5))

#Removing the duplicates
rdd_distinct = rdd.distinct()
print ("###################################################################")
print("After removing the duplicates, number of records in input file: ", rdd_distinct.count())


#Step 1 - Creating Similarity Matrix 
print ("###################################################################")
print("Step 1 - Generating Similarity Matrix")


#-----------------------------------Creating a list of unique users in the data --------------------------------#
users = rdd_distinct.map(lambda x: (x.split(":")[0])).distinct().collect()

############## =Creating a mapper function to generate key value pairs for each record
#Key - Book
#Value - User list with 1 and 0 for purchase or not
#For example (u1:book1) ==> (Book number, user vector) ==> (1, [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0])
def mapper_book_user(x):
    tokens = x.split(":")
    purchase = []
    for user in users:
        if user == tokens[0]:
            purchase.append(1)
        else:
            purchase.append(0)
    return (int(tokens[1][4:]), purchase)


#Create an rdd after applying the above mapper function
rdd_book_vector = rdd_distinct.map(mapper_book_user)

print ("###################################################################")
print("RDD output after applying a mapper function to generate (book_number, user vector)")
print(rdd_book_vector.take(2))


############### Reducer to create one user vector for each book number
def list_add(x,y):
    l = []
    for a,b in zip(x,y):
        l.append(a+b)
    return l

#Create an RDD which has each book and its corresponding aggregated user vector 
rdd_book_vector_f = rdd_book_vector.reduceByKey(lambda a, b: list_add(a, b))

print ("###################################################################")
print("RDD output after applying reducer to create one user vector per book: (book_number, user vector)")
print(rdd_book_vector_f.take(2))


################# Defining function to calculate phi correlation
#Defining a function to calculate phi correlation for two lists
def phi_corr(x, y):
    a = 0
    b = 0
    c = 0
    d = 0
    for i in range(0,len(x)):
        if ((x[i] == y[i]) & (x[i] == 1)):
            a=a+1
        elif ((x[i] == y[i]) & (x[i] == 0)):
            d=d+1
        elif ((x[i] != y[i]) & (x[i] == 1)):
            b=b+1
        else:
            c=c+1
    phi = (a*d - b*c)/sqrt((a+b)*(c+d)*(a+c)*(b+d))

    return phi
    
    
############ Create one sided cartesian product of the book_number with itself
rdd_book_1side_cartesian = rdd_book_vector_f.cartesian(rdd_book_vector_f).filter(lambda x: x[0][0] < x[1][0])

print ("###################################################################")
print("RDD output after taking one sided cartesian product of book vector: ((book_number, user vector),(book_number, user vector))")
print(rdd_book_1side_cartesian.take(1))


############# calculating phi correlation and part 1 output - (Book1, Book2, phi-correlation)
similarity_matrix = rdd_book_1side_cartesian.map(lambda x: (x[0][0], x[1][0], phi_corr(x[0][1], x[1][1])))

print ("###################################################################")
print("Similarity Matrix Output: (book_number, book_number, phi correlation value)")
print(similarity_matrix.sortBy(lambda a: a[0]).collect())


############################# Step 2 - Generating all Recommendations ################################################
print ("###################################################################")
print("Step 2 - Generating all Reccomendations")


#Using flat map to generate all combinations of the books
all_comb = similarity_matrix.flatMap(lambda x: ((x[1],x[0],x[2]),(x[0],x[1],x[2])))

print ("###################################################################")
print("Creating an RDD to create all combinations of the books")
print(all_comb.take(5))


###### Defining a function to select top 2 recommendations
def select_top_2(x):
    dict_x = dict(x)
    a = sorted(dict_x.items() , reverse=True, key=lambda x: x[1])[0][0]
    b = sorted(dict_x.items() , reverse=True, key=lambda x: x[1])[1][0]
    return (a, b)
    
#Create a mapper to generate to create combinations of (book_number, (book_number, phi-correlation))
book_corr_map = all_comb.map(lambda x: (x[0],(x[1],x[2])))

print ("###################################################################")
print("Creating an RDD to create following key value pairs - (book_number, (book_number, phi-correlation))")
print(book_corr_map.take(3))


#Now reduce the above key value pairs by taking the top 2 books from values for each key using the phi-correlation
all_recommendations = book_corr_map.groupByKey().mapValues(lambda x: select_top_2(x))
 
print ("###################################################################")
print("Reducing the above RDD by selecting top 2 books from values for each key using phi-correlation")
print(all_recommendations.take(3))

print ("###################################################################")
print ("Output - ")

for x in all_recommendations.collect():
    print('Customers who bought book'+str(x[0])+' also bought: book'+str(x[1][0])+', book'+str(x[1][1]))