// Databricks notebook source
"raghu"
// This feature is called dynamic type inference
// res2: String = raghu (result2) --- These are system generated variables

// COMMAND ----------

//  scala is very interactive which is similar to python
100+2

// COMMAND ----------

// There are 2 types of variables in scala 
// 1. Immutable - val    2. Mutable - var
val a = 100
var b = "sai"
val z:Int = 100 // better option as it reduces compile time
// Once u declare a variable and assign a type it remains through the scope i.e, it cant change it datatype

// COMMAND ----------

// Spark framework is written in scala; Hadoop is written in java
// Block expression
var add = {
  var a = 10
  var b = 20
  a-b
  a+b
  a*b// last expression is assigned to the variable add
}

// COMMAND ----------

lazy val x = {println("foo"); 10} // execution of x will be deferred till its called
println("bar")
println(x)
val p = 1 to 10 // range
val q = 1 to 50 by 1 //range with step

// COMMAND ----------

// MAGIC %python
// MAGIC x = range(1,10)
// MAGIC x

// COMMAND ----------

// if loop
    var fr = "apple"
    if (fr == "apple") println("red")
    else println("no color")
// for loop
for (i<- 1 to 10)
println(i)


// COMMAND ----------

val a = sc.parallelize(List((3,"Gnu"),(7,"YAk"),(3,"Bar"),(5,"mouse")),2)
a.countByKey

// COMMAND ----------

println(sc)
val x = sc.parallelize(List("spark","rdd","ex","sample","ex"),3)
val y = x.map(x=> (x,1))
y.collect()
y.foreach(println)
// while using flat map it just creates a 1-D array with is different from regular map
val p = sc.parallelize(List(1,2,3)).flatMap(x => List(x,x)).collect
// [(1,1),(2,2),(3,3)] ---> map
// (1, 1, 2, 2, 3, 3) ----> flatmap
// filter which is similar to where
val num = sc.parallelize((1 to 10))

// COMMAND ----------

// _ represents the inside of RDD
val num1 = sc.parallelize((1 to 10))
val num2 = sc.parallelize((6 to 15))
val even = num1.filter(_%2==0).collect
num1.intersection(num2).collect
num1.union(num2).distinct.collect


// COMMAND ----------

val sorts = num1.sortBy(c=> c,true).collect
val zips = num1 zip num2 // can only happen with equal no of arrays
zips.collect

// COMMAND ----------

val parquetFile = "dbfs:/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
val parquetFileDF = spark.read.parquet(parquetFile)
display(parquetFileDF)


// COMMAND ----------

  val Parq = parquetFileDF.toDF(Seq("Project","Article","Requests","Bytes"):_*)

// COMMAND ----------

Parq.orderBy($"Article".desc).show

// COMMAND ----------

Parq.filter("Requests>1").orderBy($"Article".desc).show

// COMMAND ----------

val csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"
val rdd = sc.textFile(csvFile)

// COMMAND ----------

val df = spark.read.option("sep","\t").option("header","true").option("inferSchema","true").option("mode","failfast").csv(csvFile)

// COMMAND ----------

df.createOrReplaceTempView("mobdata")

// COMMAND ----------

spark.sql("select * from mobdata").show

// COMMAND ----------

spark.sql("select count(*) from mobdata").show

// COMMAND ----------

spark.sql("select avg(Requests) from mobdata").show

// COMMAND ----------


