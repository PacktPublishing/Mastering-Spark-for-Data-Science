# Mastering-Spark-for-Data-Science
Mastering Spark for Data Science, published by Packt
# Mastering Spark for Data Science
This is the code repository for [Mastering Spark for Data Science](https://www.packtpub.com/big-data-and-business-intelligence/mastering-spark-data-science?utm_source=github&utm_medium=repository&utm_campaign=9781785882142), published by [Packt](https://www.packtpub.com/?utm_source=github). It contains all the supporting project files necessary to work through the book from start to finish.

## About the Book
Data science seeks to transform the world using data and this is typically achieved through disrupting and changing real processes in real industries. In order to operate at this level you need to be able to build data science solutions of substance – solutions that solve real problems and that run so relialbly that people trust and actualize them. Spark has emerged as the big data platform of choice for data scientists due to its speed, scalability, and easy-to-use APIs.

This book deep dives into Spark to deliver production grade data science solutions that are innovative, disruptive, and reliable. This process is demonstrated by exploring the construction of a sophisticated global news analysis service that uses Spark to generate continuous geopolitical and current affairs insights. You will learn all about the core Spark APIs and take a comprehensive tour of advanced libraries including: Spark SQL, visual streaming, MLlib, and more.


## Instructions and Navigation
All of the code is organized into folders. Each folder starts with a number followed by the application name. For example, Chapter02.



The code will look like the following:
```
import org.apache.spark.sql.functions._      
 
val rdd = rawDS map GdeltParser.toCaseClass    
val ds = rdd.toDS()     
  
// DataFrame-style API 
ds.agg(avg("goldstein")).as("goldstein").show() 
```

Spark 2.0 is used throughout the book along with Scala 2.11, Maven and Hadoop. This is the basic environment required, there are many other technologies used which are introduced in the relevant chapters.

## Related Products
* [Apache Spark for Data Science Cookbook](https://www.packtpub.com/big-data-and-business-intelligence/apache-spark-data-science-cookbook?utm_source=github&utm_medium=repository&utm_campaign=9781785880100)

* [Spark for Data Science](https://www.packtpub.com/big-data-and-business-intelligence/spark-data-science?utm_source=github&utm_medium=repository&utm_campaign=9781785885655)

* [Mastering Java for Data Science](https://www.packtpub.com/big-data-and-business-intelligence/mastering-java-data-science?utm_source=github&utm_medium=repository&utm_campaign=9781782174271)

### Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSe5qwunkGf6PUvzPirPDtuy1Du5Rlzew23UBp2S-P3wB-GcwQ/viewform) if you have any feedback or suggestions.

