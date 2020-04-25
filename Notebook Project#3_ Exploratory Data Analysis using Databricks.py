# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC #Project#3: Exploratory Data Analysis using Databricks 
# MAGIC ### CS 5165/6065: Introduction to Cloud Computing
# MAGIC 
# MAGIC ####(Total Points: 20)
# MAGIC ####Due Date: 04/23/2020 (End of the day)
# MAGIC 
# MAGIC ####Instruction:
# MAGIC ##### Perform exploratory data analysis (EDA) to gain insights using Community Edition of Databricks.
# MAGIC ### Use "Project#3: Exploratory Data Analysis using Databricks.dbc" file for the assigment
# MAGIC ##### “Databricks Overview Labs CS 5165 | 6065 v1_1.dbc” file has been provided for review
# MAGIC 
# MAGIC #### Project Tasks:
# MAGIC 
# MAGIC #####1.  Calculate the Total count of Crimes for each of the 6 United States cities listed below using 2016 crime data in dbfs:/mnt/training/crime-data-2016
# MAGIC #####2.  Provide Total count of different Types of Crimes for each of 6 United States cities listed below using crime-data-2016
# MAGIC #####3.  Calculate the total Robbery count for each of the 3 United States cities listed below using crime-data-2016
# MAGIC #####4.  Find the months with the Highest and Lowest Robbery counts for each of the 3 United States cities listed below using crime-data-2016
# MAGIC #####5.  Combine all three cities robberies-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016
# MAGIC #####6.  Plot the robberies per month for each of the three cities, producing one Graph using the contents of combinedRobberiesByMonthDF
# MAGIC #####7.  Find the "per capita robbery rates" using ther Hint below, and plot graph as above for the per capita robbery rates per month for each of the three cities, producing one Graph using the contents of robberyRatesByCityDF
# MAGIC #####8.  Find the monthly HOMICIDE counts for each of the 2 United States cities listed below using crime-data-2016, and Combine both cities HOMICIDE-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016
# MAGIC #####9. Find the "per capita HOMICIDE rates" using ther Hint in Qn 7, and plot graph as above for the per capita HOMICIDE rates per month for each of the 2 cities, producing one Graph using the contents of HOMICIDERatesByCityDF
# MAGIC #####10. A stretch goal to address a Data Science question on predicting crimes for a city as a time series model. How would you predict future values for monthly Robbery count rate for Log Angeles using the historical values from crime-data-2016.  Data Science is a multi-year discipline.  I am not expecting anything fancy.  There is no wrong answer. I do expect students to explore and give their best shot.
# MAGIC 
# MAGIC 
# MAGIC Submission:
# MAGIC 1.	Export the completed "Project#3: Exploratory Data Analysis using Databricks.dbc" as DBC Archive file with all the informatation in Blackboard
# MAGIC 2.	Your spark code/command and results included

# COMMAND ----------

# MAGIC %md
# MAGIC ### Project#3: Exploratory Data Analysis using Databricks
# MAGIC Perform exploratory data analysis (EDA) to gain insights from a data lake.
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC In `dbfs:/mnt/training/crime-data-2016`, there are a number of Parquet files containing 2016 crime data from seven United States cities:
# MAGIC 
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC 
# MAGIC The data is cleaned up a little, but has not been normalized. Each city reports crime data slightly differently, so
# MAGIC examine the data for each city to determine how to query it properly.
# MAGIC 
# MAGIC Your job is to use some of this data to gain insights about certain kinds of crimes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Project3-Setup

# COMMAND ----------

print("username: " + username)
print("userhome: " + userhome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1:  Calculate the Total count of Crimes for each of the 6 United States cities listed below using 2016 crime data in dbfs:/mnt/training/crime-data-2016:
# MAGIC 
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC 
# MAGIC **Hint:**
# MAGIC 
# MAGIC Start by creating DataFrames for Los Angeles, Philadelphia, and Dallas data.
# MAGIC 
# MAGIC Use `spark.read.parquet` to create named DataFrames for the files you choose. 
# MAGIC 
# MAGIC To read in the parquet file, use `crimeDataNewYorkDF = spark.read.parquet("/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")`
# MAGIC 
# MAGIC 
# MAGIC Use the following view names:
# MAGIC 
# MAGIC | City          | DataFrame Name            | Path to DBFS file
# MAGIC | ------------- | ------------------------- | -----------------
# MAGIC | Los Angeles   | `crimeDataLosAngelesDF`   | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet`
# MAGIC | Philadelphia  | `crimeDataPhiladelphiaDF` | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet`
# MAGIC | Dallas        | `crimeDataDallasDF`       | `dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet`
# MAGIC | etc...        |

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:**  Los Angeles

# COMMAND ----------

from pyspark.sql.functions import col, asc, month, lower, lit, max, min, to_date

# COMMAND ----------

#Initialization
Total_Crimes_Count = {}
Robbery_Count = {}
Population = {}

# COMMAND ----------

# TODO

LADF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Los-Angeles-2016.parquet")
PhilDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Philadelphia-2016.parquet")
DalDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Dallas-2016.parquet")
NYDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-New-York-2016.parquet")
ChicDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Chicago-2016.parquet")
BosDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/crime-data-2016/Crime-Data-Boston-2016.parquet")


# COMMAND ----------

Total_Crimes_Count["Los Angeles"] = LADF.count()
Total_Crimes_Count["Philadelphia"] = PhilDF.count()
Total_Crimes_Count["Dallas"] = DalDF.count()
Total_Crimes_Count["New York"] = NYDF.count()
Total_Crimes_Count["Chicago"] = ChicDF.count()
Total_Crimes_Count["Boston"] = BosDF.count()
print("Total Crime Count: \n",Total_Crimes_Count)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 2: Provide Total count of different Types of Crimes for each of 6 United States cities listed below using crime-data-2016:
# MAGIC * New York
# MAGIC * Los Angeles
# MAGIC * Chicago
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC * Boston
# MAGIC 
# MAGIC ### Notice in the Dataframes:
# MAGIC * The `crimeDataNewYorkDF` and `crimeDataBostonDF` DataFrames use different names for the columns.
# MAGIC * The data itself is formatted differently and different names are used for similar concepts.
# MAGIC 
# MAGIC This is common in a Data Lake. Often files are added to a Data Lake by different groups at different times. The advantage of this strategy is that anyone can contribute information to the Data Lake and that Data Lakes scale to store arbitrarily large and diverse data. The tradeoff for this ease in storing data is that it doesn’t have the rigid structure of a traditional relational data model, so the person querying the Data Lake will need to normalize data before extracting useful insights.
# MAGIC 
# MAGIC ## Same Type of Data, Different Structure
# MAGIC 
# MAGIC Examine crime data to determine how to extract homicide statistics.
# MAGIC 
# MAGIC Because the data sets are pooled together in a Data Lake, each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC 
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.

# COMMAND ----------

#Creating temporary views for each state
PhilDF.createOrReplaceTempView("Phil")
LADF.createOrReplaceTempView("LA")
DalDF.createOrReplaceTempView("Dal")
NYDF.createOrReplaceTempView("NY")
ChicDF.createOrReplaceTempView("Chic")
BosDF.createOrReplaceTempView("Bos")

# COMMAND ----------

LADF.groupBy('crimeCodeDescription').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in Los Angeles";
# MAGIC SELECT crimeCodeDescription,COUNT(*) FROM LA GROUP BY crimeCodeDescription

# COMMAND ----------

PhilDF.groupBy('ucr_general_description').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in Philadel";
# MAGIC SELECT ucr_general_description,COUNT(*) FROM phil GROUP BY ucr_general_description

# COMMAND ----------

DalDF.groupBy('typeOfIncident').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in Dallas";
# MAGIC SELECT typeOfIncident,COUNT(*) FROM dal GROUP BY typeOfIncident

# COMMAND ----------

NYDF.groupBy('offenseDescription').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in New York";
# MAGIC SELECT offenseDescription,COUNT(*) FROM NY GROUP BY offenseDescription

# COMMAND ----------

ChicDF.groupBy('primaryType').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in Chicago";
# MAGIC SELECT primaryType,COUNT(*) FROM Chic GROUP BY primaryType

# COMMAND ----------

BosDF.groupBy('OFFENSE_DESCRIPTION').count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Types of Crimes in Boston";
# MAGIC SELECT OFFENSE_DESCRIPTION,COUNT(*) FROM Bos GROUP BY OFFENSE_DESCRIPTION

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 3: Calculate the total Robbery count for each of the 3 United States cities listed below using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC **Hint:**  For each table, examine the data to figure out how to extract _robbery_ statistics.
# MAGIC 
# MAGIC Each city uses different values to indicate robbery. Commonly used terminology is "larceny", "burglary" or "robbery." These challenges are common in data lakes.  To simplify things, restrict yourself to only the word "robbery" (and not attempted-roberty, larceny, or burglary).
# MAGIC 
# MAGIC Explore the data for the three cities until you understand how each city records robbery information. If you don't want to worry about upper- or lower-case, 
# MAGIC remember to use the DataFrame `lower()` method to converts column values to lowercase.
# MAGIC 
# MAGIC Create a DataFrame containing only the robbery-related rows, as shown in the table below.
# MAGIC 
# MAGIC **Hint:** For each table, focus your efforts on the column listed below.
# MAGIC 
# MAGIC Focus on the following columns for each table:
# MAGIC 
# MAGIC | DataFrame Name            | Robbery DataFrame Name  | Column
# MAGIC | ------------------------- | ----------------------- | -------------------------------
# MAGIC | `crimeDataLosAngelesDF`   | `robberyLosAngelesDF`   | `crimeCodeDescription`
# MAGIC | `crimeDataPhiladelphiaDF` | `robberyPhiladelphiaDF` | `ucr_general_description`
# MAGIC | `crimeDataDallasDF`       | `robberyDallasDF`       | `typeOfIncident`

# COMMAND ----------

RobLADF = LADF.filter(LADF["crimeCodeDescription"].startswith("ROBBERY"))
RobPhilDF = PhilDF.filter(PhilDF["ucr_general_description"].startswith("ROBBERY"))
RobDalDF = DalDF.filter(DalDF["typeOfIncident"].startswith("ROBBERY"))
Robbery_Count["Los Angeles"] = RobLADF.count()
Robbery_Count["Philadelphia"] = RobPhilDF.count()
Robbery_Count["Dallas"] = RobDalDF.count()
print("Total Robbery Count in Each City: \n",Robbery_Count)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 4: Find the months with the Highest and Lowest Robbery counts for each of the 3 United States cities listed below using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC 
# MAGIC **Hint:** Now that you have DataFrames of only the robberies in each city, create DataFrames for each city summarizing the number of robberies in each month.
# MAGIC 
# MAGIC Your DataFrames must contain two columns:
# MAGIC * `month`: The month number (e.g., 1 for January, 2 for February, etc.).
# MAGIC * `robberies`: The total number of robberies in the month.
# MAGIC 
# MAGIC Use the following DataFrame names and date columns:
# MAGIC 
# MAGIC 
# MAGIC | City          | DataFrame Name     | Date Column 
# MAGIC | ------------- | ------------- | -------------
# MAGIC | Los Angeles   | `robberiesByMonthLosAngelesDF` | `timeOccurred`
# MAGIC | Philadelphia  | `robberiesByMonthPhiladelphiaDF` | `dispatch_date_time`
# MAGIC | Dallas        | `robberiesByMonthDallasDF` | `startingDateTime`
# MAGIC 
# MAGIC For each city, figure out which column contains the date of the incident. Then, extract the month from that date.

# COMMAND ----------

RobLADF.createOrReplaceTempView("RobLA")
RobPhilDF.createOrReplaceTempView("RobPhil")
RobDalDF.createOrReplaceTempView("RobDal")

# COMMAND ----------

RobLAByMonth = spark.sql("SELECT month(dateOccurred) as Month,count(*) as Robberies from RobLA group by  month(dateOccurred) order by 1,2")
RobPhilByMonth = spark.sql("SELECT month(dispatch_date) as Month,count(*) as Robberies from RobPhil group by  month(dispatch_date) order by 1,2")
RobDalByMonth = spark.sql("SELECT month(dateOfReport) as Month,count(*) as Robberies from RobDal group by  month(dateOfReport) order by 1,2")

# COMMAND ----------

max_in_LA = RobLAByMonth.agg({"Robberies": "max"}).collect()[0][0]
min_in_LA = RobLAByMonth.agg({"Robberies": "min"}).collect()[0][0]
max_in_Phil = RobPhilByMonth.agg({"Robberies": "max"}).collect()[0][0]
min_in_Phil = RobPhilByMonth.agg({"Robberies": "min"}).collect()[0][0]
max_in_Dal = RobDalByMonth.agg({"Robberies": "max"}).collect()[0][0]
min_in_Dal = RobDalByMonth.agg({"Robberies": "min"}).collect()[0][0]

# COMMAND ----------

print("Highest Robberies in a month in Los Angeles")
RobLAByMonth.filter(RobLAByMonth['Robberies'] == max_in_LA).show()
print("Lowest Robberies in a month in Los Angeles")
RobLAByMonth.filter(RobLAByMonth['Robberies'] == (min_in_LA)).show()
print("Highest Robberies in a month in Philadelphia")
RobPhilByMonth.filter(RobPhilByMonth['Robberies'] == (max_in_Phil)).show()
print("Lowest Robberies in a month in Philadelphia")
RobPhilByMonth.filter(RobPhilByMonth['Robberies'] == (min_in_Phil)).show()
print("Highest Robberies in a month in Dallas")
RobDalByMonth.filter(RobDalByMonth['Robberies'] == (max_in_Dal)).show()
print("Lowest Robberies in a month in Dallas")
RobDalByMonth.filter(RobDalByMonth['Robberies'] == (min_in_Dal)).show()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 5: Combine all three cities robberies-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts using crime-data-2016:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC 
# MAGIC Create another DataFrame called `combinedRobberiesByMonthDF`, that combines all three robberies-per-month views into one.
# MAGIC In creating this view, add a new column called `city`, that identifies the city associated with each row.
# MAGIC The final view will have the following columns:
# MAGIC 
# MAGIC * `city`: The name of the city associated with the row. (Use the strings "Los Angeles", "Philadelphia", and "Dallas".)
# MAGIC * `month`: The month number associated with the row.
# MAGIC * `robbery`: The number of robbery in that month (for that city).
# MAGIC 
# MAGIC **Hint:** You may want to apply the `union()` method in this example to combine the three datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC **Hint:** Combined 3 Cities

# COMMAND ----------

newLA = RobLAByMonth.withColumn("City", lit("Los Angeles"))
newPhil = RobPhilByMonth.withColumn("City", lit("Philadelphia"))
newDal = RobPhilByMonth.withColumn("City", lit("Dallas"))

# COMMAND ----------

# TODO

combinedRobberiesByMonthDF = newLA.union(newPhil.union(newDal))

# COMMAND ----------

max_in_all = combinedRobberiesByMonthDF.agg({"Robberies": "max"}).collect()[0][0]
min_in_all = combinedRobberiesByMonthDF.agg({"Robberies": "min"}).collect()[0][0]
print("Highest Robberies in a month in all three cities")
combinedRobberiesByMonthDF.filter(combinedRobberiesByMonthDF['Robberies'] == max_in_all).show(1)
print("Lowest Robberies in a month in all three cities")
combinedRobberiesByMonthDF.filter(combinedRobberiesByMonthDF['Robberies'] == min_in_all).show(1)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 6: Plot the robberies per month for each of the three cities, producing one Graph using the contents of `combinedRobberiesByMonthDF`
# MAGIC 
# MAGIC Adjust the plot options to configure the plot properly, as shown below:
# MAGIC 
# MAGIC When you first run the cell, you'll get an HTML table as the result. To configure the plot:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-4.png" style="width: 362px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>
# MAGIC 
# MAGIC 1. Click the graph button.
# MAGIC 2. If the plot doesn't look correct, click the **Plot Options** button.
# MAGIC 3. Configure the plot similar to the following example.
# MAGIC 
# MAGIC ;**Hint:** Order your results by `month`, then `city`.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/capstone-plot-2.png" style="width: 268px; margin: 10px; border: 1px solid #aaaaaa; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# TODO
display(combinedRobberiesByMonthDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 7: Find the "per capita robbery rates" using ther Hint below, and plot graph as above for the per capita robbery rates per month for each of the three cities, producing one Graph using the contents of `robberyRatesByCityDF`:
# MAGIC * Los Angeles
# MAGIC * Philadelphia
# MAGIC * Dallas
# MAGIC 
# MAGIC While the above graph is interesting, it's flawed: it's comparing the raw numbers of robberies, not the per capita robbery rates.
# MAGIC 
# MAGIC The DataFrame (already created) called `cityDataDF` (dbfs:/mnt/training/City-Data.parquet)  contains, among other data, estimated 2016 population values for all United States cities
# MAGIC with populations of at least 100,000. (The data is from [Wikipedia](https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population).)
# MAGIC 
# MAGIC * Use the population values in that table to normalize the robberies so they represent per-capita values (total robberies divided by population).
# MAGIC * Save your results in a DataFrame called `robberyRatesByCityDF`.
# MAGIC * The robbery rate value must be stored in a new column, `robberyRate`.
# MAGIC 
# MAGIC Next, graph the results, as above.

# COMMAND ----------

cityDataDF = spark.read.option("header",True).option("inferSchema",True).parquet("dbfs:/mnt/training/City-Data.parquet")


# COMMAND ----------

display(cityDataDF)

# COMMAND ----------

Population["LA"] = cityDataDF.select('estPopulation2016').filter(cityDataDF.city == "Los Angeles").collect()[0][0]
Population["Phil"] = cityDataDF.select('estPopulation2016').filter(cityDataDF.city == "Philadelphia").collect()[0][0]
Population["Dal"] = cityDataDF.select('estPopulation2016').filter(cityDataDF.city == "Dallas").collect()[0][0]
print(Population)

# COMMAND ----------

rob_rates_LA = newLA.withColumn('robberyRate', newLA.Robberies/Population["LA"])
rob_rates_Phil = newPhil.withColumn('robberyRate', newPhil.Robberies/Population["Phil"])
rob_rates_Dal = newDal.withColumn('robberyRate', newDal.Robberies/Population["Dal"])
robberyRatesByCityDF = rob_rates_LA.union(rob_rates_Phil.union(rob_rates_Dal))

# COMMAND ----------

display(robberyRatesByCityDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 8: Find the monthly HOMICIDE counts for each of the 2 United States cities listed below using crime-data-2016, and Combine both cities HOMICIDE-per-month views into one and Find the month with the Highest and Lowest combined Robbery counts.  See Question 6.
# MAGIC * New York
# MAGIC * Boston
# MAGIC 
# MAGIC **Hint:**  Same Type of Data, Different Structure
# MAGIC In this section, we examine crime data to determine how to extract homicide statistics.
# MAGIC Each city may use different field names and values to indicate homicides, dates, etc.
# MAGIC For example:
# MAGIC * Some cities use the value "HOMICIDE", "CRIMINAL HOMICIDE" or "MURDER".
# MAGIC * In the New York data, the column is named `offenseDescription` while in the Boston data, the column is named `OFFENSE_CODE_GROUP`.
# MAGIC * In the New York data, the date of the event is in the `reportDate`, while in the Boston data, there is a single column named `MONTH`.
# MAGIC 
# MAGIC To get started, create a temporary view containing only the homicide-related rows.
# MAGIC At the same time, normalize the data structure of each table so all the columns (and their values) line up with each other.
# MAGIC In the case of New York and Boston, here are the unique characteristics of each data set:
# MAGIC 
# MAGIC | | Offense-Column        | Offense-Value          | Reported-Column  | Reported-Data Type |
# MAGIC |-|-----------------------|------------------------|-----------------------------------|
# MAGIC | New York | `offenseDescription`  | starts with "murder" or "homicide" | `reportDate`     | `timestamp`    |
# MAGIC | Boston | `OFFENSE_CODE_GROUP`  | "Homicide"             | `MONTH`          | `integer`      |
# MAGIC For the upcoming aggregation, you need to alter the New York data set to include a `month` column which can be computed from the `reportDate` column using the `month()` function. Boston already has this column.
# MAGIC 
# MAGIC In this example, we use several functions in the `pyspark.sql.functions` library, and need to import:
# MAGIC 
# MAGIC * `month()` to extract the month from `reportDate` timestamp data type.
# MAGIC * `lower()` to convert text to lowercase.
# MAGIC * `contains(mySubstr)` to indicate a string contains substring `mySubstr`.
# MAGIC 
# MAGIC Also, note we use  `|`  to indicate a logical `or` of two conditions in the `filter` method.

# COMMAND ----------

HomNYDF = NYDF.filter((lower(NYDF["offenseDescription"]).startswith("homicide")) | (lower(NYDF["offenseDescription"]).startswith("murder")))
HomBosDF = BosDF.filter(lower(BosDF["OFFENSE_CODE_GROUP"]).contains("homicide"))

# COMMAND ----------

HomNYByMonth = HomNYDF.groupBy(month(HomNYDF["reportDate"])).count().sort(asc("month(reportDate)")).withColumnRenamed("month(reportDate)","Month").withColumnRenamed("count","Homicides")
HomBosByMonth = HomBosDF.groupBy('MONTH').count().sort(asc("MONTH")).withColumnRenamed("MONTH","Month").withColumnRenamed("count","Homicides")

# COMMAND ----------

print("Monthly Robberies count in New York")
display(HomNYByMonth)

# COMMAND ----------

print("Monthly Robberies count in Boston")
display(HomBosByMonth)

# COMMAND ----------

newNY = HomNYByMonth.withColumn("City", lit("New York"))
newBos = HomBosByMonth.withColumn("City", lit("Boston"))

# COMMAND ----------

combinedHomicidesByMonthDF = newNY.union(newBos)
max_in_combined = combinedHomicidesByMonthDF.agg(max('Homicides')).collect()[0][0]
min_in_combined = combinedHomicidesByMonthDF.agg(min('Homicides')).collect()[0][0]
print("Highest Homicides in a month among New York and Boston")
combinedHomicidesByMonthDF.filter(combinedHomicidesByMonthDF['Homicides'] == max_in_combined).show(1)
print("Lowest Homicides in a month among New York and Boston")
combinedHomicidesByMonthDF.filter(combinedHomicidesByMonthDF['Homicides'] == min_in_combined).show(1)

# COMMAND ----------

 display(combinedHomicidesByMonthDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 9: Find the "per HOMICIDE robbery rates" using ther Hint in Qn 7, and plot graph as above for the per capita HOMICIDE rates per month for each of the two cities, producing one Graph using the contents of HOMICIDERatesByCityDF:

# COMMAND ----------

Population["NY"] = cityDataDF.select('estPopulation2016').filter(cityDataDF.city == "New York").collect()[0][0]
Population["Bos"] = cityDataDF.select('estPopulation2016').filter(cityDataDF.city == "Boston").collect()[0][0]
print(Population)

# COMMAND ----------

hom_rates_NY = newNY.withColumn('homicideRate', newNY.Homicides/Population["NY"])
hom_rates_Bos = newBos.withColumn('homicideRate', newBos.Homicides/Population["Bos"])
homicideRatesByCityDF = hom_rates_NY.union(hom_rates_Bos)
display(homicideRatesByCityDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Question 10: A stretch goal to address a Data Science question on predicting crimes for a city as a time series model. How would you predict future values for monthly Robbery Count rate for Log Angeles using the historical values from crime-data-2016.  Data Science is a multi-year discipline.  I am not expecting anything fancy.  There is no wrong answer. I do expect students to explore and give their best shot.

# COMMAND ----------

# MAGIC %md
# MAGIC There are several models such as ARIMA, Facebook Prophet, Linear Regression etc. that can be used in the prediction of crime based on historical values.

# COMMAND ----------

# DBTITLE 1,ARIMA model
Robb_LA_DF =LADF.filter(lower(LADF['crimeCodeDescription']).startswith("robbery")).groupBy('dateOccurred').count().withColumnRenamed('dateOccurred','Date').withColumnRenamed('count','Robberies')
Robb_LA_DF = Robb_LA_DF.select((to_date(col('Date')).alias("Date").cast("date")),'Robberies')

# COMMAND ----------

display(Robb_LA_DF)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# COMMAND ----------

test = Robb_LA_DF.toPandas()

# COMMAND ----------

Robb_LA_DF.take(2)

# COMMAND ----------

Robb_LA_DF.printSchema()

# COMMAND ----------

import requests, pandas as pd, numpy as np
from pandas import DataFrame
from io import StringIO
import time, json
from datetime import date
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.metrics import mean_squared_error
import matplotlib.pylab as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

fig=test.plot(x="Date", y="Robberies", figsize=(18,4)) 
display(fig.figure)

# COMMAND ----------

uber_trained = test.copy(True)
uber_trained['Date'] = pd.to_datetime(uber_trained['Date'])
uber_trained = uber_trained.set_index('Date')

# COMMAND ----------

def test_stationarity(timeseries):
         
    #Perform Dickey-Fuller test:
    print('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries.iloc[:,0].values, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    #print(dftest)
    print(dfoutput)

# COMMAND ----------

test_stationarity(uber_trained)

# COMMAND ----------

ts_log = np.log(uber_trained)

fig_log=ts_log.plot(figsize=(18, 6))
display(fig_log.figure)

# COMMAND ----------

test_stationarity(ts_log)

# COMMAND ----------

ts_log_diff = ts_log - ts_log.shift()
ts_log_diff.drop(ts_log_diff.index[0], inplace=True)

fig_log=ts_log_diff.plot(figsize=(18, 6))
display(fig_log.figure)

# COMMAND ----------

test_stationarity(ts_log_diff)

# COMMAND ----------

lag_acf = plot_acf(ts_log_diff, lags=10)
display(lag_acf)

# COMMAND ----------

lag_pacf = plot_pacf(ts_log_diff, lags=10)
display(lag_pacf)

# COMMAND ----------

import math

model = ARIMA(ts_log_diff.astype(float), order=(7, 1, 2))   # uber_trained
results_ARIMA = model.fit(maxiter=500)  
print(results_ARIMA.summary())

# COMMAND ----------

fitted_values  = results_ARIMA.predict(1,len(ts_log_diff)-1,typ='linear')  # uber_trained
fitted_values_frame = fitted_values.to_frame()

# COMMAND ----------

x = ts_log_diff.merge(fitted_values_frame, how='outer', left_index=True, right_index=True)  #uber_trained
fig= x.plot(figsize=(20,5))
display(fig.figure)


# COMMAND ----------

print(results_ARIMA.summary())
# plot residual errors
residuals = DataFrame(results_ARIMA.resid)
residuals.plot(kind='kde')
print(residuals.describe())

# COMMAND ----------

test_set = LADF.filter(lower(LADF['crimeCodeDescription']).startswith("robbery")).groupBy('dateOccurred').count().withColumnRenamed('dateOccurred','Date').withColumnRenamed('count','Robberies').take(100)
test_df = spark.createDataFrame(test_set)
test_df = test_df.withColumnRenamed('dateOccurred','Date')
tq = test_df.toPandas()
uber_test = tq.copy(True)
uber_trained['Date'] = pd.to_datetime(uber_test['Date'])
uber_test = uber_test.set_index('Date')


# COMMAND ----------

test_log = np.log(uber_test)
test_log_diff = test_log - test_log.shift()

# COMMAND ----------

test_log_diff.fillna(0, inplace=True)

# COMMAND ----------

model_test = ARIMA(test_log_diff.astype(float), order=(7, 1, 2))   # uber_trained
#results_test_ARIMA = model_test.fit(maxiter=100) 

# COMMAND ----------

# MAGIC %md
# MAGIC Since the initial AR coefficients are not stationary, there is some issue with this prediction model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC The crime data used in this notebook comes from the following locations:
# MAGIC 
# MAGIC | City          | Original Data 
# MAGIC | ------------- | -------------
# MAGIC | Boston        | <a href="https://data.boston.gov/group/public-safety" target="_blank">https&#58;//data.boston.gov/group/public-safety</a>
# MAGIC | Chicago       | <a href="https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2" target="_blank">https&#58;//data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2</a>
# MAGIC | Dallas        | <a href="https://www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data" target="_blank">https&#58;//www.dallasopendata.com/Public-Safety/Police-Incidents/tbnj-w5hb/data</a>
# MAGIC | Los Angeles   | <a href="https://data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq" target="_blank">https&#58;//data.lacity.org/A-Safe-City/Crime-Data-From-2010-to-Present/y8tr-7khq</a>
# MAGIC | New Orleans   | <a href="https://data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data" target="_blank">https&#58;//data.nola.gov/Public-Safety-and-Preparedness/Electronic-Police-Report-2016/4gc2-25he/data</a>
# MAGIC | New York      | <a href="https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i" target="_blank">https&#58;//data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i</a>
# MAGIC | Philadelphia  | <a href="https://www.opendataphilly.org/dataset/crime-incidents" target="_blank">https&#58;//www.opendataphilly.org/dataset/crime-incidents</a>
