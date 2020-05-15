package com.rachit.uberanalytics

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object UberAnalytics {


  def main(arg: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("webLogChallenge")
      .getOrCreate()


    // READ the data

    val uber_df = spark.read.format("csv").option("delimiter",",").option("inferSchema" , "true").option("header",true).load("/Users/Karunair/Downloads/UberRequestData.csv")

    //clean by standardizing the time format and dropiing extra columns

    def to_timestamp_(col: Column,
                      formats: Seq[String] = Seq("dd/MM/yyyy HH:mm", "dd-MM-yyyy HH:mm:ss")) = {
      coalesce(formats.map(f => to_timestamp(col, f)): _*)
    }


    val uber_cleaned = uber_df.withColumn("rqt", to_timestamp_(col("Request timestamp"))).withColumn("drt", to_timestamp_(col("Drop timestamp"))).drop("Request timestamp","Drop timestamp")

    // checking the distinct drop points ad status and their counts

    /*
    uber_cleaned.select(col("Pickup point")).distinct.show

    uber_cleaned.select(col("Status")).distinct.show

    uber_cleaned.groupBy("Status","Pickup point").count.show
    */

    uber_cleaned.createOrReplaceTempView("uber_cleaned")

    // adding new column timeslot based on hour of pickup during the day

    val uber_cleaned_with_hour = uber_cleaned.withColumn("hour", hour(col("rqt")))

    val uber_timeslots = uber_cleaned_with_hour.withColumn("timeSlot",
      when(col("hour") > 5 && col("hour") <=9,"EARLY MORNING")
        .when(col("hour") > 9 && col("hour") <=12,"LATE MORNING")
        .when(col("hour") > 12 && col("hour") <=16,"AFTERNOON")
        .when(col("hour") > 16 && col("hour") <=21,"EVENING")
        .when(col("hour") > 21 && col("hour") <=24,"NIGHT")
        .otherwise("LATE NIGHT"))

    // adding new column which will specify whether there was a gap in the trip based on it was completed or not

    val uber_timeSlotsWithGap = uber_timeslots.withColumn("gap",
      when(col("Status") === "Trip Completed" ,"No Gap")
        .otherwise("Gap"))

    //find the timeslots for which gaps are highest

    val timeSlotWithHighestGaps = uber_timeSlotsWithGap.filter(col("gap")==="Gap").groupBy("timeSlot").count.orderBy(col("count").desc).first

    //Find the types of requests (city-airport or airport-city) for which the gap is the most severe in the identified time slots

    val gapCountsBytimeSlotsAndPickupPoint = uber_timeSlotsWithGap.filter(col("gap")==="Gap").groupBy("Pickup point","timeSlot").count

    gapCountsBytimeSlotsAndPickupPoint.createOrReplaceTempView("gapCountsBytimeSlotsAndPickupPoint")

    spark.sql("SELECT * FROM (SELECT *,RANK() OVER (PARTITION BY timeSlot ORDER BY count DESC ) AS rank FROM gapCountsBytimeSlotsAndPickupPoint) sub WHERE rank =1")


    //Close Ended questions –

    // Question 1 :
    uber_df.select(col("Driver id"), col("Status")).filter(col("Status")==="Trip Completed").groupBy("Driver id").count().orderBy(col("count").desc)

    /*
    Result : Driver ID - 22
    No of trips completed -16
    */

    //Question 3:

    uber_cleaned.select(col("Status"),col("rqt")).filter(col("Status")==="No Cars Available").withColumn("request_date_weekday",date_format(col("rqt"),"E")).groupBy(col("request_date_weekday")).count().show()

    /*
    +--------------------+-----+
   |request_date_weekday|count|
   +--------------------+-----+
   |                 Mon|  504|
   |                 Thu|  571|
   |                 Wed|  490|
   |                 Tue|  505|
   |                 Fri|  580|
   +--------------------+-----+


   Friday - 580

   */

    // Question 2:

    val cancelled_trips = uber_cleaned.select(col("Pickup point"),col("Status")).filter(col("Pickup point") === "Airport").filter(col("Status") === "Cancelled").count()

    val total_trips = uber_cleaned.select(col("Pickup point"),col("Status")).filter(col("Pickup point") === "Airport").count()


    val percent_trips= (cancelled_trips.toDouble/total_trips.toDouble)*100

    /*
     Cancelled - 198

     Total  - 3238
    */

  }

}
