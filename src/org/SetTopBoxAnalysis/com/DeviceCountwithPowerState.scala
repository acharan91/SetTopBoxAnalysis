package org.SetTopBoxAnalysis.com

import scala.xml.XML
import org.apache.spark.sql.SparkSession
object DeviceCountwithPowerState {
  def main(args:Array[String])
  {
      System.setProperty("hadoop.home.dir", "C:/Users/Charan/Downloads/spark-2.4.5-bin-hadoop2.6")
    
    val spark=SparkSession
    .builder()
    .appName("DeviceCountwithPowerState")
    .master("local")
    .getOrCreate()

  val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
  
  val result=data.filter{line=>{
    val tokens=line.split("\\^")
    val eventId=Integer.parseInt(tokens(2).toString())
    //println(eventId)
    eventId==101
  }}
    
    .map{line=>
      {
        val tokens=line.split("\\^")
         val xmlValue=tokens(4).toString()
          val DeviceId=tokens(5).toString()
        val xml=XML.loadString(xmlValue)
        
       var PowerState=""
       
        for(nv<-xml.child)
        {
          val NV_XML=XML.loadString(nv.toString())
         // println(NV_XML)
          val N_attribute=(NV_XML\\"@n").toString()
         val V_attribute=(NV_XML\\"@v").toString()
          //println(N_attribute+": "+V_attribute)
          if(N_attribute=="PowerState")
          {
            PowerState=V_attribute
            
          }
         
          
        }
      (DeviceId,PowerState)  
      }}
      val PowerStateOn = result.filter(x => (x._2 =="ON"))
      //.groupByKey()
      .map( rec => (rec._1, 1))
      .reduceByKey(_+_)
       
 
      println("Total Device count with PowerState ON : " + PowerStateOn.count())
      
      val PowerStateOff = result.filter(x => (x._2 =="OFF"))
      //.groupByKey()
      .map( rec => (rec._1, 1))
      .reduceByKey(_+_)
      
      //.take(5)
      //resultPowerStateOff.foreach(println)
      println("Total Device count with PowerState OFF : " + PowerStateOff.count())
      
      spark.stop
  }  
}