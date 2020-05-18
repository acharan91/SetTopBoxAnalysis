package org.SetTopBoxAnalysis.com

import org.apache.spark.sql.SparkSession
import scala.xml.XML 
object MaxMinDuration {
  
  def main(args:Array[String])
  {
    
    
    val spark=SparkSession
    .builder()
    .appName("MaxMinDuration")
    .master("local")
    .getOrCreate()
    
    val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
    val result=data.filter{line=>
      {
        val tokens=line.split("\\^")
        val eventID=tokens(2).toString().toInt
        eventID==118
        
      }}
    .map{line=>{
      val tokens=line.split("\\^")
      val xmlValue=tokens(4).toString()
      val xml=XML.loadString(xmlValue)
      var Duration=0
      for(nv<-xml.child)
      {
        val Nv_Value=XML.loadString(nv.toString())
        val N_attribute=(Nv_Value\\"@n").toString()
        val V_attribute=(Nv_Value\\"@v").toString()
        
         if(N_attribute=="DurationSecs"&& V_attribute!="" )
         {
           Duration=Integer.parseInt(V_attribute)
         }
        
      }
      (Duration)
      }}
   // result.foreach(println)
    //val MaxDuration=result.max()
   // val MinDuration=result.min()
    println("Max Duration: "+result.max())
    println("Min Duration: "+result.min())
    
  }
  
}