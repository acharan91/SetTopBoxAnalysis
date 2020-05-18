package org.SetTopBoxAnalysis.com
import org.apache.spark.sql.SparkSession
import scala.xml.XML
object MaximumPrice {
  
  
  def main(args:Array[String])
  {
    
    
    val spark=SparkSession
    .builder()
    .appName("MaximumPrice")
    .master("local")
    .getOrCreate()
    
    
    val data=spark.read.textFile("C:/Users/Charan/Downloads/Set_Top_Box_Data.txt").rdd
    val result=data.filter{line=>
      {
        val tokens=line.split("\\^")
        val eventID=Integer.parseInt(tokens(2).toString())
        eventID==102 || eventID==113
      }  
    }
    
    .map{line=>
      {
        val tokens=line.split("\\^")
        var offerID=""
        var Price=0.0
        val xmlValue=tokens(4).toString()
        val xml=XML.loadString(xmlValue)
        
        for(nv<-xml.child)
        {
          val NV_Value=XML.loadString(nv.toString())
          val N_attribute=(NV_Value\\"@n").toString()
          val V_attribute=(NV_Value\\"@v").toString()
          
          if(N_attribute=="Price")
          {
            Price=(V_attribute).toDouble
            
            
          }
          else if(N_attribute=="OfferId")
          {
            offerID=V_attribute
          }
            
        }
        (offerID,Price)
          
      } }
    
    .groupByKey()
    .map{line=>
      {
        val MaxPrice=line._2.max
        
        (line._1,MaxPrice)
      }}
    .sortBy(rec => (rec._2), false)
    result.foreach(println)
  }
}