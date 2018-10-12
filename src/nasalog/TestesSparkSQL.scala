package nasalog

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object TestesSparkSQL {
  
  //Classe principal
  def main(args: Array[String]) :Unit = {
//    if(args.length < 1){
//      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
//      System.exit(1)
//    }
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")      
      .appName("TestesSparkSQL")
      .getOrCreate()  
    
    import spark.implicits._
    
        //Definindo Esquema
//    val esquema = StructType(StructField("host", StringType, true) ::
//        StructField("datahora", StringType, true) ::
//        StructField("timezone", StringType, true) ::        
//        StructField("requisicao", StringType, true) ::
////        StructField("pagina", StringType, true) ::
////        StructField("tipo", StringType, true) ::
//        StructField("resposta", StringType, true) ::
//        StructField("bytesresposta", LongType, true) :: 
//        Nil)
        
     val esquema = StructType(StructField("host", StringType, true) :: 
                   StructField("traco1", StringType, true) :: 
                   StructField("traco2", StringType, true) :: 
                   StructField("datahora", StringType, true ) :: 
                   StructField("timezone", StringType, true):: 
                   StructField("requisicao", StringType, true) :: 
                   StructField("resposta", IntegerType, true) :: 
                   StructField("bytes", LongType, true) :: Nil)
  
     val leituras = spark
       .read
       .schema(esquema)
       .option("delimiter", " ")
       .csv("/home/felipe/Downloads/descompact July 95/xaa")
       
     //leituras.select($"host", $"resposta").filter($"resposta" !== 200).show 
     
     //leituras.groupBy("host").count().show()
     
//     leituras.createOrReplaceTempView("nasa")
//     
//     val hostCountOrder = spark.sql("select host, count(host) as total from nasa group by host order by total desc")
//     
//     hostCountOrder.show
       
       val requisicoes = leituras
      .filter(!isnull($"requisicao"))      
      .select(substring($"datahora", 2, 11) as "data", 
              substring($"datahora", 14, 21) as "hora", 
              $"bytes" as "bytes")
              
      //requisicoes.select(unix_timestamp($"data", "dd/MMM/yyyy") as "convertido", $"data", $"hora", $"bytes").show              
     
              
      val reqwindow = requisicoes
    	.select( unix_timestamp(
    	    format_string("%s %s", $"data", $"hora"), "dd/MMM/yyyy HH:mm:ss" )
    	    .cast("timestamp") as "tempo", 
    	    $"bytes" as "bytesResposta").as[TimestampResposta]
    	    
      reqwindow.select($"tempo", $"bytesResposta").show
  }
  
}