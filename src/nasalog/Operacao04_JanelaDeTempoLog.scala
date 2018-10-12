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

object Operacao04_JanelaDeTempoLog {
  
  //Classe principal
  def main(args: Array[String]) :Unit = {
    if(args.length < 1){
      System.err.println("É necessário inserir a caminho da pasta nos argumentos de execução!")
      System.exit(1)
    }    
    
    // Jogo o caminho do diretório para uma variável
    val diretorio : String = args(0)
    
		Logger.getLogger("org").setLevel(Level.ERROR)
    		
    val spark = SparkSession
      .builder
      .master("local[*]")      
      .appName("Operacao02_UrlMaisAcessadaContador")
      .getOrCreate()    
      
    import spark.implicits._
    
     //Definindo Esquema
    val esquema = StructType(StructField("host", StringType, true) ::
        StructField("datahora", StringType, true) ::
        StructField("timezone", StringType, true) ::        
        StructField("requisicao", StringType, true) ::
//        StructField("pagina", StringType, true) ::
//        StructField("tipo", StringType, true) ::
        StructField("resposta", StringType, true) ::
        StructField("bytesresposta", LongType, true) :: 
        Nil)
        
    val leituras = spark.readStream
      .schema(esquema)      
      .option("delimiter", "\t") //para quando as linhas forem divididas em espaço
    	.csv(diretorio)
    	
    val requisicoes = leituras
      .filter(!isnull($"requisicao"))      
      .select(substring($"datahora", 2, 11) as "data", 
              substring($"datahora", 14, 21) as "hora", 
              $"bytesresposta" as "bytes")
    
     val reqwindow = requisicoes
    	.select( unix_timestamp(
    	    format_string("%s %s", $"data", $"hora"), "dd/MM/yyyy HH:mm:ss"
    	    ).cast("timestamp") as "tempo", 
    	    $"bytes" as "bytesResposta").as[TimestampResposta]    	
        
   	
    val somatorio = reqwindow//reqwindow   
//      .groupBy("data")
      .groupBy(        
    		window($"tempo", "60 minutes", "30 minutes")
//    		//window($"tempo", "10 minutes", "5 minutes"),    		
    	)
    	//.count
    	.agg(sum("bytesResposta").as("somatorio_bytesresposta"))
    	//.select($"window.start" as "inicio", $bytes)
    	//.sort($"somatorio_bytesresposta".desc)
      .orderBy($"window")
    
   
    val query = somatorio.writeStream
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start
      
	  query.awaitTermination()
    
	  //reqwindow      
//      .groupBy(
//    		window($"tempo", "60 minutes", "30 minutes"),
//    		//window($"tempo", "10 minutes", "5 minutes"),
//    		$"bytesResposta"
//    	)
    	//.count
    
  }
  
  
}