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
      .option("maxFilesPerTrigger", 3) // 3 Arquivos por requisição
      .option("delimiter", "\t") //para quando as linhas forem divididas em espaço
    	.csv(diretorio)
    	
    val requisicoes = leituras
      .filter(!isnull($"requisicao"))      
      .select(substring($"datahora", 1, 12) as "data", 
              substring($"datahora", 14, 21) as "hora", 
              $"bytesresposta" as "bytesresposta").as[BytesHorasMinutos]
    
    // Agrupa por HOST	
    val somatorio = requisicoes
      .groupBy("data","hora")
    	.agg(sum("bytesresposta").as("somatorio_bytesresposta"))
    	.sort($"somatorio_bytesresposta".desc)
    
   
    val query = somatorio.writeStream
      .outputMode(Complete)
      //.trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start
      
	  query.awaitTermination()
    
    
  }
  
  
}