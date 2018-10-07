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
/*
 * Busca no log e conta as urls mais acessadas 
 */
object Operacao02_UrlMaisAcessadaContador {
  
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
        StructField("timestamp", StringType, true) ::
        StructField("timezone", StringType, true) ::        
        StructField("requisicao", StringType, true) ::
        StructField("pagina", StringType, true) ::
        StructField("tipo", StringType, true) ::
        StructField("resposta", StringType, true) ::
        StructField("bytesresposta", StringType, true) :: 
        Nil)
        
     val leituras = spark.readStream
      .schema(esquema)
      .option("maxFilesPerTrigger", 3) // 3 Arquivos por requisição
      .option("delimiter", "\t") //para quando as linhas forem divididas em espaço
    	.csv(diretorio)    	
    
    	
    val requisicoes = leituras
      .filter(!isnull($"requisicao"))
      .select("host","requisicao").as[HostRequisicao]
    
    // Flatmap para retirar apenas o endereço requisitado
    val urls = requisicoes.flatMap(t => t.requisicao.split(" "))
    	.filter(l => l.length > 0 && l(0) == '/')
    	.withColumnRenamed("value","url")
    
    // Agrupa por URL	
    val contagem = urls.groupBy("url")
    	.count
    	.sort($"count".desc)
    	.withColumnRenamed("count","ocorrencias")	
    
    //Imprime a contagem no console	
    val query = contagem.writeStream
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("console")
      .start
      
	  query.awaitTermination()    
    
  }
  
  
}