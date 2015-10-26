/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text, MapWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.collection.mutable
import java.lang.Math
import java.io._

object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	//final val KevinBacon = "G, Arpan"
	def main(args: Array[String]) 
	{
			val cores = args(0)
		val inputFile = args(1)
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		//conf.set("spark.storage.memoryFraction", "0.5") 
		conf.set("spark.default.parallelism", "500") 
		conf.set("spark.executor.memory","2g")
		conf.set("spark.rdd.compress","true")
	    conf.set("spark.files.overwrite","true")
	    //conf.set("spark.shuffle.memoryFraction","0.4")
		// conf.set("spark.core.connection.ack.wait.timeout","600")
		// conf.set("spark.akka.frameSize", "50")

		
		val hadoopConf = new Configuration() 
    	hadoopConf.set("textinputformat.record.delimiter", "\n\n") 
	
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		val sc = new SparkContext(conf)		
		println("Number of cores: " + args(0))
		println("Input file: " + inputFile)
		                                                                                                   
		
		var t0 = System.currentTimeMillis
		val filteredMap = sc.newAPIHadoopFile(inputFile,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],hadoopConf)
					.map(pair => pair._2.toString).filter(_.nonEmpty)//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		
	
		val tempactors2MoviesMap = filteredMap.map(x => x.split("\t+",2).toList).map(line => (line(0), line(1).split("\n\t+").toList)).zipWithUniqueId().persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		
		val actors2MoviesMap = tempactors2MoviesMap.map{case((actor,movie),id)=>(id,movie)}
		
		val actors2IdMap = tempactors2MoviesMap.map{case((actor,movie),id)=>(actor,id)}.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
			tempactors2MoviesMap.unpersist(false)
		val actorsCount = actors2IdMap.count()
			
		val idKevin = sc.parallelize(actors2IdMap.lookup(KevinBacon)).first
		val actors2DistanceMap = actors2IdMap.map{case (actor,id)=>(id,if(id == idKevin) 0 else 100)}.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		
		//filteredMap.unpersist(true)
		val actor2MovieMap = actors2MoviesMap.flatMapValues(x=>x)		
		val movie2ActorMap = actor2MovieMap.map{case(actor,movie)=>(movie.split("\\[")(0).trim,List(actor))}//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		val movieCount = movie2ActorMap.count()
		val movie2ActorsMap = movie2ActorMap.reduceByKey((a,b)=>(b:::a))
	
		//movie2ActorMap.unpersist(true)

    		val actorsList = movie2ActorsMap.map{case (key,value)=>(value)}
		val actor2actorsList = actorsList.map{case (list)=>(list.map(_->list))}
		val actor2Coactor = actor2actorsList.flatMap(x=>x).map{case (a,b)=>(a,b.filter(x=>(x!=a)))}//.filter(x=>(x._2.nonEmpty))
		  //super expensive, cache is a problem ExecutorLostFailure
		val actor2CoActorAll = actor2Coactor.reduceByKey((a,b)=>(b:::a))//.cache()//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)	
		var  joinRDDS = actors2DistanceMap.join(actor2CoActorAll)
		var distance  = joinRDDS.map{case (actor,(dis,list))=>(list.map(_ -> dis))}.flatMap(x=>x).map(xs => (xs._1 -> (xs._2 + 1))).reduceByKey((a,b)=>Math.min(a,b)).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		//var initialDistance = actors2DistanceMap
		
	
		for(i<- 1 until 6){
		 val tempdistance = actors2DistanceMap.join(distance)
//		 distance.unpersist(false)

		 distance = tempdistance.map({case (actor,(initDistance,newDistance))=>
			(actor,Math.min(initDistance,newDistance))})//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)	
		 joinRDDS = distance.join(actor2CoActorAll)
		distance = joinRDDS.map{case (actor,(dis,list))=>(list.map(_ -> dis))}
		 .flatMap(x=>x).map(xs => (xs._1 -> (xs._2 + 1))).reduceByKey((a,b)=>Math.min(a,b))//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		
		 }
	
		 val kevin2_0  = distance.map{case (id,distance)=>(id,if(id == idKevin) 0 else distance)} 
		 val idActorMap = actors2IdMap.map{case (actor,id)=>(id,actor)}
		 val distanceActor = kevin2_0.join(idActorMap)
		 val sortDistance= distanceActor.map{case (id,(distance,actor)) =>(distance,actor)}
		 val countOfEachKeys = sortDistance.countByKey()
		 val distance6 = sortDistance.filter(x=>x._1 == 6).map{case (dist,actor)=>(actor,dist)}.sortByKey()

		 val pw = new PrintWriter(new File("/home/agovindaraju/Documents/big-data/spark-1.5.0-bin-hadoop2.4/Template/scala/actors.txt"))
		 pw.write("actors\t")
		 pw.write(actorsCount.toString) 
		 pw.write("\n")
	         pw.write("movies\t")
		 pw.write(movieCount.toString)
		 pw.write("\n")
		 pw.write("distance to number of actors mapping \n")
		sc.parallelize(countOfEachKeys.toSeq).collect().foreach(ln=>pw.write(ln._1.toString+" "+ln._2.toString+"\n"))
		 //pw.write(countOfEachKeys.map(_.toString))
		 pw.write("Actors with distance of 6\n")
		 distance6.collect().map(ln=>(pw.write(ln._1.toString+"\n")))
		 pw.close()
		 //.saveAsTextFile("/home/agovindaraju/Documents/big-data/spark-1.5.0-bin-hadoop2.4/Template/scala/results.txt")
//		 distance6.saveAsTextFile("/home/agovindaraju/Documents/big-data/spark-1.5.0-bin-hadoop2.4/Template/scala/distance.txt")
	
		
		// Write to "actors.txt", the following:
		//	Total number of actors
		//	Total number of movies (and TV shows etc)
		//	Total number of actors at distances 1 to 6 (Each distance information on new line)
		//	The name of actors at distance 6 sorted alphabetically (ascending order), with each actor's name on new line
		
		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	} 

}
