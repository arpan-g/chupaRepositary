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
		/*setting the spark configuration change in the serializer to kryo serializer, 
		*parallelism to 400
		*executor memory to 2g
		*compress the rdd that is stored to cached*/
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.set("spark.default.parallelism", "400") 
		conf.set("spark.executor.memory","2g")
		conf.set("spark.rdd.compress","true")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)

		/*setting hadoop configuration*/
	 	val hadoopConf = new Configuration() 
	 	/*specifying the delimiter*/
    	hadoopConf.set("textinputformat.record.delimiter", "\n\n")	
		
		val sc = new SparkContext(conf)		
		println("Number of cores: " + args(0))
		println("Input file: " + inputFile)
		                                                                                                   
		
		var t0 = System.currentTimeMillis
		/*parse the input file*/
		val filteredMap = sc.newAPIHadoopFile(inputFile,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],hadoopConf)
					.map(pair => pair._2.toString).filter(_.nonEmpty)
		/*assign every actor with unique id*/			
		val tempactors2MoviesMap = filteredMap.map(x => x.split("\t+",2).toList).map(line => (line(0), line(1).split("\n\t+").toList))
		.zipWithUniqueId().persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		val actors2MoviesMap = tempactors2MoviesMap.map{case((actor,movie),id)=>(id,movie)}
		/* map containing actors and there associated id*/
		val actors2IdMap = tempactors2MoviesMap.map{case((actor,movie),id)=>(actor,id)}
		.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		tempactors2MoviesMap.unpersist(false)
		val actorsCount = actors2IdMap.count() //get the count of actors
			
		val idKevin = sc.parallelize(actors2IdMap.lookup(KevinBacon)).first //get the id for kevin bacon
		// create a map with distance equal to 100 for all actors other than kevin bacon
		val actors2DistanceMap = actors2IdMap.map{case (actor,id)=>(id,if(id == idKevin) 0 else 100)}
		.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER) 
		
		//get the movie count
		val actor2MovieMap = actors2MoviesMap.flatMapValues(x=>x)	
		//flatten the map to create actor - movie map from a actor to list of movies map	
		val movie2ActorMap = actor2MovieMap.map{case(actor,movie)=>(movie.split("\\[")(0).trim,List(actor))}
		val movieCount = movie2ActorMap.count()
		// create a map with movie as the key and all the actors acted in that movie the value
		val movie2ActorsMap = movie2ActorMap.reduceByKey((a,b)=>(b:::a))
	   	val actorsList = movie2ActorsMap.map{case (key,value)=>(value)}

		val actor2actorsList = actorsList.map{case (list)=>(list.map(_->list))}
		val actor2Coactor = actor2actorsList.flatMap(x=>x).map{case (a,b)=>(a,b.filter(x=>(x!=a)))}
		// create actor to coactor list 
		val actor2CoActorAll = actor2Coactor.reduceByKey((a,b)=>(b:::a))
		//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)	
		//assign actors there initiall distance
		var  joinRDDS = actors2DistanceMap.join(actor2CoActorAll)
		var distance  = joinRDDS.map{case (actor,(dis,list))=>(list.map(_ -> dis))}.flatMap(x=>x).map(xs => (xs._1 -> (xs._2 + 1)))
		.reduceByKey((a,b)=>Math.min(a,b))//.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		//iterate 5 times aftere initialization for the first time
		for(i<- 1 until 6){
			joinRDDS = distance.join(actor2CoActorAll)
		 	distance = joinRDDS.map{case (actor,(dis,list))=>(list.map(_ -> dis))}
		 	.flatMap(x=>x).map(xs => (xs._1 -> (xs._2 + 1))).reduceByKey((a,b)=>Math.min(a,b))
		 	val tempdistance = actors2DistanceMap.join(distance)
		 	distance.unpersist(true)
		 	distance = tempdistance.map({case (actor,(initDistance,newDistance))=>
			(actor,Math.min(initDistance,newDistance))}).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
		 }
		 /*get back the actor names from the ids*/
		val idActorMap = actors2IdMap.map{case (actor,id)=>(id,actor)}
		val distanceActor = distance.join(idActorMap)
		val sortDistance= distanceActor.map{case (id,(distance,actor)) =>(distance,actor)}.sortByKey(false)
		// count number of values for each key
		val countOfEachKeys = sortDistance.countByKey()
		//create a map of actors at distance 6 from kevin bacon.
		val distance6 = sortDistance.filter(x=>x._1 == 6).map{case (dist,actor)=>(actor,dist)}.sortByKey()
        //write to a text file
		val pw = new PrintWriter(new File("/home/agovindaraju/Documents/big-data/spark-1.5.0-bin-hadoop2.4/Template/scala/actors.txt"))
		pw.write("actors\t")
		pw.write(actorsCount.toString) 
		pw.write("\n")
	    	pw.write("movies\t")
		pw.write(movieCount.toString)
		pw.write("\n")
		pw.write("distance to number of actors mapping \n")
		sc.parallelize(countOfEachKeys.toSeq).collect().foreach(ln=>pw.write(ln._1.toString+" "+ln._2.toString+"\n"))
		pw.write("Actors with distance of 6\n")
		distance6.collect().map(ln=>(pw.write(ln._1.toString+"\n")))
		pw.close()
		 
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
