import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

//download and prepare data
val file =sc.textFile("processed.hungarian.data")


val fileData = file.map{ x=> x.split(",")}
val tupple = fileData.map( x=> (if(x(0)=="?"){-1} else{x(0).toDouble},
                        if(x(4)=="?"){-1} else{x(4).toDouble},
                        if(x(13)=="?"){-1} else{x(13).toDouble}))

val parsedData = tupple.map(x=> Vectors.dense(x._1, x._2))

val kmeans = new KMeans()
kmeans.setK(2)
val model = kmeans.run(parsedData)


model.clusterCenters.foreach(println) 
model.predict(parsedData).foreach(println)

val testdata = model.predict(Vectors.dense(41,336))
println(testdata)

    
