import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

val file =sc.textFile("auto-mpg.data")
val fileData = file.map{ x=> x.split("\\s+")}
val tupple = fileData.map( x=> (if(x(0)=="?"){-1} else{x(0).toDouble},
                        if(x(1)=="?"){-1} else{x(1).toDouble},
                        if(x(2)=="?"){-1} else{x(2).toDouble},
                        if(x(3)=="?"){-1} else{x(3).toDouble},
                        if(x(4)=="?"){-1} else{x(4).toDouble},
                        if(x(5)=="?"){-1} else{x(5).toDouble},
                        if(x(6)=="?"){-1} else{x(6).toDouble},
                        if(x(7)=="?"){-1} else{x(7).toDouble}))

val parsedData = tupple.map(x=> Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6, x._7))
val kmeans = new KMeans()
kmeans.setK(8)
val model = kmeans.run(parsedData)
model.clusterCenters
model.predict(parsedData)
val testdata = model.predict(Vectors.dense(14,6,198,95,3761,8,70))
println(testdata)
