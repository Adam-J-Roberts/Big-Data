import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._


// download and prepare data
val text = sc.textFile("adult.data");                          

val splitData = text.map(x=> x.split(", "))                   

val tupple = splitData.map( x=> (if(x(9)=="Male"){1} else if(x(9)=="Female"){2} else{-1},
                        if(x(13)=="United-States"){1} else if(x(13)=="Canada"){2} else{-1},
                        if(x(14)=="<=50K"){1} else if(x(14)==">50K"){2} else{-1} ))


val parsedData = tupple.map(x=> Vectors.dense(x._1.toDouble, x._2.toDouble, x._3.toDouble))   
val kmeans = new KMeans()
kmeans.setK(4)
val model = kmeans.run(parsedData)

model.clusterCenters.foreach(println) 
model.predict(parsedData).foreach(println)

val testdata = model.predict(Vectors.dense(1,1,1))
println(testdata)