import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._

//download and prepare data
val file =sc.textFile("processed.hungarian.data")


val fileData = file.map{ x=> x.split(",")}
//1.age 5 chol  14.prediction
val parsedData = fileData.map( x=> (if(x(0)=="?"){-1} else{x(0).toDouble},
                        if(x(4)=="?"){-1} else{x(4).toDouble},
                        if(x(13)=="?"){-1} else{x(13).toDouble}))

val data = parsedData.map{ x=>
        val featurevector = Vectors.dense(x._1, x._2)
        val label = x._3
        LabeledPoint(label, featurevector)
        }

val categoricalfeatureinfo = Map[Int, Int] ()
val model = DecisionTree.trainClassifier(data, 2, categoricalfeatureinfo, "gini", 2, 100 ); 
//data taken in, how many possible outcomes for final result,           , algorithm, depth of tree, 100 is number of trees

//test 
val testData = Vectors.dense(41,336)

val prediction = model.predict(testData)
//println("Model Tree : \n " + model.toDebugString)
 
    
