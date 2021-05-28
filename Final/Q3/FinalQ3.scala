import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._

val file =sc.textFile("auto-mpg.data")
val fileData = file.map{ x=> x.split("\\s+")}
val parsedData = fileData.map( x=> (if(x(0)=="?"){-1} else{x(0).toDouble},
                        if(x(1)=="?"){-1} else{x(1).toDouble},
                        if(x(2)=="?"){-1} else{x(2).toDouble},
                        if(x(3)=="?"){-1} else{x(3).toDouble},
                        if(x(4)=="?"){-1} else{x(4).toDouble},
                        if(x(5)=="?"){-1} else{x(5).toDouble},
                        if(x(6)=="?"){-1} else{x(6).toDouble},
                        if(x(7)=="?"){-1} else{x(7).toDouble}))
                       

val data = parsedData.map{ x=>
        val featurevector = Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6, x._7)
        val label = x._8
        LabeledPoint(label, featurevector)
        }
val categoricalfeatureinfo = Map[Int, Int] ()
val model = DecisionTree.trainClassifier(data, 8, categoricalfeatureinfo, "gini", 3, 100 ); 
val testData = Vectors.dense(14,6,198,95,3761,8,70)

val prediction = model.predict(testData)
//println("Model Tree : \n " + model.toDebugString)
 
    
