import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._

//download and prepare data
val file =sc.textFile("input_ass5.txt")
val header = file.first()
val rawData = file.filter(x => x!=header)

val data = rawData.map{ x=>
    val values = x.split(",").map(x=> x.toDouble)
    val featurevector = Vectors.dense(values.init)
    val label = values.last
    LabeledPoint(label, featurevector)
    }

//training
val categoricalfeatureinfo = Map[Int, Int] ()
val model = DecisionTree.trainClassifier(data, 2, categoricalfeatureinfo, "gini", 7, 100 )
//data taken in, how many catigories for predicted value,           , algorithm, depth of tree, 100 is number of trees

//test 
val testData = Vectors.dense(3,1,2,1,0,1)

val prediction = model.predict(testData)
println("Model Tree : \n " + model.toDebugString)