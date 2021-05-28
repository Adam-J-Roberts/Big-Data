import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._


// download and prepare data
val file = sc.textFile("input_pet.csv");
val header = file.first();
// return the data if x doesn't equal to header
val rawData = file.filter(x => x != header);
val fileData = rawData.map{ x=> x.split(",")}

val parsedData = fileData.map( x=> (
                        if(x(1)=="?"){-1} else {x(1).toDouble},   //weight
                        if(x(2)=="?"){-1} else {x(2).toDouble} ,  // legs
                        if(x(3)=="Brown") {1} else if(x(3)=="Grey"){2} else if(x(3)=="Tan"){3} else if(x(3)=="Green"){4} else {0},  //color
                        if(x(4)=="Y"){1} else {0}))  //decision

val data = parsedData.map { x=>
        val featurevector = Vectors.dense(x._1, x._2, x._3)
        val label = x._4
        LabeledPoint(label, featurevector)
        }
// training
val categoricalFeatureInfo = Map[Int, Int] () ((3,5))
val model = DecisionTree.trainClassifier(data, 2, categoricalFeatureInfo, "gini", 7, 100)

// testing
val testData = Vectors.dense(25, 4, 2)
val prediction = model.predict(testData)

println("Model Tree:\n" + (model.toDebugString))

