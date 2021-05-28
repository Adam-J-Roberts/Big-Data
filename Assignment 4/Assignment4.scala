// index 2 is age, index 3 is weight
//Read input and split
val file = sc.textFile("input.txt")
val keyVal = file.map(x => x.split(","))

//Find avg weight for each age
val result = keyVal.map(x => (x(2),x(3).toInt)) //Maps K:Age V: Weight
val newresult = result.mapValues(x => (x,1));     //Sets each age to quantity 1 (30,(90,1))
val Newresult = newresult.reduceByKey{ case(x,y) => ((x._1+y._1),(x._2+y._2))  } //Adds the weight and quantity
val avgResult = Newresult.mapValues{  case(x,y) => (x/y)}  //Divides weight and quantity

//Find Max weight
val result2 = keyVal.map(x => ( (x(1)+","+x(2))   ,x(3).toInt))  //Maps K:Name, Age V: Weight
val max = result2.values.max //Returns max value(wieght)
val maxResult = result2.filter{ case(a,b) => b == max }   //Maps K:Name,Age V:Max Weight



// output is the age, maximum weight
maxResult.collect.foreach(println)

//avgResult.collect.foreach(println)
avgResult.collect.foreach(println)