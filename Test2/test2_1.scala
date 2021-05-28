val file =  sc.textFile("imports-85.data");
val splitfile = file.map(x => x.split(","));

//Maps K:fuel V: (make , cost)
val keyval = splitfile.map(x => (x(3), (x(2),x(25))  ) ) 
//mapping unknowns in values
val processkeyval = keyval.mapValues(x => (if(x._1 == "?") {"Anon"} else {x._1}, if(x._2 == "?") {-1} else {x._2.toDouble}))
//filtering out keys: leaving only diesel
val dieselcars = processkeyval.filter{ case (a,b) => a == "diesel"}
//reducing values by cost. Leaving only most expensive
val maxpricecar = dieselcars.reduceByKey{ case(x,y) => if(x._2>y._2) {x} else {y} } 
maxpricecar.collect.foreach(println)

