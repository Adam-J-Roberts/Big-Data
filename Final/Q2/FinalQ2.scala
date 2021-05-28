val file =  sc.textFile("imports-85.data");
val splitfile = file.map(x => x.split(","));

//Maps K:make V:cost
val keyval = splitfile.map(x => (x(2), x(25))  ) 
//mapping unknowns in values
val cars = keyval.mapValues(x => (if(x == "?") {-1} else {x.toDouble}))
//reducing values by cost. Leaving only most expensive
val maxpricecar = cars.reduceByKey{ case(x,y) => if(x>y) {x} else {y} } 
maxpricecar.collect.foreach(println)