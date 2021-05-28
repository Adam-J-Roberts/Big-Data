val file = sc.textFile("UNSW-NB15_1.csv")
 
val splitfile = file.map(x => x.split(","));
 
// create map  Key=Start time     Value =(IP, best arrival time)
val keyval = splitfile.map(x => (x(28), (x(0), (if( x(30).toDouble>x(31).toDouble ) {x(30).toDouble} else {x(31).toDouble}) ) )  ) 


// create map //Key=StartTime  Value=best arrival time               for finding max
val newResult = splitfile.map(x => (x(28), (if( x(30).toDouble>x(31).toDouble ) {x(30).toDouble} else {x(31).toDouble}) )  )      
//find max time for each key  Key=StartTime Value=Max time
val findMax = newResult.reduceByKey{case (a,b) => if (a>b){a} else {b} }



//Join 2 key/value pairs Key=StartTime Value=[(IP, ArrivalTime), MaxTime
val data = keyval.join(findMax)
//Compares MaxTime With ArrivalTime for each key and only leaves ones with max
val findIPs = data.filter{ case(x,y) => y._1._2==y._2 }

//adds a one for counting frequency
val frequencyCounter = findIPs.mapValues(x => ((x._1._1,1)));  


//add frequency
val frequencyTotal = frequencyCounter.reduceByKey{case (x,y) => (x._1,x._2+y._2)}

//seperates frequency to find max
val frequency = frequencyTotal.mapValues(x => ((x._2)));  
val max = frequency.values.max

//val test2 = Newerresult.values()
//filter out the attacks with the same max frequency
val results = frequencyTotal.filter{ case(x,y) => y._2==max}
results.collect.foreach(println)
