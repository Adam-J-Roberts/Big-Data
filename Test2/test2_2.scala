val file =  sc.textFile("adult.data")
val splitfile = file.map(x => x.split(", "));
val keyval = splitfile.map(x => (x(9), (x(13), x(14), 1)  ) ) //Maps K:gender V: country , income, 1
//Filter out men and women
val male = keyval.filter{ case (x,y) => x == "Male"}
val female = keyval.filter{ case (x,y) => x == "Female"}
//filter out US
val usmale = male.filter{ case(x,y) => y._1 =="United-States" }     
val usfemale = female.filter{ case(x,y) => y._1 =="United-States"} 
//Filter out CA
val camale = male.filter{ case(x,y) => y._1 =="Canada"}     
val cafemale = female.filter{ case(x,y) => y._1 =="Canada"}
//filter out US income
val usRichMale = usmale.filter{ case(x,y) => y._2 == ">50K"}
val usRichFemale = usfemale.filter{ case(x,y) => y._2 == ">50K"}
//filter out CA income
val caRichMale = camale.filter{ case(x,y) => y._2 == ">50K"}
val caRichFemale = cafemale.filter{ case(x,y) => y._2 == ">50K"}
//add all
val addAllUsMen = usRichMale.reduceByKey{ (x, y) => (x._1,x._2,x._3+y._3)}
val addAllCaMen = caRichMale.reduceByKey{ (x, y) => (x._1,x._2,x._3+y._3)}
val addAllUsWomen = usRichFemale.reduceByKey{ (x, y) => (x._1,x._2,x._3+y._3)}
val addAllCaWomen = caRichFemale.reduceByKey{ (x, y) => (x._1,x._2,x._3+y._3)}
//Print all
addAllUsMen.foreach(println)
addAllUsWomen.foreach(println)
addAllCaMen.foreach(println)
addAllCaWomen.foreach(println)
