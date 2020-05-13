from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from pyspark.sql.functions import when, mean
from pyspark.sql import SQLContext

#nacitam subor Accidents
data=spark.read.format('csv').options(header='true', inferSchema='true').load('C:/Users/Kristian/Desktop/TSVD/dataset/Accidents.csv')
#nacitam subor Vehicles
data1=spark.read.format('csv').options(header='true', inferSchema='true').load('C:/Users/Kristian/Desktop/TSVD/dataset/Vehicles.csv')
#nacitam subor Casualties
data2=spark.read.format('csv').options(header='true', inferSchema='true').load('C:/Users/Kristian/Desktop/TSVD/dataset/Casualties.csv')

#premenujem klucove atributy na zaklade ktorych spajame subory do jedneho
data1=data1.withColumnRenamed("Accident_Index","ID")
data2=data2.withColumnRenamed("Accident_Index","IDE")
data2=data2.withColumnRenamed("Vehicle_Reference","Vehicle_Ref")

#spojime Accidents a Vehicles na zaklade Accident_Index do tabulky "merge"
merge = data.join(data1, data.Accident_Index == data1.ID)
full_merge = merge.join(data2, cond)

#potom spojime "merge" s Casualties tak, ze sa musia zhodovat v Accident_Index aj v Vehicle_Reference
cond = [merge.Accident_Index == data2.IDE, merge.Vehicle_Reference == data2.Vehicle_Ref]

newsdf = full_merge.withColumn("Accident_Severity", when(full_merge["Accident_Severity"] == 3, 2).otherwise(full_merge["Accident_Severity"]))

newsdf.registerTempTable("TempTable")
mrtvy = sqlContext.sql('SELECT * FROM TempTable WHERE Accident_Severity = 1')
nemrtvy = sqlContext.sql('SELECT * FROM TempTable WHERE Accident_Severity = 2')

vzorka_mrtvy = mrtvy.sampleBy("Accident_Severity", fractions = {1: 1}, seed = 0)
vzorka_nemrtvy = nemrtvy.sampleBy("Accident_Severity", fractions = {2: 0.02}, seed = 0)

vzorka_cela = vzorka_mrtvy.union(vzorka_nemrtvy)

#zistime korelacie atributov
correlations = []
names = vzorka_cela.schema.names
for name in names:
   	correlations.extend([vzorka_cela.stat.corr('Accident_Severity',name)])


t = zip(correlations, names)
tt = spark.createDataFrame(t)

tt.registerTempTable("TempTable")
atributy_table = sqlContext.sql('SELECT * FROM TempTable WHERE _2 > 0.14 OR _2 <-0.14')	

#zistili sme ktore atributy maju najvacsi vplyv na cielovy atribut tak som si ich tu vypisal rucne, s tymi budeme dalej pracovat
atributes = ["Number_of_Vehicles",
			 "Number_of_Casualties",
			 "1st_Road_Class",
			 "Speed_limit",
			 "Junction_Detail",
			 "Light_Conditions",
			 "Urban_or_Rural_Area",
			 "Did_Police_Officer_Attend_Scene_of_Accident",
			 "Junction_Location",
			 "Skidding_and_Overturning",
			 "Hit_Object_off_Carriageway",
			 "Accident_Severity"]


#vytvorim novu tabulku uz iba s mojimi atributmi
nova_vzorka = vzorka_cela.select(atributes)


#odstranim -1 hodnoty
names = nova_vzorka.schema.names
for name in names:
	type_counts = nova_vzorka.groupBy(name).count()
	type_counts = type_counts.orderBy(["count", name], ascending=[0, 1])

	moj_list = type_counts.select(name).collect()
	najcastejsia_hodnota = moj_list[0][0]

	nova_vzorka = nova_vzorka.withColumn(name, when(nova_vzorka[name] == -1, najcastejsia_hodnota).otherwise(nova_vzorka[name]))

	
#skontrolujem ci su tam este -1 hodnoty ( malo by vypisat same nuly)
names = nova_vzorka.schema.names
for name in names:
	x = nova_vzorka.filter(nova_vzorka[name] == -1).count()
	print(x)

	
	
#upravim hodnoty niektorych atributov, lebo to zvysilo presnost korelacie 

nova_vzorka = nova_vzorka.withColumn("Hit_Object_off_Carriageway", when(
		(nova_vzorka["Hit_Object_off_Carriageway"] == -1) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 1) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 2) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 3) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 4) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 5) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 6) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 7) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 8) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 9) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 10) |
		(nova_vzorka["Hit_Object_off_Carriageway"] == 11),1).otherwise(nova_vzorka["Hit_Object_off_Carriageway"]))
   
   
nova_vzorka = nova_vzorka.withColumn("Junction_Detail", when(
		(nova_vzorka["Junction_Detail"] == 1) |
		(nova_vzorka["Junction_Detail"] == 2) |
		(nova_vzorka["Junction_Detail"] == 3) |
		(nova_vzorka["Junction_Detail"] == 4) |
		(nova_vzorka["Junction_Detail"] == 5) |
		(nova_vzorka["Junction_Detail"] == 6) |
		(nova_vzorka["Junction_Detail"] == 7) |
		(nova_vzorka["Junction_Detail"] == 8) |
		(nova_vzorka["Junction_Detail"] == 9), 1).otherwise(nova_vzorka["Junction_Detail"]))
    
nova_vzorka = nova_vzorka.withColumn("Junction_Location", when(
		(nova_vzorka["Junction_Location"] == 1) |
		(nova_vzorka["Junction_Location"] == 2) |
		(nova_vzorka["Junction_Location"] == 3) |
		(nova_vzorka["Junction_Location"] == 4) |
		(nova_vzorka["Junction_Location"] == 5) |
		(nova_vzorka["Junction_Location"] == 6) |
		(nova_vzorka["Junction_Location"] == 7) |
		(nova_vzorka["Junction_Location"] == 8), 1).otherwise(nova_vzorka["Junction_Location"]))
    
nova_vzorka = nova_vzorka.withColumn("Skidding_and_Overturning", when(
		(nova_vzorka["Skidding_and_Overturning"] == 1) |
		(nova_vzorka["Skidding_and_Overturning"] == 2) |
		(nova_vzorka["Skidding_and_Overturning"] == 3) |
		(nova_vzorka["Skidding_and_Overturning"] == 4) |
		(nova_vzorka["Skidding_and_Overturning"] == 5), 1).otherwise(nova_vzorka["Skidding_and_Overturning"]))

#SVM model
SVM_df = nova_vzorka.withColumn("Accident_Severity", when(nova_vzorka["Accident_Severity"] == 1, 0).otherwise(nova_vzorka["Accident_Severity"]))
SVM_df = SVM_df.withColumn("Accident_Severity", when(nova_vzorka["Accident_Severity"] == 2, 1).otherwise(nova_vzorka["Accident_Severity"]))

atr_without_severity = ["Number_of_Vehicles",
			 "Number_of_Casualties",
			 "1st_Road_Class",
			 "Speed_limit",
			 "Junction_Detail",
			 "Light_Conditions",
			 "Urban_or_Rural_Area",
			 "Did_Police_Officer_Attend_Scene_of_Accident",
			 "Junction_Location",
			 "Skidding_and_Overturning",
			 "Hit_Object_off_Carriageway"]

SVM_vector_data = VectorAssembler(inputCols=atr_without_severity,
        outputCol="features").transform(SVM_df) 

SVMtraining_data, SVMtest_data = SVM_vector_data.randomSplit([0.7, 0.3], seed=123)

svm_classifier = LinearSVC(
        featuresCol="features",             
        labelCol="Accident_Severity")                  

svm_model = svm_classifier.fit(SVMtraining_data)

predictions = svm_model.transform(SVMtest_data)

test_error = predictions.filter(predictions["prediction"] != predictions["Accident_Severity"]).count() / float(SVMtest_data.count())
print "Testing error: {0:.4f}".format(test_error)




