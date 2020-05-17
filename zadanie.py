#!/usr/bin/env python2.7
#spustenie na serveri s pythonom 2.7


#Importy
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.mllib.random import RandomRDDs
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.classification import DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.util import MLUtils


sc = SparkContext(appName="example23")
print(u'Python version ' + sys.version)
print(u'Spark version ' + sc.version)
spark = SparkSession.builder.appName("Zadanie").getOrCreate()
sqlContext = SQLContext(sc)

#nacitam subor Accidents
data=spark.read.format('csv').options(header='true', inferSchema='true').load('/home/student/data/Accidents.csv')
#nacitam subor Vehicles
data1=spark.read.format('csv').options(header='true', inferSchema='true').load('/home/student/data/Vehicles.csv')
#nacitam subor Casualties
data2=spark.read.format('csv').options(header='true', inferSchema='true').load('/home/student/data/Casualties.csv')

#premenujem klucove atributy na zaklade ktorych spajame subory do jedneho
data1=data1.withColumnRenamed("Accident_Index","ID")
data2=data2.withColumnRenamed("Accident_Index","IDE")
data2=data2.withColumnRenamed("Vehicle_Reference","Vehicle_Ref")

#spojime Accidents a Vehicles na zaklade Accident_Index do tabulky "merge"
merge = data.join(data1, data.Accident_Index == data1.ID)
#potom spojime "merge" s Casualties tak, ze sa musia zhodovat v Accident_Index aj v Vehicle_Reference
cond = [merge.Accident_Index == data2.IDE, merge.Vehicle_Reference == data2.Vehicle_Ref]
full_merge = merge.join(data2, cond)

print(full_merge.count())



newsdf = full_merge.withColumn("Accident_Severity", when(full_merge["Accident_Severity"] == 1, 0).otherwise(full_merge["Accident_Severity"]))
newsdf = newsdf.withColumn("Accident_Severity", when(newsdf["Accident_Severity"] == 2, 1).otherwise(newsdf["Accident_Severity"]))
newsdf = newsdf.withColumn("Accident_Severity", when(newsdf["Accident_Severity"] == 3, 1).otherwise(newsdf["Accident_Severity"]))

newsdf.registerTempTable("TempTable")
mrtvy = sqlContext.sql('SELECT * FROM TempTable WHERE Accident_Severity = 0')
nemrtvy = sqlContext.sql('SELECT * FROM TempTable WHERE Accident_Severity = 1')

vzorka_mrtvy = mrtvy.sampleBy("Accident_Severity", fractions = {0: 1}, seed = 0)
vzorka_nemrtvy = nemrtvy.sampleBy("Accident_Severity", fractions = {1: 0.02}, seed = 0)

vzorka_cela = vzorka_mrtvy.union(vzorka_nemrtvy)

type_counts = vzorka_cela.groupBy("Accident_Severity").count()
type_counts.orderBy(["count", "Accident_Severity"], ascending=[0, 1]).show()

vzorka_cela = vzorka_cela.drop(
					 "Local_Authority_(Highway)",
 					 "LSOA_of_Accident_Location",
					 "Time",
					 "Date",
					 "Accident_Index",
					 "ID",
					 "IDE")

#zistime korelacie atributov
names = vzorka_cela.schema.names
vzorka_cela.printSchema()

correlations = []
i = 1
names = vzorka_cela.schema.names
for name in names:
   	correlations.extend([vzorka_cela.stat.corr('Accident_Severity',name)])
	print(i)
   	i += 1

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

#modelovanie
SVM_df = nova_vzorka

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

training_data, test_data = SVM_vector_data.randomSplit([0.7, 0.3], seed=123)


#Modelovanie
print "---------------------------------------------------------------------"
print "-----------------------------Modelovanie-----------------------------"
print "---------------------------------------------------------------------"

#Decision tree classifier
print "-------------------------------------------------"
print "---------------DESICION TREE---------------"
print "-------------------------------------------------"

tree_classifier = DecisionTreeClassifier(featuresCol="features",labelCol="Accident_Severity",impurity="entropy",maxDepth=10, maxBins=100) 
tree_model = tree_classifier.fit(training_data)
predictions = tree_model.transform(test_data)
#print(tree_model.toDebugString)
test_error = predictions.filter(predictions["prediction"] != predictions["Accident_Severity"]).count() / float(test_data.count())
print "Testing error: {0:.4f}".format(test_error)
# Select example rows to display.
predictions.select("prediction", "Accident_Severity", "features").show(5)
#Model rozhodovacie stromu
print(tree_model.toDebugString)
#vyhodnotenie decision tree
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="Accident_Severity", rawPredictionCol="prediction", metricName='areaUnderROC')
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
Precision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
Recall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictions)
print('Accuracy score: ',acc)
print('f1: ',f1)
print('Precision: ',Precision)
print('Recall: ',Recall)
print('Auc: ',auc)
#kontingencna tabulka
cf = predictions.crosstab("prediction","Accident_Severity")
cf.show()


print "-------------------------------------------------"
print "---------------LogisticRegression---------------"
print "-------------------------------------------------"
#Logisticka regresia
lr = LogisticRegression(featuresCol = 'features', labelCol = 'Accident_Severity', maxIter=10)
lrModel = lr.fit(training_data)
predictions = lrModel.transform(test_data)
predictions.select("prediction", "Accident_Severity", "features").show(10)
print(predictions)
#kontingencna tabulka Logisticka regresia
cf = predictions.crosstab("prediction","Accident_Severity")
cf.show()
#vyhodnotenie Logisticka regresia
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="Accident_Severity", rawPredictionCol="prediction", metricName='areaUnderROC')
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
Precision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
Recall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictions)
print('Accuracy score: ',acc)
print('f1: ',f1)
print('Precision: ',Precision)
print('Recall: ',Recall)
print('Auc: ',auc)


# bayes 
print "-------------------------------------------------"
print "--------------------NaiveBayes-------------------"
print "-------------------------------------------------"
nb = NaiveBayes(smoothing=1.0, modelType="multinomial",featuresCol="features", labelCol="Accident_Severity")
 # train the model
model = nb.fit(training_data)
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
#kontingencna tabulka bayes
cf = predictions.crosstab("prediction","Accident_Severity")
cf.show()
#vyhodnotenie bayes
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="Accident_Severity", rawPredictionCol="prediction", metricName='areaUnderROC')
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
Precision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
Recall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictions)
print('Accuracy score: ',acc)
print('f1: ',f1)
print('Precision: ',Precision)
print('Recall: ',Recall)
print('Auc: ',auc)



#Random Forest
print "-------------------------------------------------"
print "------------------Random Forest------------------"
print "-------------------------------------------------"
# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="Accident_Severity", featuresCol="features",impurity="entropy", numTrees=10, maxBins=100)
# Train model.  This also runs the indexers.
model = rf.fit(training_data)
# Make predictions.
predictions = model.transform(test_data)
test_error = predictions.filter(predictions["prediction"] != predictions["Accident_Severity"]).count() / float(test_data.count())
print "Testing error: {0:.4f}".format(test_error)
print(model.toDebugString)
#kontingencna Random Forest
cf = predictions.crosstab("prediction","Accident_Severity")
cf.show()
#vyhodnotenie Random Forest
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="Accident_Severity", rawPredictionCol="prediction", metricName='areaUnderROC')
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
Precision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
Recall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictions)
print('Accuracy score: ',acc)
print('f1: ',f1)
print('Precision: ',Precision)
print('Recall: ',Recall)
print('Auc: ',auc)



print "-------------------------------------------------"
print "---------------------KMeans----------------------"
print "-------------------------------------------------"
#Trains a k-means model.
kmeans = KMeans().setK(3).setSeed(1234)
model = kmeans.fit(training_data)
# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(training_data)
print("Within Set Sum of Squared Errors = " + str(wssse))
print("------------------------------------------------")
# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)    
    print("------------------------------------------------")
#detekovanie anomalii
for center in centers:
    for point in center:
        if (point > 5 or -5 > point):
               print "anomalia: {0:.15f}".format(point)
	

	
#SVM
print "-------------------------------------------------"
print "-----------------------SVM-----------------------"
print "-------------------------------------------------"
svm_classifier = LinearSVC(featuresCol="features",labelCol="Accident_Severity")                  
svm_model = svm_classifier.fit(training_data)
predictions = svm_model.transform(test_data)
test_error = predictions.filter(predictions["prediction"] != predictions["Accident_Severity"]).count() / float(test_data.count())
print "Testing error: {0:.4f}".format(test_error)
#kontingencna tabulka SVM
cf = predictions.crosstab("prediction","Accident_Severity")
cf.show()
#vyhodnotenie SVM
evaluatorMulti = MulticlassClassificationEvaluator(labelCol="Accident_Severity", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="Accident_Severity", rawPredictionCol="prediction", metricName='areaUnderROC')
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
Precision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
Recall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})
auc = evaluator.evaluate(predictions)
print('Accuracy score: ',acc)
print('f1: ',f1)
print('Precision: ',Precision)
print('Recall: ',Recall)
print('Auc: ',auc)





