# -*- coding: utf-8 -*-
"""
Created on Fri Dec  3 22:48:06 2021

@author: ali_a
"""
import sys
install_requires=[
    'pyspark=={site.SPARK_VERSION}'
    ]

#On import pyspark 
import pyspark
from pyspark import SparkContext, SparkConf
print('pyspark version : ', pyspark.__version__)

#Instantiation l'API de spark pour les RDD (spark context).
conf =SparkConf().setAppName("wordCount").setMaster("local")
sc = SparkContext(conf=conf)

#On importe le fichier sample.txt
distFile = sc.textFile('sample.txt')

count = distFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a,b :a+b)

#On affiche le contenu de count
print(count.collect())

#On exporte le contenu de count (RDD) dans un fichier au format .txt
count.coalesce(1).saveAsTextFile("LeCompteur")
