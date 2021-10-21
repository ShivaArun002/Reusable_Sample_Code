# -*- coding: utf-8 -*-
"""Scoring_file_Sample_.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1R3_qX1dsFOw2dNf0ztdeLT1wh9GQsRK9
"""

# Importing Packages
import datetime
import pickle
from getpass import getuser
from collections import defaultdict
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.metrics import (classification_report,precision_score,recall_score,roc_auc_score,f1_score,confusion_matrix)
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import (Bucketizer,QuantileDiscretizer,VectorAssembler,StandardScaler,OneHotEncoder,StringIndexer)
from pyspark.ml import Pipeline,PipelineModel
from pyspark.ml.stat import Correlation
from pyspark.ml.classification import GBTClassifier,GBTClassificationModel
from pyspark.mllib.evaluation import (BinaryClassificationMetrics,MulticlassMetrics)
import tensorflow as tf
import argparse
import subprocess
import os
import sys
import gc
gc.collect()

sc = SparkContext.getOrCreate()
hivecontext = HiveContext(sc)
hivecontext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

modelid = 'accessorypurchasedig' # Model ID to be used
spark = SparkSession.builder.enableHiveSupport().appName(modelid).getOrCreate()

# Parser arguments addition
if len(sys.argv) >= 4:
  parser = argparse.ArgumentParser(description = "My Parser")
  parser.add_argument("--env", default='', help = "db details.")
  parser.add_argument("--db_feat_data", default = '' , help = "input db.")
  parser.add_argument("--tbl_feat_data", default ='', help ="input table.")
  parser.add_argument("--db_pred_data", default='', help = "output db.")
  parser.add_argument("--tbl_pred_data", default ='', help "output table.")
  print(sys.argv)
  parsed_args = parser.parse_args(sys.argv[1:])
  envnmnt = parsed_args.env
  intable = parsed_args.db_feat_data + "." + parsed_args.tbl_feat_data
  outtable =parsed_args.db_pred_data + "." + parsed_args.tbl_pred_data
else:
  sys.exit("no env passed: pass input features db & table, output predictions db & table")

# Loading Saved pickle file
serialized_object_path = "hdfs:///user/ashiva/pipelineModel_obj.file" # Saved Serialized objects path (Such as median/mean value imputations)
propensity_model_path = "hdfs:///user/ashiva/pipelineModel.file" # Path of the trained Model file 
print('Propensity_Model_Path-->', propensity_model_path)
pipeline_model = PipelineModel.load(propensity_model_path) # Loading the pre trained Model
print('serialized_object_path-->', serialized_object_path)
serialized_objects_file = tf.io.gfile.GFile(serialized_object_path, 'rb') # converting serialized objects to GFile
serialized_objects = pickle.load(serialized_objects_file) # Loading Serialized objects using pickle
serialized_objects_file.close()

# Data Preprocessing
def pre_processing_(data_df , serialized_objects):
  """
    preprocess the test dataframe to be passed to model and get predictions
    :param data_df: dataframe to be preprocessed
    :param serialized_objects: dictionary to impute null values during prediction
    :return : preprocessed dataframe
    """
  max_recency_acc_dig = serialized_objects['max_recency_acc_dig'] # These values are taken from trained model values
  max_recency_dig_2yr = serialized_objects['max_recency_dig_2yr'] # These values are taken from trained model values
  max_acc_recency_mf  = serialized_objects['max_acc_recency_mf'] #These are values imported in training dataset. Same values needs to be used to impute missing values in unseen data

  data_df = data_df.na.fill({
      'recency_acc_dig' : max_recency_acc_dig, # Filling missing values
      'recency_dig_2yr' : max_recency_dig_2yr,
      'acc_recency_mf'  : max_acc_recency_mf
  })

  freq_acc_upg_2yrs_split = [-float('inf'), 0, 1, 2, float('inf')]
  bucketizer_freq_acc_upg_2yrs = Bucketizer(splits=freq_acc_upg_2yrs_split, inputCol='freq_acc_upg_acc_2yrs', outputCol='freq_acc_upg_acc_2yrs_bkt')
  data_df = bucketizer_freq_acc_upg_2yrs.setHandleInvalid('keep').transform(data_df) # Binning the freq_acc_upg_acc_2yrs column

  tot_purchase_split = [-float('inf'), 0, 1, 2, 3, float('inf')]
  bucketizer_tot_purchase = Bucketizer(splits=tot_purchase_split, inputCol='tot_accsry_purchse', outputCol='tot_accsry_purchse_bkt')
  data_df = bucketizer_tot_purchase.setHandleInvalid('keep').transform(data_df) # Binning the tot_accsry_purchse column

  del_cols_new = ['freq_acc_upg_acc_2yrs', 'tot_accsry_purchse']
  data_df = data_df.drop(*del_cols_new) # Dropping the older continuous columns
  return data_df

# Loading Features Data
feat_df = spark.sql('SELECT * FROM {}'.format(intable))

# Defining Predictions Function
def propensity_predictions(pipeline_model, serialized_objects, df, spark, hiveContext, outtable):
  """
    get predictions for preprocessed data frame and write them into the final table
    :param pipeline_model: pretrained model instance
    :param serialized_objects: dictionary to impute null values during prediction
    :param df : preprocessed data frame
    :param spark : Spark Context
    :param hiveContext : hiveContext
    :outtable : Final table to which the predictions need to be written into
    :return nothing
    """
  processed_df = pre_processing_(data_df = df, serialized_objects = serialized_objects)
  predictions = pipeline_model.transform(processed_df)
  finalCols = ['sor_id', 'cust_id', 'acct_num', 'rpt_mth', 'score_model_id', 'score_value', 'score_decile',
               'score_centile', 'area_cd', 'score_type', 'orig_score_value', 'high_score_ind'] # Mandatory columns that should be present in outtable i.e., predictions table
  to_array = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType())) 
  prop_final_df = predictions.withColumn('probability_value', to_array('probability')) . \
                                  withColumn('score_value', predictions['prediction'])

  prop_final_df = prop_final_df.withColumn('probability', prop_final_df.probability_value[1])
  discretizer_decile = QuantileDiscretizer(numBuckets =10, inputCol ='probability', outputCol='score_decile',relativeError=0.0)
  discretizer_centile = QuantileDiscretizer(numBuckets =100, inputCol ='probability', outputCol='score_centile',relativeError=0.0)
  prop_final_df = discretizer_decile.fit(prop_final_df).transform(prop_final_df)
  prop_final_df = discretizer_centile.fit(prop_final_df).transform(prop_final_df)
  prop_final_df = prop_final_df.withColumn('score_decile', prop_final_df['score_decile'] + lit(1)) . \
                                withColumn('score_centile', prop_final_df['score_centile'] + lit(1)) .\
                                withColumn('orig_score_value', prop_final_df['probability'])
  prop_final_df = prop_final_df.withColumn('high_score_ind', when(prop_final_df['score_centile'] > 75, 'Y').otherwise('N'))
  prop_final_df = prop_final_df.drop('probability')
  prop_final_df = prop_final_df.select(*finalCols)

  # Saving Scoring into the final table
  prop_final_df.createOrReplaceTempView('accessorypurchasedig_predictions_final')
  hiveContext.sql('drop table if exists' + outtable)
  print('out table name: ' + outtable)
  hiveContext.sql('create table ' + outtable + ' as select * from accessorypurchasedig_predictions_final')
  print(outtable + ' table is created')

propensity_pred = propensity_predictions(pipeline_model = pipeline_model, serialized_objects = serialized_objects, df = feat_df,
                                         spark = spark, hiveContext = hivecontext, outtable=outtable)