# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.bronze import BronzeIngestionJob, BronzeTableConfig

BronzeIngestionJob(spark, BronzeTableConfig(table="POS_CASH_balance")).run()
