# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.gold import DimBureauJob, GoldTableConfig

DimBureauJob(spark, GoldTableConfig(target="dim_bureau")).run()
