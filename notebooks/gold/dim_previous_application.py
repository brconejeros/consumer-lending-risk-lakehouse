# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.gold import DimPreviousApplicationJob, GoldTableConfig

DimPreviousApplicationJob(spark, GoldTableConfig(target="dim_previous_application")).run()
