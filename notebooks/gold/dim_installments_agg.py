# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.gold import DimInstallmentsAggJob, GoldTableConfig

DimInstallmentsAggJob(spark, GoldTableConfig(target="dim_installments_agg")).run()
