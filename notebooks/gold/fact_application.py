# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.gold import FactApplicationJob, GoldTableConfig

FactApplicationJob(spark, GoldTableConfig(target="fact_application")).run()
