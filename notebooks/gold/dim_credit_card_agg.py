# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))

from src.lakehouse.gold import DimCreditCardAggJob, GoldTableConfig

DimCreditCardAggJob(spark, GoldTableConfig(target="dim_credit_card_agg")).run()
