# Streaming Pipeline with Delta Lake
In this Techhub we are creating Databricks notebook for Building pipeline for handling Streaming data on delta lake. We are reading data from GCP PUB/SUB Topic.

## Bronze Table 
In Bronze table we gonna read raw data from GCP Pub/Sub topic and write into delta lake table.

## Sliver Table
In Sliver table we are Reading streaming vehicle events from bronze table, As for transformation we are going drop tax column and Perform Merge operation if same carID already present in silver table update the row otherwise insert a new row. For Each batch of data frame perform above merge operation then writing transformed silver data frame to the silver table.

## Gold Table
In Gold table we are Reading streaming data frame from silver table, As for we are performing aggregation on cars quantity and price for each brands and years. Afterwards, writing aggregated result to Gold table.
