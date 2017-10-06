# Frequent-Itemset-Discovery
Spark implementation of SON algorithm

## Background:
Given a set of baskets, SON algorithm divides them into chunks/partitions and then proceed in two stages.
First, local frequent itemsets are collected, which form candidates; next, it makes second pass through data to determine which candidates are globally frequent.


Stage 1 involves A-priori implementation for local frequent itemset discovery.

## Input file:
baskets.txt is a text file which contains a basket (a list of comma-separated item numbers) per line. For example
1,2,3
1,2,5
1,3,4
2,3,4
1,2,3,4
2,3,5
1,2,4
1,2
1,2,3
1,2,3,4,5

## Output Format:
all frequent itemsets, represented line by line.
For example,
4
1,3,4
1,2,3
2
1,3
2,4
2,3
1
2,3,4
1,4
3
3,4
1,2,4
2,5
1,2
5

## Running Instructions:
The program takes 3 arguments:
<li> Input file </li>
<li> Minimum support ratio </li>
<li> Output file </li>

2. All basket item are positive integers.

### Run as:
bin/spark-submit son.py baskets.txt .3 output.txt

