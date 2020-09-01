# Dealing with the data stream

## Project description
This project is a mini-project in class Data Mining. The goal is to be familiar with dealing with the data stream. This project uses the Bloom Filtering algorithm, Flajolet-Martin algorithm, and Fixed Size Sample.

## Programming language and libraries
Python, Spark Streaming library, tweepy

## Procedure
- Generated a simulate data stream from the Yelp dataset.
- Estimated the existence of a coming city in the data stream using the Bloom Filtering algorithm, improved accuracy by finding the proper hash functions.
- Estimated the number of unique cities within a 30-second-length window using the Flajolet-Martin algorithm.
- Found popular tags on tweets from Twitter API of streaming by Reservoir Sampling Algorithm ( Fixed Size Sampling).
