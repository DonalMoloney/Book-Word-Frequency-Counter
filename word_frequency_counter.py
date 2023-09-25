"""
This script processes a text file to compute and display the frequency of each word.
The words are converted to lowercase, and the results are sorted in descending order
by their frequency. The top 50 most frequent words are then displayed.

Dependencies:
- PySpark

Usage:
- Ensure that PySpark is installed and correctly configured.
- Modify the path "file:///SparkCourse/book.txt" to point to the desired text file.
- Run the script to view the word frequencies.
"""


from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Initialize Spark session with application name "WordCount"
spark_session = SparkSession.builder.appName("WordCount").getOrCreate()

# Load the content of the book from the given path into a DataFrame
input_text_df = spark_session.read.text("file:///SparkCourse/book.txt")

# Convert the text into individual words and then explode
words_temp_df = input_text_df.select(func.explode(func.split(input_text_df.value, "\\W+")).alias("word"))

# Convert the words into lowercase, excluding empty strings
cleaned_words_df = (words_temp_df
                    .select(func.lower(words_temp_df.word).alias("word"))
                    .filter(func.col("word") != ""))

# Compute the frequency of each word
word_counts_df = cleaned_words_df.groupBy("word").count()

# Sort the word frequencies in descending order to get most frequent words first
sorted_word_counts_df = word_counts_df.sort(func.desc("count"))

# Display the top 50 frequent words
sorted_word_counts_df.show(n=50)

# Close the Spark session to free up resources
spark_session.stop()
