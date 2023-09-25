# Book Word Frequency Counter

This script processes a text file to compute and display the frequency of each word. Words are converted to lowercase, and the results are sorted in descending order by frequency. The top 50 most frequent words are then displayed.

## Dependencies

- **PySpark**: Ensure that PySpark is installed and correctly configured.

## Usage

1. Ensure that PySpark is installed on your system. If not, you can install it using pip:
```
pip install pyspark
```

2. Modify the path `file:///SparkCourse/book.txt` within the script to point to your desired text file.

3. Run the script:
```
python word_frequency_counter.py
```

This will display the word frequencies of the specified text file.

## Output

The script will output the top 50 words from the specified file, sorted by their frequency in descending order.

Example:
```
+-------+-----+
|   word|count|
+-------+-----+
|    the| 1200|
|     of|  900|
|    and|  800|
|     to|  700|
|     in|  600|
|     is|  500|
...
+-------+-----+
```

## Note

The script utilizes Spark's DataFrame operations for efficient processing. Ensure that your Spark environment is correctly configured before running the script.
