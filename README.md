# Data Processing using PySpark

## Task 1: DataFrame Creation with REGEX

Each member will define a custom schema using REGEX to extract specific metrics from the dataset.

### Student Metrics to Extract

- **Student 1**: IP Address, Timestamp, HTTP Method  
   **REGEX Example**: `(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] \"([A-Z]+)`
- **Student 2**: HTTP Status Code, Response Size, Timestamp  
   **REGEX Example**: `\".*\" (\d+) (\d+) \[(.*?)\]`
- **Student 3**: URL Path, IP Address, Response Size  
   **REGEX Example**: `\"[A-Z]+ (\/.*?) HTTP.* (\d+\.\d+\.\d+\.\d+) (\d+)`
- **Student 4**: Log Message, HTTP Status Code, Timestamp  
   **REGEX Example**: `\".*\" (\d+) .* \[(.*?)\] (.*)`

## Task 2: Two Advanced DataFrame Analysis

Each member will write unique SQL queries for the analysis:

### SQL Query 1: Window Functions

- **Student 1**: Rolling hourly traffic per IP  
   **Description**: Calculate traffic count per IP over a sliding window.
- **Student 2**: Session identification  
   **Description**: Identify sessions based on timestamp gaps.
- **Student 3**: Unique visitors per hour  
   **Description**: Count distinct IPs for each hour.
- **Student 4**: Average response size per status code  
   **Description**: Compute averages grouped by status codes.

### SQL Query 2: Aggregation Functions

- **Student 1**: Traffic patterns by URL path  
   **Description**: Analyze URL visits by hour.
- **Student 2**: Top 10 failed requests by size  
   **Description**: Identify the largest failed requests.
- **Student 3**: Response size distribution by status  
   **Description**: Show min, max, and avg sizes for each status.
- **Student 4**: Daily unique visitors  
   **Description**: Count unique IPs per day.

## Task 3: Data Visualization using Matplotlib and Seaborn

Each member will visualize the results of their unique SQL queries using different chart types.

### Student Visualization Type Examples

- **Student 1**: Line Chart (Hourly Traffic)  
   **Tool**: Matplotlib for rolling traffic visualization.
- **Student 2**: Bar Chart (Top 10 Failed Requests)  
   **Tool**: Seaborn for aggregated failure counts.
- **Student 3**: Heatmap (Hourly Unique Visitors)  
   **Tool**: Seaborn for visualizing traffic density.
- **Student 4**: Pie Chart (Response Code Distribution)  
   **Tool**: Matplotlib for status code proportions.

# Data Processing using PySpark RDD

## Task 1: Basic RDD Analysis

Each member will create a custom function to parse and process the log entries.

### Student Basic Extraction Examples

- **Student 1**: Extract Timestamp and IP  
   **Description**: Parse timestamp and IP address from logs.
- **Student 2**: Extract URL and HTTP Method  
   **Description**: Parse URL path and HTTP method from logs.
- **Student 3**: Extract Status Code and Response Size  
   **Description**: Parse HTTP status and response size from logs.
- **Student 4**: Extract Log Message and IP Address  
   **Description**: Parse log messages and corresponding IP addresses.

## Task 2: Two Advanced RDD Analysis

Each member will perform unique advanced processing tasks.

### Student Advanced Analysis Examples

- **Student 1**: Calculate hourly visit counts per IP  
   **Description**: Count visits grouped by hour and IP.
- **Student 2**: Identify top 10 URLs by visit count  
   **Description**: Aggregate visit counts and rank top URLs.
- **Student 3**: Find average response size per URL  
   **Description**: Compute average response size for each URL.
- **Student 4**: Detect failed requests per IP  
   **Description**: Identify IPs with the most failed requests.

## Optimization and LSEPI Considerations

Each member chooses two unique methods for optimization.

### Student Optimization Methods

- **Student 1**: Partition Strategies, Caching
- **Student 2**: Caching, Bucketing & Indexing
- **Student 3**: Partition Strategies, Bucketing & Indexing
- **Student 4**: Caching, Partition Strategies

## Starter Code

```json
{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "collapsed_sections": ["Lx9-Fre4FMda"]
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "Lx9-Fre4FMda"
      },
      "source": [
        "# Big Data Analytics [CN7031] CRWK 2024-25\n",
        "# Group ID: [Your Group ID]\n",
        "1.   Student 1: Name and ID\n",
        "2.   Student 2: Name and ID\n",
        "3.   Student 3: Name and ID\n",
        "4.   Student 4: Name and ID\n",
        "\n",
        "---\n",
        "\n",
        "If you want to add comments on your group work, please write it here for us:"
      ]
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "GdMZR-9QTwG3"
      },
      "source": ["\n", "# Initiate and Configure Spark\n", "\n", "---\n"]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "2wbXV70D6xbl"
      },
      "source": ["!pip3 install pyspark"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "_z0p88Xtw_3-"
      },
      "source": ["# linking with Spark\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "6P2CZVl6TOQX"
      },
      "source": ["# Load Unstructured Data\n", "\n", "---\n"]
    },
    {
      "cell_type": "code",
      "source": [
        "# Load the unstructured data: (1) drag and drop data on the \"Files\" section or (2) use Google Drive"
      ],
      "metadata": {
        "id": "efdQkCg_soaq"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "-RjT7_UHAqic"
      },
      "source": [
        "\n",
        "# Task 1: Data Processing using PySpark DF [40 marks]\n",
        "\n",
        "---\n",
        "\n"
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 1 (Name and ID)\n",
        "\n",
        "- DF Creation with REGEX (10 marks)\n",
        "- Two advanced DF Analysis (20 marks)\n",
        "- Utilize data visualization (10 marks)"
      ],
      "metadata": {
        "id": "LSE7bNND4caH"
      }
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "7fCFTcOQBLD2"
      },
      "source": ["# Task 1 - Student 1\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 2 (Name and ID)\n",
        "\n",
        "- DF Creation with REGEX (10 marks)\n",
        "- Two advanced DF Analysis (20 marks)\n",
        "- Utilize data visualization (10 marks)"
      ],
      "metadata": {
        "id": "QkJNiyVu4tKK"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 1 - Student 2\n"],
      "metadata": {
        "id": "kHPoRpSD4vYW"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 3 (Name and ID)\n",
        "\n",
        "- DF Creation with REGEX (10 marks)\n",
        "- Two advanced DF Analysis (20 marks)\n",
        "- Utilize data visualization (10 marks)"
      ],
      "metadata": {
        "id": "JFiwgD4H4vph"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 1 - Student 3\n"],
      "metadata": {
        "id": "-TZIFMZB4xFZ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 4 (Name and ID)\n",
        "\n",
        "- DF Creation with REGEX (10 marks)\n",
        "- Two advanced DF Analysis (20 marks)\n",
        "- Utilize data visualization (10 marks)"
      ],
      "metadata": {
        "id": "F7AQAa574xnx"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 1 - Student 4\n"],
      "metadata": {
        "id": "5KsnRrDK4zRB"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "dcJhGbI2BKpx"
      },
      "source": [
        "\n",
        "# Task 2 - Data Processing using PySpark RDD [40 marks]\n",
        "\n",
        "---\n"
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 1 (Name and ID)\n",
        "\n",
        "- One Basic RDD Analysis (10 marks)\n",
        "- Two Advanced RDD Analysis (30 marks)"
      ],
      "metadata": {
        "id": "mDEDGQOh450o"
      }
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "V3eiN9geBQRf"
      },
      "source": ["# Task 2 - Student 1\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 2 (Name and ID)\n",
        "\n",
        "- One Basic RDD Analysis (10 marks)\n",
        "- Two Advanced RDD Analysis (30 marks)"
      ],
      "metadata": {
        "id": "92RPdoeV5SHz"
      }
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "FQ_-hgdeiMle"
      },
      "source": ["# Task 2 - Student 2\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 3 (Name and ID)\n",
        "\n",
        "- One Basic RDD Analysis (10 marks)\n",
        "- Two Advanced RDD Analysis (30 marks)"
      ],
      "metadata": {
        "id": "y7MY1leq5USZ"
      }
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "2JGQHXYliMK5"
      },
      "source": ["# Task 2 - Student 3\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 4 (Name and ID)\n",
        "\n",
        "- One Basic RDD Analysis (10 marks)\n",
        "- Two Advanced RDD Analysis (30 marks)"
      ],
      "metadata": {
        "id": "n8G2vN3g5Vua"
      }
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "A5mwMvIsBQlX"
      },
      "source": ["# Task 2 - Student 4\n"],
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "wHft1Jht1Qxl"
      },
      "source": [
        "# (3) Optimization and LSEPI (Legal, Social, Ethical, and Professional Issues) Considerations [10 marks]\n",
        "\n",
        "---\n"
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 1 (Name and ID)\n",
        "\n",
        "Choose two out of the following three methods to apply. Compare results with and without optimization for the chosen methods.\n",
        "\n",
        "- Different Partition Strategies (5 Marks)\n",
        "  - Explore and evaluate various strategies for partitioning data.\n",
        "\n",
        "- Caching vs. No Caching (5 Marks)\n",
        "  - Analyze the impact of caching data versus not caching.\n",
        "\n",
        "- Bucketing and Indexing (5 Marks)\n",
        "  - Investigate the effects of bucketing and indexing on data operations."
      ],
      "metadata": {
        "id": "95m9jb8f5d_s"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 3 - Student 1\n"],
      "metadata": {
        "id": "8dbo5dG25ra2"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 2 (Name and ID)\n",
        "\n",
        "Choose two out of the following three methods to apply. Compare results with and without optimization for the chosen methods.\n",
        "\n",
        "- Different Partition Strategies (5 Marks)\n",
        "  - Explore and evaluate various strategies for partitioning data.\n",
        "\n",
        "- Caching vs. No Caching (5 Marks)\n",
        "  - Analyze the impact of caching data versus not caching.\n",
        "\n",
        "- Bucketing and Indexing (5 Marks)\n",
        "  - Investigate the effects of bucketing and indexing on data operations."
      ],
      "metadata": {
        "id": "cQpYG-4k5rrq"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 3 - Student 2\n"],
      "metadata": {
        "id": "8ZTAGJiz5tIX"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 3 (Name and ID)\n",
        "\n",
        "Choose two out of the following three methods to apply. Compare results with and without optimization for the chosen methods.\n",
        "\n",
        "- Different Partition Strategies (5 Marks)\n",
        "  - Explore and evaluate various strategies for partitioning data.\n",
        "\n",
        "- Caching vs. No Caching (5 Marks)\n",
        "  - Analyze the impact of caching data versus not caching.\n",
        "\n",
        "- Bucketing and Indexing (5 Marks)\n",
        "  - Investigate the effects of bucketing and indexing on data operations."
      ],
      "metadata": {
        "id": "thZJwceS5tX7"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 3 - Student 3\n"],
      "metadata": {
        "id": "WOFn2U7F5urh"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "# Student 4 (Name and ID)\n",
        "\n",
        "Choose two out of the following three methods to apply. Compare results with and without optimization for the chosen methods.\n",
        "\n",
        "- Different Partition Strategies (5 Marks)\n",
        "  - Explore and evaluate various strategies for partitioning data.\n",
        "\n",
        "- Caching vs. No Caching (5 Marks)\n",
        "  - Analyze the impact of caching data versus not caching.\n",
        "\n",
        "- Bucketing and Indexing (5 Marks)\n",
        "  - Investigate the effects of bucketing and indexing on data operations."
      ],
      "metadata": {
        "id": "uX-rH0Uz5u-2"
      }
    },
    {
      "cell_type": "code",
      "source": ["# Task 3 - Student 4\n"],
      "metadata": {
        "id": "Gu3ere9c5wJ4"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "mIM6uLApSxi2"
      },
      "source": [
        "# Convert ipynb to HTML for Turnitin submission [10 marks]\n",
        "\n",
        "---\n",
        "\n"
      ]
    },
    {
      "cell_type": "code",
      "metadata": {
        "id": "ZrQu11N_DCfZ"
      },
      "source": [
        "# install nbconvert\n",
        "#!pip3 install nbconvert\n",
        "\n",
        "\n",
        "# convert ipynb to html\n",
        "# file name: \"Your_Group_ID_CN7031.ipynb\n",
        "!jupyter nbconvert --to html Your_Group_ID_CN7031.ipynb"
      ],
      "execution_count": null,
      "outputs": []
    }
  ]
}
```
