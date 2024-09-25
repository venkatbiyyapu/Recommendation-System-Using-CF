# Recommendation Systems using Collaborative Filtering(CF)

## Overview

This project implements a Recommendation System using Collaborative Filtering using PySpark on **Databricks**. The **Alternating Least Squares (ALS)** model is used to predict movie ratings. The performance of the model is evaluated using **Mean Squared Error (MSE)**.

The dataset used is `ratings.dat`, which contains movie ratings with the following fields:
- `User ID`
- `Movie ID`
- `Rating`
- `Timestamp`

The dataset is split into 70% for training and 30% for testing, and the program reports the **MSE** of the model predictions.

## Dataset

The dataset used is `ratings.dat` with the following format:
```
UserID :: MovieID :: Rating :: Timestamp
```

### File Location:
- Upload the `ratings.dat` file to Databricks at the path: `/FileStore/tables/ratings.dat`

## Environment Setup (Databricks)

### Steps to run the project in **Databricks**:
1. **Create a Databricks Cluster**: Ensure the cluster is running with a Spark environment.
2. **Upload the Data**: Upload the `ratings.dat` file to Databricks FileStore:
   - Navigate to the `Data` tab on Databricks.
   - Select `Add Data` and upload the `ratings.dat` file.
   - Ensure the file path is `/FileStore/tables/ratings.dat`.
3. **Create a New Notebook**: Create a new Databricks notebook to run the code provided in the `.ipynb` file.

### Running the Code:
- The provided `.ipynb` file contains the code to train the **ALS** model and evaluate its performance using MSE.
- The code reads the dataset, splits it into training and testing sets, trains the model using the **ALS** algorithm, and calculates the **Mean Squared Error**.

### Example Commands in Databricks:
Once the `ratings.dat` file is uploaded, the commands in the Databricks notebook will look like this:
```python
ratings_rdd = spark.sparkContext.textFile("/FileStore/tables/ratings.dat") \
                .map(lambda x: x.split("::")) \
                .map(lambda x: (int(x[0]), int(x[1]), int(x[2])))

train_ratings_rdd, test_ratings_rdd = ratings_rdd.randomSplit([0.7, 0.3], seed=10)
```

## Results

After running the notebook, the output will display:
- **Mean Squared Error (MSE)**: `0.8279526729277772`
- **Accuracy**: `82.79%`

This demonstrates that the ALS model achieved an MSE of **0.8279** and an accuracy of **82.79%** on the test set.

## Requirements

- **Databricks account**: Ensure you have access to a Databricks workspace.
- **PySpark**: Databricks provides built-in support for Spark, so no additional installation is required.
- **Dataset**: The `ratings.dat` file should be uploaded to `/FileStore/tables/ratings.dat`.

## Conclusion

This project demonstrates how to implement and evaluate a Collaborative Filtering Recommendation System using the **ALS** algorithm on **Databricks**. The system predicts movie ratings with a high level of accuracy (82.79%) using user and movie data from the `ratings.dat` file.

## References

- [PySpark Documentation on Collaborative Filtering](https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html)
- [Databricks Documentation](https://docs.databricks.com/)
