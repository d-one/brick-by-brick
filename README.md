# Databricks

Current repo could be use to test an dummy ml train and deploy cycle on databricks with asset bundles (DAPs).

* databricks auth login
* databricks bundle validate
* databricks bundle deploy
* databricks bundle run iris_pipeline_job


# Endpoint use

### Input
```json
{
  "dataframe_split": {
    "columns": [
      "sepal length (cm)",
      "sepal width (cm)",
      "petal length (cm)",
      "petal width (cm)"
    ],
    "data": [
      [5.1, 3.5, 1.4, 0.2],
      [0, 1, 5.4, 5.2]
    ]
  }
}
```

### Output
```json
{
  "predictions": [
    {
      "0": {
        "setosa": 0.44,
        "versicolor": 0.25,
        "virginica": 0.31,
        "predicted_class": "setosa",
        "confidence": 0.44
      }
    },
    {
      "0": {
        "setosa": 0.48,
        "versicolor": 0.49,
        "virginica": 0.03,
        "predicted_class": "versicolor",
        "confidence": 0.49
      }
    }
  ]
}
```