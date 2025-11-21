# Expectation Suite

::: hsfs.expectation_suite.ExpectationSuite

## Creation with Great Expectations

```python3
import great_expectations as ge

expectation_suite = ge.core.ExpectationSuite(
    "new_expectation_suite",
    expectations=[
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_max_to_be_between",
            kwargs={
                "column": "feature",
                "min_value": -1,
                "max_value": 1
            }
        )
    ]
)
```

## Attach to Feature Group

To attach an ExpectationSuite to a FeatureGroup, call [`FeatureGroup.save_expectation_suite`][hsfs.feature_group.FeatureGroup.save_expectation_suite].
