# Expectation Suite

::: hsfs.expectation_suite.ExpectationSuite

## Creation with Great Expectations

```python3
import great_expectations as gx

# Create an ExpectationSuite with expectations
expectation_suite = gx.ExpectationSuite(
    name="new_expectation_suite",
    expectations=[
        gx.core.ExpectationConfiguration(
            type="expect_column_max_to_be_between",
            kwargs={
                "column": "feature",
                "min_value": -1,
                "max_value": 1
            }
        )
    ]
)

# Or add expectations using class-based syntax (recommended)
expectation_suite = gx.ExpectationSuite(name="new_expectation_suite")
expectation_suite.add_expectation(
    gx.expectations.ExpectColumnMaxToBeBetween(
        column="feature",
        min_value=-1,
        max_value=1
    )
)
```

## Attach to Feature Group

To attach an ExpectationSuite to a FeatureGroup, call [`FeatureGroup.save_expectation_suite`][hsfs.feature_group.FeatureGroup.save_expectation_suite].
