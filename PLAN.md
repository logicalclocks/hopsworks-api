# Migration Plan: Great Expectations 0.18.12 → 1.11.0

## Executive Summary

This plan outlines the migration from Great Expectations (GE) 0.18.12 to 1.11.0. The migration involves significant API changes but can be accomplished while maintaining full backward compatibility for the Hopsworks user-facing API.

**Key Principle:** The Hopsworks wrapper classes (`ExpectationSuite`, `GeExpectation`, `ValidationResult`, `ValidationReport`) remain stable. All GE version-specific changes are isolated to internal conversion methods and engine implementations.

---

## 1. Breaking Changes in GE 1.0+

### 1.1 ExpectationSuite Field Renames
| Old (0.18) | New (1.0+) |
|------------|------------|
| `expectation_suite_name` | `name` |
| `ge_cloud_id` | `id` (now required UUID) |
| `evaluation_parameters` | `suite_parameters` |
| `data_asset_type` | **Removed** (suites are now data-type agnostic) |

### 1.2 ExpectationConfiguration Field Renames
| Old (0.18) | New (1.0+) |
|------------|------------|
| `expectation_type` | `type` |
| (none) | `id` (required UUID) |

### 1.3 Validation API Changes
- **`great_expectations.from_pandas()`** - Replaced with batch-based workflow
- **`RuntimeBatchRequest`** - Removed
- **`BaseDataContext`** - Replaced with `gx.get_context()`
- **`InMemoryStoreBackendDefaults`** - Removed
- **`DataContextConfig`** - Removed
- **`Validator`** class - Removed (use `batch.validate()`)

### 1.4 New Validation Pattern (GE 1.0+)
```python
import great_expectations as gx

context = gx.get_context()
data_source = context.data_sources.add_pandas(name="...")
data_asset = data_source.add_dataframe_asset(name="...")
batch_definition = data_asset.add_batch_definition_whole_dataframe("...")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
validation_result = batch.validate(expectation_suite)
```

---

## 2. Files Requiring Changes

### 2.1 Version Update
- [pyproject.toml](python/pyproject.toml) (line 76): Change `great_expectations==0.18.12` to `great_expectations==1.11.0`

### 2.2 Core Engine Files (Validation Logic)
1. **[python/hsfs/engine/python.py](python/hsfs/engine/python.py)** (lines 790-812)
   - Replace `great_expectations.from_pandas().validate()` with new batch-based workflow

2. **[python/hsfs/engine/spark.py](python/hsfs/engine/spark.py)** (lines 1364-1416)
   - Replace `RuntimeBatchRequest`, `BaseDataContext`, `InMemoryStoreBackendDefaults` with new Spark DataFrame workflow

### 2.3 Wrapper Classes (to_ge_type / from_ge_type methods)
3. **[python/hsfs/expectation_suite.py](python/hsfs/expectation_suite.py)** (lines 205-216)
   - Update `to_ge_type()` to use new field names (`name` instead of `expectation_suite_name`)
   - Update `from_ge_type()` to handle both old and new field names

4. **[python/hsfs/ge_expectation.py](python/hsfs/ge_expectation.py)** (lines 112-117)
   - Update `to_ge_type()` to use `type` instead of `expectation_type`
   - Handle the new `id` requirement

5. **[python/hsfs/validation_report.py](python/hsfs/validation_report.py)** (lines 113-122)
   - Verify `ExpectationSuiteValidationResult` constructor still works

6. **[python/hsfs/ge_validation_result.py](python/hsfs/ge_validation_result.py)** (lines 111-120)
   - Verify `ExpectationValidationResult` constructor still works

### 2.4 Test Files
7. **[python/tests/core/test_great_expectation_engine.py](python/tests/core/test_great_expectation_engine.py)**
   - Update tests using `great_expectations.core.ExpectationSuite()` constructor

8. **[python/tests/test_expectation_suite.py](python/tests/test_expectation_suite.py)**
   - Update any GE-specific assertions

---

## 3. Implementation Steps

### Step 1: Update pyproject.toml
Change the GE version from `0.18.12` to `1.11.0`:
```toml
great-expectations = ["great_expectations==1.11.0"]
```

### Step 2: Update Python Engine Validation (python.py)

**Current code (lines 790-812):**
```python
@uses_great_expectations
def validate_with_great_expectations(
    self,
    dataframe: pl.DataFrame | pd.DataFrame,
    expectation_suite: great_expectations.core.ExpectationSuite,
    ge_validate_kwargs: dict[Any, Any] | None = None,
) -> great_expectations.core.ExpectationSuiteValidationResult:
    # ... polars conversion ...
    if ge_validate_kwargs is None:
        ge_validate_kwargs = {}
    return great_expectations.from_pandas(
        dataframe, expectation_suite=expectation_suite
    ).validate(**ge_validate_kwargs)
```

**New code:**
```python
@uses_great_expectations
def validate_with_great_expectations(
    self,
    dataframe: pl.DataFrame | pd.DataFrame,
    expectation_suite: great_expectations.core.ExpectationSuite,
    ge_validate_kwargs: dict[Any, Any] | None = None,
) -> great_expectations.core.ExpectationSuiteValidationResult:
    # Polars conversion (unchanged)
    if HAS_POLARS and (
        isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame))
    ):
        warnings.warn(
            "Currently Great Expectations does not support Polars dataframes. "
            "This operation will convert to Pandas dataframe that can be slow.",
            util.FeatureGroupWarning,
            stacklevel=1,
        )
        dataframe = dataframe.to_pandas()

    if ge_validate_kwargs is None:
        ge_validate_kwargs = {}

    # GE 1.0+ batch-based validation
    context = great_expectations.get_context()
    data_source = context.data_sources.add_pandas(name="hopsworks_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name="hopsworks_dataframe_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="hopsworks_batch_definition"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    return batch.validate(expectation_suite, **ge_validate_kwargs)
```

### Step 3: Update Spark Engine Validation (spark.py)

**Current code (lines 1364-1416):** Uses `RuntimeBatchRequest`, `BaseDataContext`, etc.

**New code:**
```python
@uses_great_expectations
def validate_with_great_expectations(
    self,
    dataframe: DataFrame,
    expectation_suite: great_expectations.core.ExpectationSuite,
    ge_validate_kwargs: dict | None,
):
    if ge_validate_kwargs is None:
        ge_validate_kwargs = {}

    # GE 1.0+ Spark DataFrame validation
    context = great_expectations.get_context()
    data_source = context.data_sources.add_spark(name="hopsworks_spark_datasource")
    data_asset = data_source.add_dataframe_asset(name="hopsworks_spark_dataframe_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="hopsworks_spark_batch_definition"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    return batch.validate(expectation_suite, **ge_validate_kwargs)
```

### Step 4: Update ExpectationSuite.to_ge_type() (expectation_suite.py)

**Current code (lines 205-216):**
```python
@uses_great_expectations
def to_ge_type(self) -> great_expectations.core.ExpectationSuite:
    return great_expectations.core.ExpectationSuite(
        expectation_suite_name=self._expectation_suite_name,
        ge_cloud_id=self._ge_cloud_id,
        data_asset_type=self._data_asset_type,
        expectations=[
            expectation.to_ge_type() for expectation in self._expectations
        ],
        meta=self._meta,
    )
```

**New code:**
```python
@uses_great_expectations
def to_ge_type(self) -> great_expectations.core.ExpectationSuite:
    """Convert to Great Expectations ExpectationSuite type."""
    return great_expectations.core.ExpectationSuite(
        name=self._expectation_suite_name,
        id=self._ge_cloud_id,  # GE 1.0+ uses 'id' instead of 'ge_cloud_id'
        expectations=[
            expectation.to_ge_type() for expectation in self._expectations
        ],
        meta=self._meta,
        # Note: data_asset_type is removed in GE 1.0+ (suites are data-type agnostic)
    )
```

### Step 5: Update ExpectationSuite.from_ge_type() (expectation_suite.py)

**Current code (lines 130-166):** Uses `to_json_dict()` which returns GE 1.0 field names

**Update needed:** Handle both old and new field names in deserialization:
```python
@classmethod
@uses_great_expectations
def from_ge_type(
    cls,
    ge_expectation_suite: great_expectations.core.ExpectationSuite,
    run_validation: bool = True,
    validation_ingestion_policy: Literal["ALWAYS", "STRICT"] = "ALWAYS",
    id: int | None = None,
    feature_store_id: int | None = None,
    feature_group_id: int | None = None,
) -> ExpectationSuite:
    suite_dict = ge_expectation_suite.to_json_dict()

    # Handle GE 1.0+ field name changes
    # 'name' -> 'expectation_suite_name' (for Hopsworks internal representation)
    if "name" in suite_dict and "expectation_suite_name" not in suite_dict:
        suite_dict["expectation_suite_name"] = suite_dict.pop("name")

    # 'id' in GE 1.0+ is the cloud ID (was 'ge_cloud_id')
    if "id" in suite_dict and id is None:
        # GE's id is the cloud ID, not our internal ID
        suite_dict["ge_cloud_id"] = suite_dict.pop("id")

    # 'type' -> 'expectation_type' for expectations
    if "expectations" in suite_dict:
        for exp in suite_dict["expectations"]:
            if "type" in exp and "expectation_type" not in exp:
                exp["expectation_type"] = exp.pop("type")

    return cls(
        **suite_dict,
        id=id,
        run_validation=run_validation,
        validation_ingestion_policy=validation_ingestion_policy,
        feature_group_id=feature_group_id,
        feature_store_id=feature_store_id,
    )
```

### Step 6: Update GeExpectation.to_ge_type() (ge_expectation.py)

**Current code (lines 112-117):**
```python
@uses_great_expectations
def to_ge_type(self) -> great_expectations.core.ExpectationConfiguration:
    return great_expectations.core.ExpectationConfiguration(
        expectation_type=self.expectation_type, kwargs=self.kwargs, meta=self.meta
    )
```

**New code:**
```python
@uses_great_expectations
def to_ge_type(self) -> great_expectations.core.ExpectationConfiguration:
    """Convert to Great Expectations ExpectationConfiguration type."""
    # GE 1.0+ uses 'type' instead of 'expectation_type'
    return great_expectations.core.ExpectationConfiguration(
        type=self.expectation_type,
        kwargs=self.kwargs,
        meta=self.meta,
    )
```

### Step 7: Update GeExpectation.from_ge_type() (ge_expectation.py)

**Current code (lines 73-77):**
```python
@classmethod
def from_ge_type(
    cls, ge_expectation: great_expectations.core.ExpectationConfiguration
):
    return cls(**ge_expectation.to_json_dict())
```

**New code:**
```python
@classmethod
def from_ge_type(
    cls, ge_expectation: great_expectations.core.ExpectationConfiguration
):
    exp_dict = ge_expectation.to_json_dict()
    # Handle GE 1.0+ field name: 'type' -> 'expectation_type'
    if "type" in exp_dict and "expectation_type" not in exp_dict:
        exp_dict["expectation_type"] = exp_dict.pop("type")
    return cls(**exp_dict)
```

### Step 8: Update _convert_expectation() in ExpectationSuite (expectation_suite.py)

**Current code (lines 263-288):** Handles `ExpectationConfiguration` type check

**Update needed:** Also handle the new class-based expectations (e.g., `gx.expectations.ExpectColumnValuesToBeBetween`):
```python
def _convert_expectation(
    self,
    expectation: GeExpectation
    | great_expectations.core.ExpectationConfiguration
    | dict[str, Any],
) -> GeExpectation:
    if HAS_GREAT_EXPECTATIONS:
        # Handle both ExpectationConfiguration and class-based expectations
        if isinstance(expectation, great_expectations.core.ExpectationConfiguration):
            exp_dict = expectation.to_json_dict()
            # Handle GE 1.0+ field name changes
            if "type" in exp_dict and "expectation_type" not in exp_dict:
                exp_dict["expectation_type"] = exp_dict.pop("type")
            return GeExpectation(**exp_dict)
    if isinstance(expectation, GeExpectation):
        return expectation
    if isinstance(expectation, dict):
        # Handle GE 1.0+ field name changes in dict
        if "type" in expectation and "expectation_type" not in expectation:
            expectation = expectation.copy()
            expectation["expectation_type"] = expectation.pop("type")
        return GeExpectation(**expectation)
    raise TypeError(f"Expectation of type {type(expectation)} is not supported.")
```

### Step 9: Update Tests

Update test files to use new GE 1.0+ constructors:

**In test_great_expectation_engine.py:**
```python
# Old
suite = great_expectations.core.ExpectationSuite(
    expectation_suite_name="suite_name",
)

# New
suite = great_expectations.core.ExpectationSuite(
    name="suite_name",
)
```

### Step 10: Update Documentation

Update [docs/expectation_suite_api.md](docs/expectation_suite_api.md):

**Current example:**
```python
import great_expectations as ge

expectation_suite = ge.core.ExpectationSuite(
    "new_expectation_suite",
    expectations=[
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_max_to_be_between",
            kwargs={...}
        )
    ]
)
```

**New example:**
```python
import great_expectations as gx

# Option 1: Using ExpectationSuite with class-based expectations
expectation_suite = gx.ExpectationSuite(
    name="new_expectation_suite",
)
expectation_suite.add_expectation(
    gx.expectations.ExpectColumnMaxToBeBetween(
        column="feature",
        min_value=-1,
        max_value=1
    )
)

# Option 2: Using ExpectationConfiguration (backwards compatible)
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
```

---

## 4. Backward Compatibility Strategy

### 4.1 User-Facing API Unchanged
The Hopsworks API remains stable:
- `ExpectationSuite` class signature unchanged
- `GeExpectation` class signature unchanged
- `ValidationReport` class signature unchanged
- `FeatureGroup.validate()`, `save_expectation_suite()`, etc. unchanged

### 4.2 Internal Conversion Handles Both Formats
- `from_ge_type()` methods handle both old (`expectation_suite_name`, `expectation_type`) and new (`name`, `type`) field names
- `to_ge_type()` methods produce valid GE 1.0+ objects

### 4.3 Existing Hopsworks Data Compatible
- Backend stores expectations with Hopsworks field names (`expectation_suite_name`, `expectation_type`)
- Conversion happens only at GE boundary

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| GE 1.0+ may not support all validation kwargs | Medium | Test `ge_validate_kwargs` passthrough; document any deprecated options |
| Spark DataFrame support may differ | High | Verify `context.data_sources.add_spark()` exists and works correctly |
| Performance regression with new context pattern | Low | Context creation is lightweight; can cache if needed |
| Class-based expectations may have different serialization | Medium | Test `to_json_dict()` output format thoroughly |
| Users may pass old-style GE objects | Medium | Handle gracefully in `from_ge_type()` with field name normalization |

---

## 6. Testing Strategy

1. **Unit Tests:** Update all tests in `test_great_expectation_engine.py`, `test_expectation_suite.py`, `test_ge_expectation.py`
2. **Integration Tests:** Verify end-to-end validation with both Pandas and Spark DataFrames
3. **Backward Compatibility Tests:** Ensure existing serialized expectations can be loaded and validated
4. **Documentation Tests:** Verify all code examples in documentation work

---

## 7. Summary of Changes

| File | Change Type | Description |
|------|-------------|-------------|
| `pyproject.toml` | Version | Update GE version to 1.11.0 |
| `engine/python.py` | Rewrite | Replace `from_pandas()` with batch workflow |
| `engine/spark.py` | Rewrite | Replace old context/batch request with new workflow |
| `expectation_suite.py` | Update | Modify `to_ge_type()`, `from_ge_type()`, `_convert_expectation()` |
| `ge_expectation.py` | Update | Modify `to_ge_type()`, `from_ge_type()` |
| `validation_report.py` | Verify | Check constructor compatibility |
| `ge_validation_result.py` | Verify | Check constructor compatibility |
| `tests/*.py` | Update | Update GE constructor calls |
| `docs/expectation_suite_api.md` | Update | Update code examples |
