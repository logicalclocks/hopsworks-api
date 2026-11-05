# ruff: noqa: INP001, B008 - a udf asks for statistics through a default argument
"""The model-dependent transformations of the leasing feature view: the system's only feature engineering.

The feature pipeline writes the four tables as they are generated. These custom
transformations, attached to the feature view, turn the joined columns into the
model's features at training and, through `FeatureView.transform`, in the app, so the
two can never compute a feature differently. They run in pandas mode: each
argument is a column of the batch, one row or thousands.

`prev_*` is the customer's last order settled before this one, from the feature
view's point-in-time join with leasing_outcomes; it is empty for a first order.
"""

from hopsworks import udf
from hsfs.transformation_statistics import TransformationStatistics


@udf(float, mode="pandas")
def lease_cost(monthly_price, lease_months):
    """What the whole lease costs the customer."""
    return monthly_price * lease_months


@udf([float, float], drop=["monthly_income", "total_debt"], mode="pandas")
def affordability(monthly_price, monthly_income, total_debt):
    """The monthly payment and the debt, each as a share of income (monthly and yearly)."""
    income = monthly_income.clip(lower=1.0)
    return monthly_price / income, total_debt / (income * 12)


@udf(
    [int, int, int, float, float],
    drop=[
        "prev_outcome",
        "prev_paid",
        "prev_problem_free",
        "prev_days_late",
        "prev_damage_cost",
    ],
    mode="pandas",
)
def prior_lease(
    prev_outcome, prev_paid, prev_problem_free, prev_days_late, prev_damage_cost
):
    """Whether the customer had a lease before, whether it was paid, and paid with no problem.

    A declined order is a prior order but not a prior lease.
    """
    leased = prev_outcome.notna() & (prev_outcome != "declined")
    paid = leased & (prev_paid.fillna(0) == 1)
    return (
        leased.astype(int),
        paid.astype(int),
        (paid & (prev_problem_free.fillna(0) == 1)).astype(int),
        prev_days_late.fillna(0).astype(float),
        prev_damage_cost.fillna(0).astype(float),
    )


@udf(float, drop=["credit_score"], mode="pandas")
def credit_score_z(credit_score, statistics=TransformationStatistics("credit_score")):
    """The credit score in standard deviations from the training data's mean."""
    return (
        credit_score - statistics.credit_score.mean
    ) / statistics.credit_score.std_dev


@udf(float, mode="pandas")
def price_to_typical(
    monthly_price, statistics=TransformationStatistics("monthly_price")
):
    """The machine's monthly price against the mean price of the machines leased in training."""
    return monthly_price / statistics.monthly_price.mean


def attached():
    """The transformations to attach to the feature view, with their outputs named."""
    from hsfs.builtin_transformations import label_encoder

    return [
        lease_cost("monthly_price", "lease_months").alias("lease_cost"),
        affordability("monthly_price", "monthly_income", "total_debt").alias(
            "payment_to_income", "debt_to_income"
        ),
        prior_lease(
            "prev_outcome",
            "prev_paid",
            "prev_problem_free",
            "prev_days_late",
            "prev_damage_cost",
        ).alias(
            "prior_lease",
            "prior_paid",
            "prior_paid_problem_free",
            "prior_days_late",
            "prior_damage_cost",
        ),
        credit_score_z("credit_score").alias("credit_score_z"),
        price_to_typical("monthly_price").alias("price_to_typical"),
        label_encoder("category"),
        label_encoder("power_source"),
        label_encoder("customer_type"),
    ]
