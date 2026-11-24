# ruff: noqa: INP001
"""The names every job and the app share, and the world the data is drawn from."""

ORDERS = "leasing_orders"
OUTCOMES = "leasing_outcomes"
CREDIT = "customer_credit_history"
EQUIPMENT = "leasing_equipment"
FEATURE_VIEW = "leasing_decisions"
MODEL = "leasing_xgboost"
KUMO = "kumo_tabular_medium"
LABEL = "leased"
# The customers and machines are rebuilt from these by the app's simulation, so
# generate_data.py and the app must agree on them.
SEED = 20261030
CUSTOMERS = 5_000
EQUIPMENT_ITEMS = 100
