# ruff: noqa: INP001
"""Leasing decisions for new orders, by XGBoost and Kumo Tabular side by side, logged through the feature view.

A new order is a customer, a machine and a lease length. The feature view gives its
features: `FeatureView.get_feature_vectors` looks the machine and the customer up in
the online store (their newest credit report and last settled lease, what the feature
view's joins give an order placed now) with the lease length passed in, and
`FeatureView.transform` applies its model-dependent transformations with the statistics
of the training dataset the model was trained on, so the app computes every feature as
training did and joins nothing itself.

The simulation stands in for the business: for every logged decision not yet
simulated, an underwriter decides the order and the lease ends (paid, paid late,
damaged, defaulted or declined), drawn from the same world as the history, and the
order and its outcome are written to leasing_orders and leasing_outcomes. The next
training run learns from them, and the customer's next order sees the outcome.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

import names
import numpy as np
import pandas as pd
import synth


CATALOGUE_SECONDS = 60.0


def utc_naive(times: pd.Series) -> pd.Series:
    """Timestamps as naive UTC: the feature store may return them with a zone."""
    times = pd.to_datetime(times)
    if times.dt.tz is not None:
        times = times.dt.tz_convert("UTC").dt.tz_localize(None)
    return times


class Scorer:
    """The latest leasing model with its feature view, and Kumo Tabular."""

    def __init__(self, project, system: str):
        self.project = project
        self.system = system
        self.fs = project.get_feature_store()
        self.lock = threading.Lock()
        self.kumo = None
        self.model = None
        self._catalogue = None
        self._catalogue_at = 0.0
        # The world the history was drawn from: the customers' hidden risk and the machines' damage risk.
        self.customers, self.machines, _ = synth.world(
            names.SEED, names.CUSTOMERS, names.EQUIPMENT_ITEMS
        )

    # region Loading

    def load(self) -> None:
        """Load the newest leasing_xgboost, its feature view and Kumo context, and Kumo once."""
        import xgboost as xgb

        registry = self.project.get_model_registry()
        models = registry.get_models(names.MODEL)
        if not models:
            raise RuntimeError(
                f"{names.MODEL} is not registered yet: run the training job"
            )
        latest = max(models, key=lambda m: int(m.version))
        if self.model is not None and int(latest.version) == int(self.model.version):
            return
        directory = Path(latest.download())
        classifier = xgb.XGBClassifier()
        classifier.load_model(directory / "model.json")
        fv = latest.get_feature_view(init=False)
        # The transformations use the statistics of the training dataset the model learned from.
        fv.init_serving(training_dataset_version=latest.training_dataset_version)
        if self.kumo is None:
            from kumo import KumoTabular

            kumo_models = registry.get_models(names.KUMO)
            if not kumo_models:
                raise RuntimeError(
                    f"{names.KUMO} is not registered yet: run register_kumo.py"
                )
            self.kumo = KumoTabular(
                max(kumo_models, key=lambda m: int(m.version)).download()
            )
        with self.lock:
            self.model = latest
            self.classifier = classifier
            self.fv = fv
            self.features = json.loads((directory / "features.json").read_text())
            self.context = pd.read_parquet(directory / "kumo_context.parquet")
            self.metrics = dict(latest.training_metrics or {})

    def _group(self, name: str):
        return self.fs.get_feature_group(name, version=1)

    def catalogue(self) -> dict[str, pd.DataFrame]:
        """The machines and the customers' newest credit reports, for the app's pickers; read every minute."""
        if (
            self._catalogue is None
            or time.monotonic() - self._catalogue_at > CATALOGUE_SECONDS
        ):
            self._catalogue = {
                name: self._group(name).read(online=True, dataframe_type="pandas")
                for name in (names.EQUIPMENT, names.CREDIT)
            }
            self._catalogue_at = time.monotonic()
        return self._catalogue

    # endregion

    # region Scoring

    @staticmethod
    def untransformed(fv, orders: pd.DataFrame) -> pd.DataFrame:
        """The feature view's untransformed features for `orders` (customer_id, equipment_id, lease_months).

        The order itself is new, so its key is missing from leasing_orders: the lease
        length is passed in, and the machine and the customer are looked up by their keys.
        """
        rows = orders.to_dict("records")
        return fv.get_feature_vectors(
            serving_keys=[
                {
                    "customer_id": int(r["customer_id"]),
                    "equipment_id": int(r["equipment_id"]),
                }
                for r in rows
            ],
            passed_features=[{"lease_months": int(r["lease_months"])} for r in rows],
            allow_missing=True,
            transform=False,
            return_type="pandas",
        )

    def score(self, orders: pd.DataFrame, log: bool = True) -> pd.DataFrame:
        """Both models' decisions on `orders`, logged with their features unless `log` is False."""
        with self.lock:
            fv, classifier, features, context, model = (
                self.fv,
                self.classifier,
                self.features,
                self.context,
                self.model,
            )
        started = time.perf_counter()
        raw = self.untransformed(fv, orders).reset_index(drop=True)
        transformed = fv.transform(raw, return_type="pandas")[features]
        transform_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        xgb_probability = classifier.predict_proba(transformed)[:, 1]
        xgb_ms = (time.perf_counter() - started) * 1000
        kumo_probability, kumo_seconds = self.kumo.probabilities(
            context, transformed, names.LABEL
        )
        out = orders.reset_index(drop=True).assign(
            xgboost_probability=np.round(xgb_probability, 4),
            leased=(xgb_probability >= 0.5).astype(int),
            kumo_probability=np.round(kumo_probability.to_numpy(), 4),
            kumo_leased=(kumo_probability.to_numpy() >= 0.5).astype(int),
        )
        if log:
            # The order is placed now: its event time, which the simulation reads back from the log.
            placed = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s")
            fv.log(
                untransformed_features=raw,
                transformed_features=transformed,
                predictions=out[[names.LABEL]],
                inference_helper_columns=out[["customer_id", "equipment_id"]],
                extra_logging_features=out[
                    ["xgboost_probability", "kumo_probability", "kumo_leased"]
                ],
                event_time=pd.DataFrame({"order_ts": [placed] * len(out)}),
                request_id=[str(uuid.uuid4()) for _ in range(len(out))],
                model=model,
            )
        out.attrs["features"] = raw
        out.attrs["timings_ms"] = {
            "transform": round(transform_ms, 1),
            "xgboost": round(xgb_ms, 1),
            "kumo": round(kumo_seconds * 1000, 1),
        }
        return out

    def random_orders(self, n: int, seed: int | None = None) -> pd.DataFrame:
        """`n` new orders: random customers, machines and lease lengths."""
        rng = np.random.default_rng(seed)
        return pd.DataFrame(
            {
                "customer_id": rng.choice(self.customers["customer_id"].to_numpy(), n),
                "equipment_id": rng.choice(self.machines["equipment_id"].to_numpy(), n),
                "lease_months": rng.choice(synth.LEASE_MONTHS, n).astype(np.int64),
            }
        )

    # endregion

    # region Simulation

    def simulate(self) -> dict:
        """Turn every logged decision since the last simulation into an order with its outcome."""
        orders_fg = self._group(names.ORDERS)
        orders = orders_fg.read(online=True, dataframe_type="pandas")
        since = utc_naive(orders["order_ts"]).max()
        logged = self.fv.read_log(
            start_time=since + pd.Timedelta(seconds=1), online=True
        )
        if logged is None or len(logged) == 0:
            return {"orders": 0}
        logged = logged.assign(order_ts=utc_naive(logged["order_ts"]))
        logged = logged[logged["order_ts"] > since].sort_values(
            "order_ts", ignore_index=True
        )
        if len(logged) == 0:
            return {"orders": 0}
        now = pd.Timestamp.now(tz="UTC").tz_localize(None).floor("s")
        new = pd.DataFrame(
            {
                "order_id": int(orders["order_id"].max())
                + 1
                + np.arange(len(logged), dtype=np.int64),
                "customer_id": logged["customer_id"].astype(np.int64),
                "equipment_id": logged["equipment_id"].astype(np.int64),
                "lease_months": logged["lease_months"].astype(np.int64),
                "order_ts": logged["order_ts"].astype("datetime64[us]").to_numpy(),
            }
        )
        # Each customer's latest outcome: the online store keeps one per customer.
        last = (
            self._group(names.OUTCOMES)
            .read(online=True, dataframe_type="pandas")
            .set_index("customer_id")["outcome"]
        )
        decided = synth.decide(
            new,
            self.customers,
            self.machines,
            last,
            np.random.default_rng(),
            settled_at=now,
        )
        # Each outcome a second apart: a customer's outcomes are keyed by the customer and its time.
        decided["outcome_ts"] = (
            now + pd.to_timedelta(np.arange(len(decided)), unit="s")
        ).astype("datetime64[us]")
        new[names.LABEL] = decided[names.LABEL].astype(np.int64).to_numpy()
        outcomes = decided[
            [
                "order_id",
                "customer_id",
                "outcome_ts",
                "outcome",
                "paid",
                "problem_free",
                "days_late",
                "damage_cost",
            ]
        ]
        orders_fg.insert(new, write_options={"wait_for_job": True})
        self._group(names.OUTCOMES).insert(
            outcomes, write_options={"wait_for_job": True}
        )
        agreed = (
            logged[f"predicted_{names.LABEL}"].astype(int).to_numpy()
            == new[names.LABEL].to_numpy()
        ).mean()
        return {
            "orders": len(new),
            "leased": int(new[names.LABEL].sum()),
            "outcomes": outcomes["outcome"].value_counts().to_dict(),
            "xgboost_agreed": round(float(agreed), 3),
            "kumo_agreed": round(
                float(
                    (
                        logged["kumo_leased"].astype(int).to_numpy()
                        == new[names.LABEL].to_numpy()
                    ).mean()
                ),
                3,
            ),
        }

    # endregion
