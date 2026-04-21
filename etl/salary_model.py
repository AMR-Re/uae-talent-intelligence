"""
etl/salary_model.py  (v4 — confidence_score + fixed seeds)
─────────────────────────────────────────────────────────────────────────────
Tier 2 salary imputation using a Gradient Boosting Regressor.

Changes in v4 (production additions — core ML logic unchanged):

  FIX 8 — Stabilised random seeds
      numpy.random.seed(42) set at module import.
      GradientBoostingRegressor already uses random_state=42.
      Both are now explicit, making results fully reproducible across runs.

  ADD 4 — confidence_score for ML-imputed rows
      After predicting salaries, each imputed row receives a confidence_score
      derived from the cross-validated MAE:

          confidence = clip(90 - (cv_mae / MAX_AED_MONTHLY * 90), 50, 90)

      Interpretation:
        · Lower CV MAE  → higher confidence (closer to 90)
        · Higher CV MAE → lower confidence (floor at 50)
      This overwrites the Tier-1 placeholder (75) set in transform.py with a
      data-driven value specific to this run's model quality.

      Stored as:
        salary_source        = 'imputed_model' | 'imputed_model_override'
        confidence_score     = <computed>

All FIX LOG entries from v3 (bootstrap-aware) are retained unchanged.
─────────────────────────────────────────────────────────────────────────────
"""

import sys
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from colorama import Fore, Style, init as colorama_init
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

colorama_init(autoreset=True)

# FIX 8 — Fixed random seed at module level for reproducibility
np.random.seed(42)

CLEAN_CSV  = Path("data/processed/jobs_clean.csv")
MODEL_PATH = Path("data/reference/salary_model.pkl")
META_PATH  = Path("data/processed/transform_meta.json")

CATEGORICAL_FEATURES = [
    "sector", "emirate", "seniority_band",
    "employment_type", "company_size_band",
]
NUMERIC_FEATURES = ["skills_count", "is_mnc"]
ALL_FEATURES     = CATEGORICAL_FEATURES + NUMERIC_FEATURES

MIN_TRAINING_ROWS     = 30
BOOTSTRAP_MAE_CEILING = 8_000   # AED
MAX_AED_MONTHLY       = 200_000  # used in confidence normalisation

# ── CLI flags ─────────────────────────────────────────────────────────────────
OVERRIDE_TIER1 = "--override-tier1" in sys.argv


def _load_data() -> pd.DataFrame:
    if not CLEAN_CSV.exists():
        raise FileNotFoundError(
            "data/processed/jobs_clean.csv not found. Run transform first."
        )
    return pd.read_csv(CLEAN_CSV)


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["is_mnc"] = df["is_mnc"].fillna(False).astype(int)
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = "Unknown"
        df[col] = df[col].fillna("Unknown").astype(str)
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def _print_diagnostic(df: pd.DataFrame) -> None:
    print(f"\n    Pre-train diagnostic:")
    print(f"      Total rows           : {len(df):,}")
    source_counts = df["salary_source"].value_counts()
    for src, n in source_counts.items():
        sub = df[df["salary_source"] == src]["salary_aed_monthly"]
        sal_note = ""
        if not sub.empty and sub.max() > 0:
            sal_note = f"  AED range: {sub[sub > 0].min():,.0f}–{sub.max():,.0f}"
        print(f"      {src:<36}: {n:>5} rows{sal_note}")

    zero_reported = (
        (df["salary_source"] == "reported") & (df["salary_aed_monthly"] == 0)
    ).sum()
    if zero_reported > 0:
        print(
            f"\n    {Fore.RED}✗  {zero_reported} rows labelled 'reported' "
            f"have salary_aed_monthly=0.\n"
            f"       Re-run transform.py to fix before training.{Style.RESET_ALL}"
        )

    imputed_count = df["salary_source"].str.startswith("imputed").sum()
    if imputed_count > 0 and not OVERRIDE_TIER1:
        print(
            f"\n    {Fore.CYAN}ℹ  {imputed_count:,} rows have Tier-1 imputed salaries.\n"
            f"       Run with --override-tier1 to let the ML model refine them.\n"
            f"       (Recommended during bootstrap when reported salaries < 200){Style.RESET_ALL}"
        )


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                ),
                CATEGORICAL_FEATURES,
            ),
            ("num", "passthrough", NUMERIC_FEATURES),
        ]
    )
    # FIX 8: random_state=42 already set; leaving explicit for clarity
    model = GradientBoostingRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=4,
        min_samples_leaf=5,
        subsample=0.8,
        random_state=42,        # FIX 8 — stabilised
    )
    return Pipeline([("preprocessor", preprocessor), ("model", model)])


# ADD 4 — compute confidence from CV MAE
def _confidence_from_mae(cv_mae: float) -> float:
    """
    Maps CV MAE → confidence score (0–100).
    Lower MAE = higher confidence. Floor at 50, ceiling at 90.
    Formula: confidence = 90 - (cv_mae / MAX_AED_MONTHLY * 90)
    """
    raw = 90.0 - (cv_mae / MAX_AED_MONTHLY * 90.0)
    return round(float(np.clip(raw, 50.0, 90.0)), 1)


def train_and_impute() -> None:
    mode_label = "Bootstrap Override (--override-tier1)" if OVERRIDE_TIER1 else "Standard"
    print(f"\n  {Fore.CYAN}── Salary Model (Tier 2) [{mode_label}] ──────{Style.RESET_ALL}")

    df = _load_data()
    df = _prepare_features(df)

    # Ensure confidence_score column exists (transform.py sets it; guard for
    # standalone runs or schema migration)
    if "confidence_score" not in df.columns:
        df["confidence_score"] = 0.0
    if "salary_imputed_flag" not in df.columns:
        df["salary_imputed_flag"] = df["salary_source"] != "reported"

    _print_diagnostic(df)

    reported = df[
        (df["salary_source"] == "reported") &
        (df["salary_aed_monthly"] > 0)
    ].copy()
    reported = reported[reported["salary_aed_monthly"].between(2_000, 200_000)]

    if OVERRIDE_TIER1:
        to_predict_mask = df["salary_source"].str.startswith("imputed", na=False)
        predict_label   = "Tier-1 imputed (override)"
    else:
        to_predict_mask = df["salary_aed_monthly"] == 0
        predict_label   = "zero-salary"

    missing = df[to_predict_mask].copy()

    print(f"\n    Training rows (reported, salary > 0) : {len(reported):,}")
    print(f"    Rows to predict ({predict_label:^30}): {len(missing):,}")

    if len(reported) < MIN_TRAINING_ROWS:
        print(
            f"\n    {Fore.YELLOW}⚠  Only {len(reported)} usable training rows "
            f"(need ≥ {MIN_TRAINING_ROWS}).\n"
            f"       Skipping ML imputation — no model saved.{Style.RESET_ALL}"
        )
        return

    if missing.empty:
        if OVERRIDE_TIER1:
            print(f"    {Fore.YELLOW}⚠  No Tier-1 imputed rows found.{Style.RESET_ALL}")
        else:
            print(f"    {Fore.GREEN}✓  No missing salaries to impute.{Style.RESET_ALL}")
        return

    X_train = reported[ALL_FEATURES]
    y_train = reported["salary_aed_monthly"]

    assert (y_train > 0).all(), (
        "Training set contains zero salaries — re-run transform.py first."
    )

    pipeline  = build_pipeline()
    n_folds   = min(5, len(reported))
    cv_scores = cross_val_score(
        pipeline, X_train, y_train,
        cv=n_folds,
        scoring="neg_mean_absolute_error",
        n_jobs=-1,
    )
    cv_mae = float(-cv_scores.mean())
    print(f"    CV MAE ({n_folds}-fold): AED {cv_mae:,.0f}/month")

    # ADD 4 — confidence score derived from MAE
    ml_confidence = _confidence_from_mae(cv_mae)
    print(f"    ML confidence score : {ml_confidence}")

    if OVERRIDE_TIER1 and cv_mae > BOOTSTRAP_MAE_CEILING:
        print(
            f"\n    {Fore.YELLOW}⚠  CV MAE ({cv_mae:,.0f}) exceeds ceiling "
            f"({BOOTSTRAP_MAE_CEILING:,}).\n"
            f"       Model too noisy — Tier-1 values kept as-is.{Style.RESET_ALL}"
        )
        return

    pipeline.fit(X_train, y_train)

    X_pred      = missing[ALL_FEATURES]
    predictions = pipeline.predict(X_pred)
    predictions = np.clip(predictions, 2_000, 200_000)

    # Write salary + source + imputed flag + confidence
    df.loc[missing.index, "salary_aed_monthly"]  = predictions.round(0)
    df.loc[missing.index, "salary_imputed_flag"] = True
    df.loc[missing.index, "confidence_score"]    = ml_confidence   # ADD 4

    if OVERRIDE_TIER1:
        df.loc[missing.index, "salary_source"] = "imputed_model_override"
    else:
        df.loc[missing.index, "salary_source"] = "imputed_model"

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as fh:
        pickle.dump(pipeline, fh)
    print(f"    Model saved → {MODEL_PATH}")

    feature_names = CATEGORICAL_FEATURES + NUMERIC_FEATURES
    importances   = pipeline.named_steps["model"].feature_importances_
    fi = sorted(zip(feature_names, importances), key=lambda x: -x[1])
    print(f"\n    Feature importances:")
    for feat, imp in fi:
        bar = "█" * int(imp * 40)
        print(f"      {feat:<22} {bar} {imp:.3f}")

    df.to_csv(CLEAN_CSV, index=False)
    print(f"\n    Updated {CLEAN_CSV} with model-imputed salaries")

    if META_PATH.exists():
        with open(META_PATH) as f:
            meta = json.load(f)
        meta["salary_source_split"]         = df["salary_source"].value_counts().to_dict()
        meta["salary_coverage_pct"]         = round((df["salary_aed_monthly"] > 0).mean() * 100, 1)
        meta["salary_model_cv_mae"]         = round(cv_mae, 0)
        meta["salary_model_confidence"]     = ml_confidence          # ADD 4
        meta["salary_model_bootstrap_mode"] = OVERRIDE_TIER1
        meta["avg_confidence"]              = round(df["confidence_score"].mean(), 1)
        with open(META_PATH, "w") as f:
            json.dump(meta, f, indent=2)

    final_coverage = round((df["salary_aed_monthly"] > 0).mean() * 100, 1)
    print(f"\n    {Fore.GREEN}✅ Salary model complete{Style.RESET_ALL}")
    print(f"       Final salary coverage  : {final_coverage}%")
    print(f"       ML confidence score    : {ml_confidence}")
    print(f"       Source breakdown       : {df['salary_source'].value_counts().to_dict()}")


if __name__ == "__main__":
    train_and_impute()