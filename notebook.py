# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

# DBTITLE 1,Setup
import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

OUTPUT_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

#Your investigation here.
from pathlib import Path
import json

repo_root = Path("/Workspace/Users/taraka.l.vemulapalli.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

# DBTITLE 1,Source reference
# MAGIC %md
# MAGIC ### Source reference (from Catalog descriptions and the supplied lineage view)
# MAGIC * **Inputs:** `training.to6_challenge3_inputs.returns_initial` (Catalog comment: *"Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output."*) and `training.to6_challenge3_inputs.returns_corrected` (*"Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness."*).
# MAGIC * **Upstream source:** the supplied view `training.to6_challenge3_inputs.source_lineage` maps both delivery tables to `training.to6_challenge3_inputs.parsed_source` (created by SQL from it). The view states it does not claim a Catalog UI lineage check.
# MAGIC * **Row meaning:** one row = one synthetic tax return. `return_id` is the uniqueness identifier (must occur exactly once); `taxpayer_id` identifies the filer, who can have returns in several years, so it is *not* a unique key.
# MAGIC * **Relevant fields:** required `return_id`, `tax_year`, `income`, `deduction` (all int except `return_id`). Amounts must be nonnegative; `income = 0` is valid. `taxpayer_id` is not needed for the features (need-to-know) and is dropped from feature output.

# COMMAND ----------

# DBTITLE 1,Catalog descriptions and lineage
# Catalog table comments and column types, read from Unity Catalog
for t in [INITIAL_TABLE, CORRECTED_TABLE, LINEAGE_VIEW]:
    info = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {t}").collect()}
    print(t, "->", info.get("Comment"))
spark.table(INITIAL_TABLE).printSchema()

# Supplied lineage view
display(spark.table(LINEAGE_VIEW))

# COMMAND ----------

# DBTITLE 1,Sample rows
# At least three sample rows from each batch
print("returns_initial sample")
spark.table(INITIAL_TABLE).orderBy("return_id").limit(4).show()
print("returns_corrected sample")
spark.table(CORRECTED_TABLE).orderBy("return_id").limit(4).show()

# COMMAND ----------

# DBTITLE 1,Full-batch profile
# Profile the FULL batches (not only samples): returns by year, missing required values,
# duplicate return IDs and zero-income records.
def profile(table):
    df = spark.table(table)
    dup_ids = df.groupBy("return_id").count().filter("count > 1")
    summary = {
        "table": table,
        "rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
        "duplicate_return_ids": dup_ids.count(),
        **{f"missing_{c}": df.filter(F.col(c).isNull()).count() for c in REQUIRED_FIELDS},
        "zero_income": df.filter("income = 0").count(),
        "negative_amounts": df.filter("income < 0 OR deduction < 0").count(),
    }
    return summary, dup_ids

for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    s, dups = profile(t)
    print(s)
    if dups.count():
        print("duplicate return_ids:"); dups.show()

print("Returns by year")
display(
    spark.table(INITIAL_TABLE).withColumn("batch", F.lit("initial"))
    .unionByName(spark.table(CORRECTED_TABLE).withColumn("batch", F.lit("corrected")))
    .groupBy("batch", "tax_year").agg(F.count("*").alias("rows"),
                                       F.countDistinct("return_id").alias("distinct_returns"),
                                       F.sum(F.when(F.col("income") == 0, 1).otherwise(0)).alias("zero_income"),
                                       F.sum(F.when(F.col("income").isNull(), 1).otherwise(0)).alias("missing_income"))
    .orderBy("batch", "tax_year")
)

print("Zero-income records (corrected batch)")
spark.table(CORRECTED_TABLE).filter("income = 0").show()

# COMMAND ----------

# DBTITLE 1,Findings that drive next steps
# MAGIC %md
# MAGIC ### Findings and how they affect the next steps
# MAGIC * `returns_initial` has **13 rows but 12 distinct return IDs**: `R01` appears twice (identical row, 2022). It also has **one missing required value**: `R02` (2022) has `income = NULL`. Both break the data contract, so the uniqueness and completeness checks in step 2 must reject this batch. A null income cannot be turned into zero (zero is a real, valid value) and the duplicate cannot be silently dropped.
# MAGIC * `returns_corrected` has **12 rows / 12 distinct IDs**, no missing required values, no negative amounts; `R02` now has `income = 200`. It is assessed with the same rules before use.
# MAGIC * Both batches contain **2 zero-income returns** (`R03` 2022, `R09` 2024). Zero income is valid, so these must stay in the feature table with a **null** `deduction_to_income` instead of failing or dividing by zero.
# MAGIC * Years are 2022–2024 with 4 returns per year in the corrected batch, matching the train/validation/test split by year.
# MAGIC
# MAGIC These observations are exploration only; the enforced checks follow in step 2.

# COMMAND ----------

# DBTITLE 1,2. Decide which batch is usable
# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.
# MAGIC
# MAGIC Rules (from CHALLENGE.md data contract): **completeness** – `return_id`, `tax_year`, `income`, `deduction` must be non-null; **uniqueness** – each `return_id` occurs exactly once; **validity** – amounts nonnegative (zero income allowed). Feature output is only ever written through `gate()`, which raises on any violation.
# MAGIC
# MAGIC **Initial batch decision:** rejected. `R01` occurs twice (uniqueness) and `R02` has `income = NULL` (completeness). Imputing zero would invent a value (zero is a real income), and dropping rows would silently lose returns, so neither is a correction. The affected records and reasons are saved to `initial_rejected_records` in the output schema and `outputs/initial_rejected_records.csv`. **Next action:** report to the data owner and assess `returns_corrected` with the same rules; it is used for the final run only if it passes.

# COMMAND ----------

# DBTITLE 1,Quality gate and batch decision
# Data-contract checks. A batch is usable only if it has zero violations.
class DataQualityError(Exception):
    pass

def find_violations(table):
    """Return every affected record with the rule it breaks. Records are kept whole (not dropped)."""
    df = spark.table(table)
    parts = []
    for c in REQUIRED_FIELDS:  # completeness
        parts.append(df.filter(F.col(c).isNull())
                     .withColumn("rule", F.lit("completeness"))
                     .withColumn("reason", F.lit(f"required field {c} is null")))
    n = F.count(F.lit(1)).over(Window.partitionBy("return_id"))  # uniqueness
    parts.append(df.withColumn("n", n).filter("n > 1 AND return_id IS NOT NULL")
                 .withColumn("rule", F.lit("uniqueness"))
                 .withColumn("reason", F.concat(F.lit("return_id occurs "), F.col("n").cast("string"),
                                                F.lit(" times; each return must occur exactly once")))
                 .drop("n"))
    for c in ["income", "deduction"]:  # validity: amounts nonnegative (zero income is valid)
        parts.append(df.filter(F.col(c) < 0)
                     .withColumn("rule", F.lit("validity"))
                     .withColumn("reason", F.lit(f"{c} is negative")))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("source_table", F.lit(table)).orderBy("return_id", "rule")

def gate(table):
    """Raise DataQualityError if the batch breaks the contract; otherwise return the accepted DataFrame."""
    v = find_violations(table)
    if v.count() > 0:
        rules = sorted({(r.rule, r.return_id, r.reason) for r in v.collect()})
        raise DataQualityError(f"{table} rejected: {v.count()} affected record(s): {rules}")
    return spark.table(table)

# ---- Initial batch: must be rejected BEFORE any feature output is written ----
initial_violations = find_violations(INITIAL_TABLE)
display(initial_violations)
initial_rejected = False
try:
    gate(INITIAL_TABLE)
except DataQualityError as e:
    initial_rejected = True
    initial_rejection_message = str(e)
    print("REJECTED:", initial_rejection_message)
assert initial_rejected, "The initial batch was expected to fail the data contract"

# Preserve the affected records and reasons (table in own schema + small export)
(initial_violations.withColumn("checked_at_utc", F.lit(RUN_STARTED_UTC))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE))
initial_violations.toPandas().to_csv(OUTPUTS / "initial_rejected_records.csv", index=False)
(OUTPUTS / "initial_rejection.json").write_text(json.dumps({
    "table": INITIAL_TABLE, "decision": "rejected", "checked_at_utc": RUN_STARTED_UTC,
    "notebook": NOTEBOOK_URL, "saved_table": REJECTED_TABLE, "message": initial_rejection_message,
    "next_action": f"Reported to data owner; use replacement {CORRECTED_TABLE} after it passes the same checks.",
}, indent=2) + "\n", encoding="utf-8")

# ---- Replacement batch: same rules; must pass ----
corrected_violations = find_violations(CORRECTED_TABLE)
assert corrected_violations.count() == 0, "Replacement batch failed the data contract"
accepted = gate(CORRECTED_TABLE)
print(f"ACCEPTED: {CORRECTED_TABLE} with {accepted.count()} returns, 0 violations")
(OUTPUTS / "corrected_check.json").write_text(json.dumps({
    "table": CORRECTED_TABLE, "decision": "accepted", "checked_at_utc": RUN_STARTED_UTC,
    "rules": ["completeness of return_id, tax_year, income, deduction", "return_id unique", "amounts nonnegative"],
    "violations": 0, "accepted_rows": accepted.count(),
}, indent=2) + "\n", encoding="utf-8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build, save and verify return_features
# Features are built ONLY from a batch that passes gate(); a failing batch raises before any write.
def write_features(source_table):
    src = gate(source_table)
    feats = src.select(
        "return_id", "tax_year",
        F.when(F.col("income") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("deduction").cast("double") / F.col("income").cast("double"))
         .alias("deduction_to_income"))
    # Overwrite (not append) so reruns replace the table instead of adding copies
    feats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)
    return src.count()

# Demonstrate that the initial batch cannot produce feature output
try:
    write_features(INITIAL_TABLE)
    raise AssertionError("initial batch must not produce features")
except DataQualityError as e:
    print("Feature write blocked for initial batch:", str(e)[:120], "...")

n_source = write_features(CORRECTED_TABLE)

# ---- Read the saved table back and verify ----
saved = spark.table(FEATURES_TABLE)
display(saved.orderBy("return_id"))
assert saved.columns == ["return_id", "tax_year", "deduction_to_income"], saved.columns
assert saved.count() == n_source == saved.select("return_id").distinct().count(), "each accepted return exactly once"
src_ids = {r.return_id for r in spark.table(CORRECTED_TABLE).select("return_id").collect()}
assert {r.return_id for r in saved.select("return_id").collect()} == src_ids, "same IDs as accepted source"

# Nonzero case: expected value derived independently from the raw source row (plain Python arithmetic)
src_r04 = spark.table(CORRECTED_TABLE).filter("return_id = 'R04'").first()
expected_r04 = src_r04.deduction / src_r04.income          # 59 / 400
assert expected_r04 == 0.1475
got_r04 = saved.filter("return_id = 'R04'").first()
assert got_r04.tax_year == 2022 and abs(got_r04.deduction_to_income - expected_r04) < 1e-12
print(f"R04: source {src_r04.deduction}/{src_r04.income} = {expected_r04}; saved = {got_r04.deduction_to_income}")

# Zero-income cases stay in the table with a null ratio
zero_ids = sorted(r.return_id for r in spark.table(CORRECTED_TABLE).filter("income = 0").collect())
zero_saved = {r.return_id: r.deduction_to_income for r in saved.filter(F.col("return_id").isin(zero_ids)).collect()}
assert sorted(zero_saved) == zero_ids and all(v is None for v in zero_saved.values())
print("zero-income returns kept with null ratio:", zero_saved)

# Every row: saved ratio equals Python-computed ratio from the source
src_rows = {r.return_id: r for r in spark.table(CORRECTED_TABLE).collect()}
for r in saved.collect():
    s = src_rows[r.return_id]
    exp = None if s.income == 0 else s.deduction / s.income
    assert r.tax_year == s.tax_year and (r.deduction_to_income == exp if exp is None else abs(r.deduction_to_income - exp) < 1e-12)
print(f"Readback verified: {saved.count()} rows, columns {saved.columns}, no taxpayer_id or raw amounts")

# COMMAND ----------

# DBTITLE 1,4. Make the handoff usable
# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.
# MAGIC
# MAGIC **Handoff.** Input: `training.to6_challenge3_inputs.returns_corrected` (accepted; `returns_initial` rejected). Outputs in the assigned schema: `return_features`, `features_train` (2022), `features_validation` (2023), `features_test` (2024), plus `initial_rejected_records`. `deduction_to_income = deduction / income`, null when `income = 0`. Checks: completeness, uniqueness, nonnegative amounts, readback of columns/IDs/values incl. a nonzero expected value and zero-income nulls, split coverage/year assignment, overwrite-only history and an IDs-and-values comparison of two runs. Not verified: the upstream `parsed_source` itself and any model-leakage or prediction-time availability; no labels are supplied.

# COMMAND ----------

# DBTITLE 1,Year splits, exports and run evidence
# ---- Year splits built from the SAVED feature table (overwrite => no appended copies) ----
features = spark.table(FEATURES_TABLE)
for year, table in SPLITS.items():
    features.filter(F.col("tax_year") == year).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

# Verify: every accepted return in exactly one split, in its assigned year, with identical values
split_rows = {t: spark.table(t).collect() for t in SPLITS.values()}
for year, t in SPLITS.items():
    assert all(r.tax_year == year for r in split_rows[t]), f"{t} contains a wrong year"
    assert spark.table(t).columns == features.columns
all_split_ids = [r.return_id for rows in split_rows.values() for r in rows]
assert len(all_split_ids) == len(set(all_split_ids)), "a return appears in more than one split / twice"
feat_set = {tuple(r) for r in features.collect()}
assert {tuple(r) for rows in split_rows.values() for r in rows} == feat_set, "splits must equal features exactly"
assert features.count() == len(all_split_ids) == accepted.count()
print({t: len(rows) for t, rows in split_rows.items()}, "-> total", len(all_split_ids))

# Write history check: last operation on each table is a full overwrite, not an append
for t in [FEATURES_TABLE, *SPLITS.values()]:
    h = spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()
    print(t, "version", h.version, h.operation, h.operationParameters.get("mode"))
    assert h.operation != "WRITE" or h.operationParameters.get("mode") == "Overwrite"

# ---- Small synthetic exports of saved tables (read back from Unity Catalog) ----
for t in [FEATURES_TABLE, *SPLITS.values()]:
    spark.table(t).orderBy("return_id").toPandas().to_csv(OUTPUTS / f"{t.split('.')[-1]}.csv", index=False)

# ---- Run evidence: actual IDs and values for this run ----
import hashlib as _h
def snapshot(t):
    rows = [[r.return_id, r.tax_year, r.deduction_to_income] for r in spark.table(t).orderBy("return_id").collect()]
    return {"rows": rows, "row_count": len(rows), "sha256": _h.sha256(json.dumps(rows).encode()).hexdigest()}
run_record = {"run_started_utc": RUN_STARTED_UTC, "notebook_url": NOTEBOOK_URL, "source_table": CORRECTED_TABLE,
              "tables": {t: snapshot(t) for t in [FEATURES_TABLE, *SPLITS.values()]}}
run_dir = OUTPUTS / "runs"; run_dir.mkdir(exist_ok=True)
(run_dir / f"run_{RUN_STARTED_UTC.replace(':', '')}.json").write_text(json.dumps(run_record, indent=2) + "\n", encoding="utf-8")

# ---- Compare the two most recent successful runs ----
runs = sorted(run_dir.glob("run_*.json"))
if len(runs) >= 2:
    a, b = (json.loads(p.read_text()) for p in runs[-2:])
    comparison = {"run_a": {"file": runs[-2].name, "run_started_utc": a["run_started_utc"], "notebook_url": a["notebook_url"]},
                  "run_b": {"file": runs[-1].name, "run_started_utc": b["run_started_utc"], "notebook_url": b["notebook_url"]},
                  "tables": {t: {"identical_ids_and_values": a["tables"][t]["rows"] == b["tables"][t]["rows"],
                                 "row_count_a": a["tables"][t]["row_count"], "row_count_b": b["tables"][t]["row_count"],
                                 "sha256_a": a["tables"][t]["sha256"], "sha256_b": b["tables"][t]["sha256"]}
                             for t in a["tables"]}}
    comparison["all_identical"] = all(v["identical_ids_and_values"] for v in comparison["tables"].values())
    (OUTPUTS / "run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(comparison, indent=2))
    assert comparison["all_identical"], "Runs differ"
else:
    print("Only one run recorded so far; run the whole notebook again for the comparison.")