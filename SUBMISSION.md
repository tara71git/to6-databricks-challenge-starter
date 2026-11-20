# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

**Inputs**

* `training.to6_challenge3_inputs.returns_initial`: first delivery, **rejected**.
* `training.to6_challenge3_inputs.returns_corrected`: replacement delivery, **accepted** and used for the final runs.
* Lineage: the supplied view `training.to6_challenge3_inputs.source_lineage` maps both delivery tables to the upstream table `training.to6_challenge3_inputs.parsed_source`.

**Row meaning**: each row is one synthetic tax return. `return_id` is the unique key and must occur exactly once. `taxpayer_id` identifies the filer, and one filer can have returns in several years. The required fields are `return_id`, `tax_year`, `income` and `deduction`.

**Outputs** (all in the assigned schema `training.to6_c3_1c198aff1c3c`)

* `training.to6_c3_1c198aff1c3c.return_features`: columns `return_id`, `tax_year`, `deduction_to_income` (12 rows).
* `training.to6_c3_1c198aff1c3c.features_train`: 2022 (4 rows).
* `training.to6_c3_1c198aff1c3c.features_validation`: 2023 (4 rows).
* `training.to6_c3_1c198aff1c3c.features_test`: 2024 (4 rows).
* `training.to6_c3_1c198aff1c3c.initial_rejected_records`: the affected initial records and the reason each was rejected.

**Ratio definition**: `deduction_to_income = deduction / income` (double). It is null when `income = 0`, and those returns stay in the table. The filer ID and the raw amounts are left out of the feature tables (need-to-know rule).

## Checks and decisions

* **Source references**: Catalog comments, 4 sample rows from each batch, full-batch profiles (returns by year, missing required values, duplicate IDs, zero income) and the lineage view. All are in notebook section 1.
* **Rules**: every required field is non-null (completeness), each `return_id` occurs exactly once (uniqueness), and amounts are nonnegative (validity). Feature tables are written only through `gate()`, which raises `DataQualityError` before any write when a batch breaks a rule.
* **Initial rejection**: `returns_initial` (13 rows, 12 distinct IDs) failed on 3 affected records:
  * `R01` (2022) appears twice. It breaks the uniqueness rule ("return_id occurs 2 times").
  * `R02` (2022) has `income = NULL`. It breaks the completeness rule ("required field income is null").
  * I did not set the missing income to zero, because zero is a real income value. I also did not drop rows silently.
  * Evidence: `outputs/initial_rejected_records.csv`, `outputs/initial_rejection.json` (the batch was rejected in both runs; the file holds the latest rejection, from run 2 at 2026-11-20T16:35:16Z) and the table `initial_rejected_records`.
  * The notebook also shows that `write_features(returns_initial)` is blocked.
  * Next action: report the defects to the data owner and use the replacement batch.
* **Replacement checks**: `returns_corrected` was checked with the same rules. Result: 12 rows, 12 distinct IDs, 0 violations (`outputs/corrected_check.json`).
* **Nonzero expected value**: I calculated the value for `R04` (2022) from its source row, deduction 59 / income 400 = **0.1475**. The saved table reads back 0.1475. Every other row was also recalculated in Python from the source and compared within 1e-12.
* **Saved-table readback**:
  * The columns are exactly `return_id`, `tax_year`, `deduction_to_income`.
  * There are 12 rows and 12 distinct IDs, the same set as the accepted source.
  * The zero-income returns `R03` (2022) and `R09` (2024) are present with a null ratio.

## Two successful runs

| Run | Notebook URL | Execution timestamp (UTC) | Evidence |
| --- | --- | --- | --- |
| 1 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558664 | 2026-11-20T16:33:32Z | `outputs/runs/run_2026-11-20T163332Z.json` |
| 2 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558664 | 2026-11-20T16:35:16Z | `outputs/runs/run_2026-11-20T163516Z.json` |

* Both were interactive runs of every task cell in order, so there is no Jobs run ID.
* Each run file stores the actual `return_id`, `tax_year` and `deduction_to_income` rows of all four tables, plus a SHA-256 of those rows.
* `outputs/run_comparison.json` compares the rows of both runs table by table. Result: `identical_ids_and_values: true` for all four tables (`all_identical: true`).
* **Year assignment**: each split table contains only its year (2022, 2023 or 2024).
* **Each accepted return exactly once**: across the 3 split tables there are 12 IDs and none repeats. Together the split rows equal the `return_features` rows exactly.
* **No appended copies**: every table is written in overwrite mode. Row counts stayed at 12/4/4/4 after the second run. Delta history shows the latest operation on each table is `CREATE OR REPLACE TABLE AS SELECT` (version 0 after run 1, version 1 after run 2). There were no APPEND writes.

## Final files and limitations

**Final exports in `outputs/`**

* `return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`: read back from the saved tables.
* `initial_rejected_records.csv`, `initial_rejection.json`: the rejected initial batch, its affected records and the reasons.
* `corrected_check.json`: the replacement batch check.
* `runs/run_2026-11-20T163332Z.json`, `runs/run_2026-11-20T163516Z.json`, `run_comparison.json`: the two runs and their comparison.
* `connection-practice.json` is the ungraded practice file and is not part of the task evidence.

**Not verified**

* I did not inspect the upstream `parsed_source` table directly; the lineage comes from the supplied view.
* I checked only that the year splits match the tax years. I did not show that they are free of model leakage, and I did not check which fields will be available at prediction time.
* No target labels are supplied.
* The rejected-records export keeps the full original records, including `taxpayer_id`, as rejection evidence. This is synthetic data. The feature tables exclude `taxpayer_id`.
