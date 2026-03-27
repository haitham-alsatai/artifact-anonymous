## Anonymous Artifact Package

This artifact package contains the code and derived result folders supporting
the experiments reported in the associated paper on budget-constrained
observability for microservice diagnosis.

The package covers two stages of experiments:

- `stage_a`: RCAEval-based experiments on trace-budget allocation and trace
  selection.
- `stage_b`: GAIA-based experiments on integrated observability-budget
  allocation and two follow-up analyses.

The raw benchmark datasets are public and are not redistributed here. Official
public links are listed in `DATA_SOURCES.txt`.

## Intended Repository Layout

The artifact is intended to be organized as follows:

```text
README.md
DATA_SOURCES.txt
LICENSE
requirements.txt

scripts/
  stage_a/
    run_trace_budget_experiment.py
    run_trace_policy_comparison_experiment.py
    run_trace_good_extensions_experiment.py
    run_trace_when_to_trace_pilot.py
  stage_b/
    run_gaia_integrated_budget_experiment.py
    gaia_weight_sensitivity.py
    gaia_ablation_eval.py

results/
  stage_a/
    trace_budget_experiment/
    trace_policy_experiment/
    trace_good_extensions_experiment/
    trace_when_pilot_experiment/
  stage_b/
    gaia_integrated_experiment/
    gaia_weight_sensitivity/
    gaia_ablation_experiment/
```

## Scripts

### Stage A: RCAEval

- `run_trace_budget_experiment.py`
  Runs the RCAEval trace-budget experiment comparing diagnosis quality across
  post-fault trace budgets.

- `run_trace_policy_comparison_experiment.py`
  Runs the RCAEval policy-comparison experiment for random versus
  latency-prioritized trace retention.

- `run_trace_good_extensions_experiment.py`
  Runs the RCAEval extension experiment with richer budget definitions and more
  structured trace-selection policies.

- `run_trace_when_to_trace_pilot.py`
  Runs the RCAEval timing experiment comparing random, early-window, and
  late-window retention.

### Stage B: GAIA

- `run_gaia_integrated_budget_experiment.py`
  Runs the GAIA MicroSS integrated observability-budget experiment over traces
  and metrics.

- `gaia_weight_sensitivity.py`
  Runs the GAIA weight-sensitivity follow-up experiment to study how scoring
  weights affect the integrated diagnosis results.

- `gaia_ablation_eval.py`
  Runs the GAIA ablation follow-up experiment to evaluate reduced or modified
  versions of the integrated scoring and allocation setup.

## Result Folders

### Stage A: RCAEval

- `trace_budget_experiment/`
  Derived outputs for the initial RCAEval trace-budget study, including
  case-level and summary-level CSV files.

- `trace_policy_experiment/`
  Derived outputs for the RCAEval policy-comparison study.

- `trace_good_extensions_experiment/`
  Derived outputs for the RCAEval richer-budget and structured-policy study.

- `trace_when_pilot_experiment/`
  Derived outputs for the RCAEval timing-aware retention pilot.

### Stage B: GAIA

- `gaia_integrated_experiment/`
  Derived outputs for the GAIA MicroSS integrated observability-budget study,
  including overall summaries, fault-wise summaries, family-wise summaries, and
  robustness summaries.

- `gaia_weight_sensitivity/`
  Derived outputs for the GAIA weight-sensitivity follow-up experiment.

- `gaia_ablation_experiment/`
  Derived outputs for the GAIA ablation follow-up experiment.

## Paper Mapping

The table below links the main paper elements to the corresponding scripts and
derived results. This is intended to help reviewers verify paper claims quickly.

| Paper element | Script | Results folder |
| --- | --- | --- |
| E1 / RQ1: RCAEval trace-budget study | `scripts/stage_a/run_trace_budget_experiment.py` | `results/stage_a/trace_budget_experiment/` |
| E2 / RQ2: RCAEval policy comparison | `scripts/stage_a/run_trace_policy_comparison_experiment.py` | `results/stage_a/trace_policy_experiment/` |
| E3: RCAEval richer-budget extension | `scripts/stage_a/run_trace_good_extensions_experiment.py` | `results/stage_a/trace_good_extensions_experiment/` |
| E4: RCAEval timing pilot | `scripts/stage_a/run_trace_when_to_trace_pilot.py` | `results/stage_a/trace_when_pilot_experiment/` |
| E5 / RQ3: GAIA integrated experiment | `scripts/stage_b/run_gaia_integrated_budget_experiment.py` | `results/stage_b/gaia_integrated_experiment/` |
| E6 / RQ4: GAIA ablation | `scripts/stage_b/gaia_ablation_eval.py` | `results/stage_b/gaia_ablation_experiment/` |
| E6 / RQ4: GAIA weight sensitivity | `scripts/stage_b/gaia_weight_sensitivity.py` | `results/stage_b/gaia_weight_sensitivity/` |

## Reproducibility Notes

- The scripts are written in Python.
- External Python dependencies used by the scripts are listed in
  `requirements.txt`.
- The package contains derived outputs for reviewer inspection. These outputs
  are intended to let a reviewer verify key numbers without rerunning the full
  experiments immediately.
- Raw benchmark datasets are public and linked in `DATA_SOURCES.txt`.

## Notes on Current Packaging

If this artifact is temporarily mirrored in a flat repository or uploaded as
archives during preparation, the intended final reviewer-facing organization is
the staged `scripts/` and `results/` layout shown above.
