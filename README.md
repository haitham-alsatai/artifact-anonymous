## Anonymous Artifact Package

This artifact package contains the code and derived result folders supporting the experiments reported in the paper on budget-constrained observability for microservice diagnosis.

### Contents

#### Scripts

- `run_trace_budget_experiment.py`
  Runs the RCAEval trace-budget experiment comparing diagnosis quality across post-fault trace budgets.

- `run_trace_policy_comparison_experiment.py`
  Runs the RCAEval policy-comparison experiment for random versus latency-prioritized trace retention.

- `run_trace_good_extensions_experiment.py`
  Runs the RCAEval extension experiment with richer budget definitions and more structured trace-selection policies.

- `run_trace_when_to_trace_pilot.py`
  Runs the RCAEval timing experiment comparing random, early-window, and late-window retention.

- `run_gaia_integrated_budget_experiment.py`
  Runs the GAIA MicroSS integrated observability-budget experiment over traces and metrics.

- `gaia_weight_sensitivity.py`
  Runs the GAIA weight-sensitivity follow-up experiment to study how the scoring weights affect the integrated diagnosis results.

- `gaia_ablation_eval.py`
  Runs the GAIA ablation follow-up experiment to evaluate reduced or modified versions of the integrated scoring and allocation setup.

#### Result Folders

- `trace_budget_experiment/`
  Derived outputs for the initial RCAEval trace-budget study, including case-level and summary-level CSV files.

- `trace_policy_experiment/`
  Derived outputs for the RCAEval policy-comparison study.

- `trace_good_extensions_experiment/`
  Derived outputs for the RCAEval richer-budget and structured-policy study.

- `trace_when_pilot_experiment/`
  Derived outputs for the RCAEval timing-aware retention pilot.

- `gaia_integrated_experiment/`
  Derived outputs for the GAIA MicroSS integrated observability-budget study, including overall summaries, fault-wise summaries, family-wise summaries, and robustness summaries.

- `gaia_weight_sensitivity/`
  Derived outputs for the GAIA weight-sensitivity follow-up experiment.

- `gaia_ablation_experiment/`
  Derived outputs for the GAIA ablation follow-up experiment.

### Relation to the Paper

This artifact supports the experimental results described in the paper. The package includes the scripts used to generate the experiments and the derived result folders used to analyze and summarize the findings.

The artifact is centered on the six main experiments reported in the paper, and it can also include additional GAIA follow-up studies such as weight-sensitivity analysis and ablation analysis when these are part of the current submission package.

### Dataset Availability

The raw datasets are public and are not re-distributed in this folder. Official public dataset links are listed separately in `DATA_SOURCES.txt`.
