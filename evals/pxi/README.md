# PXI Evals

This tree is the canonical home for PXI-specific eval work.

## Layout

- `harness/` runs live PXI agent experiments against Phoenix datasets.
- `datasets/` stores YAML datasets shared by harness and CI workflows.
- `evaluators/` stores code evaluators for PXI tool behavior.
- `tests/` contains fast unit coverage for the harness and evaluators.
- `trace_ingest/` is reserved for future trace-to-dataset tooling.

## Splits

Every dataset example must declare list-shaped `splits: [...]`, even when the
example belongs to only one split:

```yaml
examples:
  - id: llm-spans
    splits: [regression]
    input:
      query: Show me only LLM spans.
```

The harness defaults to the `regression` split; `dev` is for manual
experimentation, `val` is reserved for optimizer scoring, and `holdout` is
manual-only.

Examples may carry more than one split tag, but `val` must stay disjoint from
both `regression` and `dev`. The loader enforces that contract and warns when an
example is tagged with both `regression` and `holdout`.

## Manual CI

The `PXI Evals` GitHub Actions workflow runs live regression evals against
Phoenix Cloud. It is intentionally manual-only: maintainers run it from the
Actions tab on PR branches that change PXI evals or PXI agent behavior.

The workflow invokes the runner for every YAML file in
`evals/pxi/datasets/*.yaml` with `--splits regression` and
`--fail-on-regression`. The runner skips datasets when the requested split has
no regression examples. Each dataset run is printed as its own log group with
the dataset file and CI experiment name. The workflow keeps going after
individual dataset failures, and the final status is red if any dataset fails.
