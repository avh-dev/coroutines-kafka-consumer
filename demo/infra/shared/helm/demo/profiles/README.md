# Demo Helm Profiles

Deployment profiles are grouped by the environment that selects them:

- `aws/` contains profiles referenced by AWS test definitions.
- `internal-lab/` contains profiles offered by the interactive internal-lab runner.

Keep a separate copy when both environments need a similarly named preset. This
allows each runner to discover only its own profiles and evolve them independently.
