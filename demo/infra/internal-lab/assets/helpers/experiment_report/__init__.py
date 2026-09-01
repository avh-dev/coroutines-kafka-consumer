import sys

from demo.infra.shared import experiment_report as _shared
from demo.infra.shared.experiment_report import analyze, generate, markdown, model, prometheus, svg
from demo.infra.shared.experiment_report import *  # noqa: F403


for _name, _module in {
    "analyze": analyze,
    "generate": generate,
    "markdown": markdown,
    "model": model,
    "prometheus": prometheus,
    "svg": svg,
}.items():
    sys.modules[f"{__name__}.{_name}"] = _module
