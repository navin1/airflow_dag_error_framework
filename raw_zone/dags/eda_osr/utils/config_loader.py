import json
from pathlib import Path

# Resolved relative to this file: utils/ → eda_osr/ → dags/ → raw_zone/ → project_root/
_GLOBAL_CONFIG_PATH = Path(__file__).parents[4] / "common" / "config" / "airflow_global_config.json"
_PROJECT_CONFIG_PATH = Path(__file__).parents[1] / "config" / "eda_osr_airflow_config.json"


def load_config() -> dict:
    """Merge global and project config dicts; project keys take precedence."""
    with open(_GLOBAL_CONFIG_PATH) as f:
        global_cfg: dict = json.load(f)
    with open(_PROJECT_CONFIG_PATH) as f:
        project_cfg: dict = json.load(f)
    return {**global_cfg, **project_cfg}


def render_template(template: str, config: dict) -> str:
    """Replace {key} placeholders using config.

    Uses explicit per-key replacement so Airflow Jinja vars ({{ ds }}, etc.)
    are left untouched for the operator's own template engine.
    """
    result = template
    for key, value in config.items():
        result = result.replace(f"{{{key}}}", str(value))
    return result
