from functools import lru_cache

from .config_loader import load_config, render_template


@lru_cache(maxsize=1)
def get_config() -> dict:
    """Return merged global + project config (cached after first call)."""
    return load_config()


def resolve(template: str) -> str:
    """Substitute {key} config placeholders in *template* using the merged config."""
    return render_template(template, get_config())
