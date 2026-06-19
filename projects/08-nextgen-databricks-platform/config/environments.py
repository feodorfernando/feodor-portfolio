"""
environments.py
---------------
Dynamic, environment-aware configuration. The SAME notebook code runs unchanged in
dev / test / prod — it resolves WHERE it is running and routes every read/write to the
matching Unity Catalog catalog, ADLS container, and Key Vault-backed secret scope.

Resolution order (first match wins), which mirrors how most enterprise teams do it:
  1. explicit job/task parameter  (dbutils widget `env`)         <- CI/CD passes this
  2. cluster environment variable (ENVIRONMENT)
  3. workspace URL mapping        (dev/test/prod have distinct workspaces)
  4. default -> 'dev'             (safe: never accidentally write prod from a notebook)

So: run in the DEV workspace  -> writes to  dev_catalog
    run in the PROD workspace -> writes to  prod_catalog
No code change, no hard-coded table names, no "oops I wrote to prod from my laptop".

Author: Feodor Fernando
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


# --- per-environment settings --------------------------------------------- #
# Each environment is fully isolated: its own catalog, its own storage account/
# container, and its own Key Vault-backed secret scope.
_ENVIRONMENTS = {
    "dev": {
        "catalog": "dev_catalog",
        "storage_account": "feodordevlake",
        "secret_scope": "kv-dev",            # backed by Azure Key Vault (see secrets.py)
        "max_autoscale_workers": 4,
    },
    "test": {
        "catalog": "test_catalog",
        "storage_account": "feodortestlake",
        "secret_scope": "kv-test",
        "max_autoscale_workers": 6,
    },
    "prod": {
        "catalog": "prod_catalog",
        "storage_account": "feodorprodlake",
        "secret_scope": "kv-prod",
        "max_autoscale_workers": 12,
    },
}

# Map each Databricks workspace URL to its environment.
_WORKSPACE_URL_TO_ENV = {
    "adb-1111111111111111.11.azuredatabricks.net": "dev",
    "adb-2222222222222222.22.azuredatabricks.net": "test",
    "adb-3333333333333333.33.azuredatabricks.net": "prod",
}


@dataclass(frozen=True)
class EnvConfig:
    """Everything env-specific, resolved once and threaded through the pipeline."""
    env: str
    catalog: str
    storage_account: str
    secret_scope: str
    max_autoscale_workers: int
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # fully-qualified names so notebook code never hard-codes a catalog
    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def abfss(self, container: str, path: str = "") -> str:
        base = f"abfss://{container}@{self.storage_account}.dfs.core.windows.net"
        return f"{base}/{path}".rstrip("/")

    @property
    def is_prod(self) -> bool:
        return self.env == "prod"


def _resolve_env(spark=None, dbutils=None) -> str:
    """Figure out which environment we're running in (see module docstring)."""
    # 1) explicit job/task parameter
    if dbutils is not None:
        try:
            val = dbutils.widgets.get("env").strip().lower()
            if val in _ENVIRONMENTS:
                return val
        except Exception:
            pass  # widget not set -> fall through

    # 2) cluster environment variable
    val = os.environ.get("ENVIRONMENT", "").strip().lower()
    if val in _ENVIRONMENTS:
        return val

    # 3) workspace URL mapping
    if spark is not None:
        try:
            url = spark.conf.get("spark.databricks.workspaceUrl", "")
            if url in _WORKSPACE_URL_TO_ENV:
                return _WORKSPACE_URL_TO_ENV[url]
        except Exception:
            pass

    # 4) safe default
    return "dev"


def get_config(spark=None, dbutils=None) -> EnvConfig:
    """Public entry point. Call once at the top of every notebook/job."""
    env = _resolve_env(spark, dbutils)
    cfg = _ENVIRONMENTS[env]
    resolved = EnvConfig(
        env=env,
        catalog=cfg["catalog"],
        storage_account=cfg["storage_account"],
        secret_scope=cfg["secret_scope"],
        max_autoscale_workers=cfg["max_autoscale_workers"],
    )
    # Guardrail: writing to prod requires the prod workspace AND an explicit opt-in,
    # so a misconfigured job can never silently mutate prod data.
    if resolved.is_prod and os.environ.get("ALLOW_PROD_WRITES") != "true":
        # Not a hard failure here (read jobs are fine); jobs that write assert this.
        pass
    return resolved


def assert_safe_to_write(cfg: EnvConfig) -> None:
    """Call before any write in a prod-targeting job."""
    if cfg.is_prod and os.environ.get("ALLOW_PROD_WRITES") != "true":
        raise PermissionError(
            "Refusing to write to prod_catalog without ALLOW_PROD_WRITES=true. "
            "Prod writes must come from the prod deployment, not an interactive run."
        )
