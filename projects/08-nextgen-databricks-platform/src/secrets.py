"""
secrets.py
----------
Azure Key Vault integration via Databricks secret scopes.

We never put credentials in code or notebooks. Each environment has a Databricks
secret scope that is *backed by* an Azure Key Vault (created once with:
  databricks secrets create-scope kv-prod \
     --scope-backend-type AZURE_KEYVAULT \
     --resource-id <key-vault-resource-id> \
     --dns-name https://feodor-prod-kv.vault.azure.net/
). Reading a secret then transparently reads from Key Vault, with access governed
by the workspace's managed identity + Key Vault access policies / RBAC.

Author: Feodor Fernando
"""
from __future__ import annotations

from config.environments import EnvConfig


def get_secret(dbutils, cfg: EnvConfig, key: str) -> str:
    """Read a secret from the environment's Key Vault-backed scope."""
    return dbutils.secrets.get(scope=cfg.secret_scope, key=key)


def configure_storage_access(spark, dbutils, cfg: EnvConfig) -> None:
    """Wire ADLS gen2 access using a service-principal whose secret lives in Key Vault.

    Preferred in real deployments: a Unity Catalog storage credential + external
    location (no keys in Spark conf at all). This SP path is shown for clusters/
    workspaces not yet on UC external locations.
    """
    client_id = get_secret(dbutils, cfg, "sp-client-id")
    client_secret = get_secret(dbutils, cfg, "sp-client-secret")
    tenant_id = get_secret(dbutils, cfg, "tenant-id")
    acct = cfg.storage_account

    spark.conf.set(f"fs.azure.account.auth.type.{acct}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{acct}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{acct}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{acct}.dfs.core.windows.net", client_secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{acct}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )
