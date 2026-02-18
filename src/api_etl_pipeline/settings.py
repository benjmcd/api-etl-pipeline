from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_db_path: Path = Field(default=Path("./data/api_etl_pipeline.db"), alias="APP_DB_PATH")
    app_blob_dir: Path = Field(default=Path("./blobs"), alias="APP_BLOB_DIR")
    sec_user_agent: str | None = Field(default=None, alias="SEC_USER_AGENT")
    nrc_subscription_key: str | None = Field(default=None, alias="NRC_SUBSCRIPTION_KEY")
    nrc_aps_subscription_key: str | None = Field(default=None, alias="NRC_APS_SUBSCRIPTION_KEY")

    @property
    def resolved_nrc_subscription_key(self) -> str | None:
        return self.nrc_subscription_key or self.nrc_aps_subscription_key

    @model_validator(mode="after")
    def normalize_paths(self) -> "AppSettings":
        self.app_db_path = self.app_db_path.expanduser()
        self.app_blob_dir = self.app_blob_dir.expanduser()
        return self
