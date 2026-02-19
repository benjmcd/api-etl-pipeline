from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_db_path: Path = Field(default=Path("./data/api_etl_pipeline.db"), alias="APP_DB_PATH")
    app_blob_dir: Path = Field(default=Path("./blobs"), alias="APP_BLOB_DIR")
    app_run_dir: Path | None = Field(default=None, alias="APP_RUN_DIR")
    app_capture_pretty_max_bytes: int = Field(
        default=2_000_000,
        alias="APP_CAPTURE_PRETTY_MAX_BYTES",
    )
    app_capture_gzip_min_bytes: int = Field(
        default=5_000_000,
        alias="APP_CAPTURE_GZIP_MIN_BYTES",
    )
    sec_user_agent: str | None = Field(default=None, alias="SEC_USER_AGENT")
    nrc_subscription_key: str | None = Field(default=None, alias="NRC_SUBSCRIPTION_KEY")
    nrc_aps_subscription_key: str | None = Field(default=None, alias="NRC_APS_SUBSCRIPTION_KEY")

    @property
    def resolved_nrc_subscription_key(self) -> str | None:
        return self.nrc_subscription_key or self.nrc_aps_subscription_key

    @property
    def resolved_run_dir(self) -> Path:
        if self.app_run_dir is not None:
            return self.app_run_dir
        return self.app_db_path.parent / "runs"

    @model_validator(mode="after")
    def normalize_paths(self) -> "AppSettings":
        self.app_db_path = self.app_db_path.expanduser()
        self.app_blob_dir = self.app_blob_dir.expanduser()
        if self.app_run_dir is not None:
            self.app_run_dir = self.app_run_dir.expanduser()
        return self
