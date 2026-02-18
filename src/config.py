from __future__ import annotations

from typing import List, Optional

from pydantic import AnyHttpUrl
try:
    # Prefer pydantic-settings (v2+) which provides SettingsConfigDict and BaseSettings
    from pydantic_settings import (
        BaseSettings,
        SettingsConfigDict,
    )

    class AppConfig(BaseSettings):
        """Application configuration using pydantic-settings (v2) settings API.

        Field names map to environment variables by default (uppercase).
        """

        model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

        # GitHub
        github_events_url: AnyHttpUrl = "https://api.github.com/events"
        poll_interval_seconds: int = 5
        github_token: Optional[str] = None

        # Kafka
        kafka_bootstrap_servers: List[str] = ["localhost:9092"]
        kafka_topic_prefix: str = "github_events"
        dlq_topic: str = "github_events_dlq"

        # Producer / Retry
        request_max_retries: int = 3
        request_backoff_seconds: float = 1.0
        kafka_produce_timeout: int = 10

        # Avro / Serialization
        use_avro: bool = False
        avro_schema_dir: Optional[str] = None

        # Rate limiting (GitHub API)
        rate_limit_per_hour: int = 5000

        # Metrics
        metrics_enabled: bool = False
        metrics_port: int = 8000
        metrics_log_interval_seconds: int = 30

        # Logging
        log_level: str = "INFO"

        # Spark / Consumer
        spark_app_name: str = "github-events-consumer"
        spark_master: Optional[str] = None
        checkpoint_dir: str = "./checkpoints"
        output_path: str = "./output"
        consumer_poll_interval_seconds: int = 10

        # Producer backpressure
        max_in_flight: int = 200
        backpressure_acquire_timeout: float = 2.0
        backpressure_block: bool = False
        backpressure_backoff_seconds: float = 1.0

        def __init__(self, **values):
            # Allow comma-separated string for kafka_bootstrap_servers
            servers = values.get("kafka_bootstrap_servers")
            if isinstance(servers, str):
                servers_list = [
                    s.strip()
                    for s in servers.split(",")
                    if s.strip()
                ]
                values["kafka_bootstrap_servers"] = servers_list
            super().__init__(**values)

except Exception:
    # Fallback: try to use pydantic BaseSettings (older environments)
    try:
        from pydantic import (
            BaseSettings,
            Field,
        )

        class AppConfig(BaseSettings):
            github_events_url: AnyHttpUrl = Field("https://api.github.com/events")
            poll_interval_seconds: int = Field(5)
            github_token: Optional[str] = Field(None)

            kafka_bootstrap_servers: List[str] = Field(
                default_factory=lambda: ["localhost:9092"]
            )
            kafka_topic_prefix: str = Field("github_events")
            dlq_topic: str = Field("github_events_dlq")

            request_max_retries: int = Field(3)
            request_backoff_seconds: float = Field(1.0)
            kafka_produce_timeout: int = Field(10)

            use_avro: bool = Field(False)
            avro_schema_dir: Optional[str] = Field(None)

            rate_limit_per_hour: int = Field(5000)

            metrics_enabled: bool = Field(False)
            metrics_port: int = Field(8000)
            metrics_log_interval_seconds: int = Field(30)

            log_level: str = Field("INFO")

            spark_app_name: str = Field("github-events-consumer")
            spark_master: Optional[str] = Field(None)
            checkpoint_dir: str = Field("./checkpoints")
            output_path: str = Field("./output")
            consumer_poll_interval_seconds: int = Field(10)

            max_in_flight: int = Field(200)
            backpressure_acquire_timeout: float = Field(2.0)
            backpressure_block: bool = Field(False)
            backpressure_backoff_seconds: float = Field(1.0)

            class Config:
                env_file = ".env"
                env_file_encoding = "utf-8"

            def __init__(self, **values):
                servers = values.get("kafka_bootstrap_servers")
                if isinstance(servers, str):
                    servers_list = [
                        s.strip()
                        for s in servers.split(",")
                        if s.strip()
                    ]
                    values["kafka_bootstrap_servers"] = servers_list
                super().__init__(**values)

    except Exception:
        # As a last resort, provide a minimal dataclass-like fallback.
        # Prevents import failures in tests/environments without BaseSettings.
        from pydantic import BaseModel as _BaseModel

        class AppConfig(_BaseModel):
            github_events_url: AnyHttpUrl = "https://api.github.com/events"
            poll_interval_seconds: int = 5
            github_token: Optional[str] = None

            kafka_bootstrap_servers: List[str] = ["localhost:9092"]
            kafka_topic_prefix: str = "github_events"
            dlq_topic: str = "github_events_dlq"

            request_max_retries: int = 3
            request_backoff_seconds: float = 1.0
            kafka_produce_timeout: int = 10

            use_avro: bool = False
            avro_schema_dir: Optional[str] = None

            rate_limit_per_hour: int = 5000

            metrics_enabled: bool = False
            metrics_port: int = 8000
            metrics_log_interval_seconds: int = 30

            log_level: str = "INFO"

            spark_app_name: str = "github-events-consumer"
            spark_master: Optional[str] = None
            checkpoint_dir: str = "./checkpoints"
            output_path: str = "./output"
            consumer_poll_interval_seconds: int = 10

            max_in_flight: int = 200
            backpressure_acquire_timeout: float = 2.0
            backpressure_block: bool = False
            backpressure_backoff_seconds: float = 1.0


__all__ = ["AppConfig"]
