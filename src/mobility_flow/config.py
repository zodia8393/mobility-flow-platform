from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_env: str = "local"
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic: str = "mobility.traffic-observation.v1"
    kafka_dlq_topic: str = "mobility.traffic-observation.dlq.v1"
    kafka_consumer_group: str = "mobility-traffic-bronze-writer-v1"

    s3_endpoint_url: str | None = "http://localhost:19000"
    s3_bucket: str = "mobility-lake"
    aws_access_key_id: str | None = "mobility_minio"
    aws_secret_access_key: str | None = "mobility_minio_local"
    aws_region: str = "ap-northeast-2"

    database_url: str = "postgresql://mobility:mobility_local@localhost:15433/mobility"
    ops_token: str = "local-ops-token"

    bronze_batch_size: int = 500
    bronze_flush_seconds: float = 5.0
    metrics_port: int = 9101
    spark_master: str = "local[2]"
    spark_driver_memory: str = "1g"

    @property
    def is_aws_object_store(self) -> bool:
        return not self.s3_endpoint_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
