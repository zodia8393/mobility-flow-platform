from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TrafficObservationEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0", "1.1"] = "1.1"
    event_id: str = Field(min_length=8, max_length=80)
    trace_id: str = Field(min_length=8, max_length=80)
    segment_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{2,79}$")
    road_name: str = Field(min_length=1, max_length=100)
    area_code: str | None = Field(default=None, max_length=30)
    area_name: str | None = Field(default=None, max_length=100)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    start_latitude: float | None = Field(default=None, ge=-90, le=90)
    start_longitude: float | None = Field(default=None, ge=-180, le=180)
    end_latitude: float | None = Field(default=None, ge=-90, le=90)
    end_longitude: float | None = Field(default=None, ge=-180, le=180)
    observed_at: datetime
    ingested_at: datetime
    source_system: Literal["TMAP", "TCS", "VDS", "GPS", "SEOUL_TOPIS"]
    vehicle_type: Literal["PASSENGER", "BUS", "TRUCK", "ALL"]
    segment_length_km: float = Field(gt=0, le=100)
    speed_kph: float = Field(gt=0, le=180)
    reference_speed_kph: float | None = Field(default=None, gt=0, le=180)
    traffic_volume: int | None = Field(default=None, ge=0, le=20_000)
    travel_time_seconds: float = Field(gt=0, le=7_200)
    source_congestion_level: Literal[
        "SEVERE", "CONGESTED", "SLOW", "SMOOTH", "UNKNOWN"
    ] | None = None
    source_payload_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def validate_timestamps(self) -> "TrafficObservationEvent":
        if self.observed_at.tzinfo is None or self.ingested_at.tzinfo is None:
            raise ValueError("timestamps must include timezone information")
        return self


class DlqEnvelope(BaseModel):
    failed_at: datetime
    error_type: str
    error_message: str
    source_topic: str
    source_partition: int
    source_offset: int
    payload: str


class TransformRequest(BaseModel):
    run_id: str | None = None
    force_reprocess: bool = False
    prefix: str = "bronze/seoul_citydata/"


class TransformResult(BaseModel):
    run_id: str
    status: Literal["TRANSFORMED", "NO_DATA", "FAILED"]
    source_objects: int = 0
    input_rows: int = 0
    accepted_rows: int = 0
    duplicates_removed: int = 0
    late_rows: int = 0
    output_objects: int = 0
    job_duration_seconds: float = 0
    details: dict[str, Any] = Field(default_factory=dict)


class PipelineRunUpdate(BaseModel):
    status: Literal["RUNNING", "TRANSFORMED", "SUCCESS", "FAILED", "NO_DATA"]
    dbt_status: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class SeoulSyncRequest(BaseModel):
    areas: list[str] | None = None
