from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Engine = Literal["rust"]


class RunMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    tool: str = "LogsReaper"
    tool_version: str
    run_id: str
    created_at: str
    service_name: str | None = None
    invocation_command: str | None = None
    input_globs: list[str]
    input_files: list[str]
    file_count: int = Field(ge=0)
    event_count: int = Field(ge=0)
    template_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    lib_versions: dict[str, str] = Field(default_factory=dict)
    rules_path: str | None = None
    baseline_dir: str | None = None
    hash_algorithm: str
    runtime_counts: dict[str, int]
    parse_status: dict[str, int]
    autodiscovery: dict[str, Any] | None = None
    instances: dict[str, Any] | None = None
    focus: str = "both"
    connectivity_timeline: dict[str, Any] | None = None
    engine: Engine
    scan_duration_seconds: float = Field(ge=0.0)
    input_bytes: int = Field(ge=0)
    input_gigabytes: float = Field(ge=0.0)
    throughput_gb_per_second: float = Field(ge=0.0)
    events_per_second: float = Field(ge=0.0)


class Summary(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_count: int = Field(ge=0)
    template_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    severity_counts: dict[str, int]
    classification_counts: dict[str, int]
    parse_status: dict[str, int]
    scan_duration_seconds: float = Field(ge=0.0)
    input_bytes: int = Field(ge=0)
    input_gigabytes: float = Field(ge=0.0)
    throughput_gb_per_second: float = Field(ge=0.0)
    events_per_second: float = Field(ge=0.0)
    top_templates: list[dict[str, Any]]
