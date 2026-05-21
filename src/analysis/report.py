from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple, Union


MetricValue = Optional[Union[int, float, str]]


@dataclass(frozen=True)
class TableSource:
    kind: str
    location: str


@dataclass(frozen=True)
class HealthMetric:
    key: str
    label: str
    value: MetricValue
    unit: Optional[str]
    source: str

    @property
    def is_unknown(self) -> bool:
        return self.value is None


@dataclass(frozen=True)
class DisplayStatistic:
    key: str
    label: str
    value: MetricValue
    derived_from: Tuple[str, ...]


@dataclass(frozen=True)
class CalculationWarning:
    metric_key: str
    message: str


@dataclass(frozen=True)
class PartitionHealthMetric:
    partition: Mapping[str, Any]
    data_file_count: int
    delete_file_count: int
    total_data_file_size_bytes: MetricValue
    average_data_file_size_bytes: MetricValue
    source: str


@dataclass(frozen=True)
class TableHealthReport:
    table_name: str
    table_source: TableSource
    health_metrics: Tuple[HealthMetric, ...]
    display_statistics: Tuple[DisplayStatistic, ...]
    calculation_warnings: Tuple[CalculationWarning, ...] = ()
    partition_metrics: Tuple[PartitionHealthMetric, ...] = ()

    def health_metric(self, key: str) -> HealthMetric:
        for metric in self.health_metrics:
            if metric.key == key:
                return metric
        raise KeyError(key)

    def display_statistic(self, key: str) -> DisplayStatistic:
        for statistic in self.display_statistics:
            if statistic.key == key:
                return statistic
        raise KeyError(key)
