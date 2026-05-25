from __future__ import annotations

from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Label, ListItem

from .theme import semantic_status

SELECTION_RAIL = "▌"


@dataclass(frozen=True)
class CatalogTable:
    namespace: tuple[str, ...]
    name: str
    identifier: str
    table_format: str = "UNKNOWN"
    freshness: str = "not analyzed"


class NamespaceListItem(ListItem):
    def __init__(
        self, name: tuple[str, ...], display_name: str, meta: str = ""
    ) -> None:
        super().__init__()
        self.namespace = name
        self.display_name = display_name
        self.meta = meta

    def compose(self) -> ComposeResult:
        with Horizontal(classes="namespace-row"):
            yield Label(SELECTION_RAIL, classes="selection-rail")
            yield Label(self.display_name, classes="namespace-name")
            yield Label(self.meta, classes="row-meta")


class TableListItem(ListItem):
    def __init__(self, table: CatalogTable) -> None:
        super().__init__()
        self.table = table

    def compose(self) -> ComposeResult:
        cache_status = semantic_status(self.table.freshness)
        with Horizontal(classes="table-row"):
            yield Label(SELECTION_RAIL, classes="selection-rail")
            yield Label(self.table.name, classes="table-name")
            yield Label(
                self.table.table_format,
                classes=f"table-format {self._badge_class}",
            )
            yield Label(
                cache_status.label,
                classes=f"table-cache {cache_status.css_class}",
            )

    @property
    def _badge_class(self) -> str:
        status = semantic_status(self.table.table_format)
        return (
            f"{self.table.table_format.lower().replace('_', '-')} "
            f"{status.css_class}"
        )
