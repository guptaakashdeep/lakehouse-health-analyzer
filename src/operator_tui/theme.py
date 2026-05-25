from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticStatus:
    label: str
    css_class: str


@dataclass(frozen=True)
class LayoutState:
    workspace_class: str
    show_workspace: bool
    compact_chrome: bool
    minimum_message: str = ""


_SEMANTIC_LABELS = {
    "critical": "CRITICAL",
    "warning": "WARNING",
    "info": "INFO",
    "healthy": "HEALTHY",
    "unknown": "UNKNOWN",
    "cached": "CACHED",
    "stale": "STALE",
}

_ALIASES = {
    "error": "critical",
    "failed": "critical",
    "non-iceberg": "critical",
    "setup-needed": "warning",
    "analyzing": "info",
    "refreshing": "info",
    "loading": "info",
    "fresh": "healthy",
    "iceberg": "healthy",
    "loaded": "healthy",
    "no-data": "unknown",
    "not-analyzed": "unknown",
}


def semantic_status(raw_status: object) -> SemanticStatus:
    key = _normalize(raw_status)
    semantic_key = _ALIASES.get(key, key)
    if semantic_key not in _SEMANTIC_LABELS:
        semantic_key = "unknown"
    label = _SEMANTIC_LABELS.get(key, _display_label(raw_status))
    return SemanticStatus(
        label=label,
        css_class=f"semantic-{semantic_key}",
    )


def layout_state_for_size(width: int, height: int) -> LayoutState:
    if width < 70 or height < 18:
        return LayoutState(
            workspace_class="layout-minimum",
            show_workspace=False,
            compact_chrome=True,
            minimum_message=(
                "Terminal too small. Resize to at least 70 columns by 18 rows."
            ),
        )
    if width < 112:
        return LayoutState(
            workspace_class="layout-compact",
            show_workspace=True,
            compact_chrome=True,
        )
    return LayoutState(
        workspace_class="layout-wide",
        show_workspace=True,
        compact_chrome=False,
    )


def _normalize(raw_status: object) -> str:
    return str(raw_status or "unknown").strip().lower().replace("_", "-").replace(
        " ", "-"
    )


def _display_label(raw_status: object) -> str:
    label = str(raw_status or "unknown").strip().upper().replace("_", " ")
    return label or "UNKNOWN"
