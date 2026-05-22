import asyncio

from operator_tui.app import OperatorCatalogBrowserApp
from operator_tui.theme import layout_state_for_size, semantic_status
from textual.widgets import Static


def test_semantic_statuses_use_text_labels_and_accessible_classes():
    expectations = {
        "critical": ("CRITICAL", "semantic-critical"),
        "warning": ("WARNING", "semantic-warning"),
        "info": ("INFO", "semantic-info"),
        "healthy": ("HEALTHY", "semantic-healthy"),
        "unknown": ("UNKNOWN", "semantic-unknown"),
        "cached": ("CACHED", "semantic-cached"),
        "stale": ("STALE", "semantic-stale"),
    }

    for raw_status, expected in expectations.items():
        status = semantic_status(raw_status)

        assert (status.label, status.css_class) == expected
        assert status.label.isascii()


def test_semantic_aliases_preserve_visible_operator_labels():
    expectations = {
        "NON-ICEBERG": ("NON-ICEBERG", "semantic-critical"),
        "ICEBERG": ("ICEBERG", "semantic-healthy"),
        "fresh": ("FRESH", "semantic-healthy"),
        "setup needed": ("SETUP NEEDED", "semantic-warning"),
        "not analyzed": ("NOT ANALYZED", "semantic-unknown"),
    }

    for raw_status, expected in expectations.items():
        status = semantic_status(raw_status)

        assert (status.label, status.css_class) == expected
        assert status.label.isascii()


def test_layout_state_for_size_prefers_readable_panels_over_fixed_columns():
    wide = layout_state_for_size(width=140, height=40)
    compact = layout_state_for_size(width=96, height=28)
    minimum = layout_state_for_size(width=58, height=15)

    assert wide.workspace_class == "layout-wide"
    assert wide.show_workspace is True
    assert compact.workspace_class == "layout-compact"
    assert compact.compact_chrome is True
    assert minimum.workspace_class == "layout-minimum"
    assert minimum.show_workspace is False
    assert minimum.minimum_message


def test_catalog_browser_applies_responsive_layout_classes_at_startup():
    async def run_app():
        wide_app = OperatorCatalogBrowserApp(setup_needed_message="Setup required.")
        async with wide_app.run_test(size=(140, 40)):
            assert wide_app.query_one("#workspace").has_class("layout-wide")
            assert wide_app.query_one("#minimum-size-message").has_class("hidden")

        compact_app = OperatorCatalogBrowserApp(setup_needed_message="Setup required.")
        async with compact_app.run_test(size=(96, 28)):
            assert compact_app.query_one("#workspace").has_class("layout-compact")
            assert "SETUP" in str(
                compact_app.query_one("#catalog-header", Static).content
            )

        minimum_app = OperatorCatalogBrowserApp(setup_needed_message="Setup required.")
        async with minimum_app.run_test(size=(58, 15)):
            assert minimum_app.query_one("#workspace").has_class("hidden")
            assert "Terminal too small" in str(
                minimum_app.query_one("#minimum-size-message", Static).content
            )

    asyncio.run(run_app())
