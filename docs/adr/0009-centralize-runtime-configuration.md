# Centralize Runtime Configuration

Table sources, catalog settings, analysis policies, runtime policies, and output options should be normalized through one configuration model that can be populated from CLI flags, environment variables, config files, and UI overrides. This keeps Streamlit, TUI, and tests from each inventing separate configuration rules for Glue, metadata-file analysis, recommendation thresholds, cache TTL, concurrency, and exports.

The setup command should persist user defaults to
`${XDG_CONFIG_HOME:-~/.config}/lakehouse-health-analyzer/config.toml`.
Configuration precedence should be: built-in defaults, then setup config file,
then environment variables, then CLI flags or UI overrides. Environment keys
currently supported are `LHA_METADATA_LOCATION`,
`LHA_SNAPSHOT_RETENTION_DAYS`, `LHA_RECOMMENDATION_THRESHOLDS`,
`LHA_HISTORY_DEPTH`, `LHA_CACHE_TTL_SECONDS`, `LHA_TIMEOUT_SECONDS`, and
`LHA_MAX_CONCURRENCY`. Streamlit metadata-file inputs are treated as UI
overrides.

For local AWS authentication setup, named profiles from `~/.aws/config` should
be offered when present. If no named profiles are configured, setup should save
no AWS profile and explain that analysis will use the default AWS credential
chain. Setup should optionally validate the selected profile and catalog by
attempting a lightweight namespace listing. Validation success should be shown
to the user, but validation failure should not block saving the configuration;
instead setup should save with a clear warning.

The first setup implementation should be a plain Rich-powered terminal prompt
flow behind `lakehouse-health-operator setup`, not a Textual form. The TUI may
later invoke or link to the same setup flow from its settings action, but setup
must work before the interactive app has valid catalog configuration. Setup
should write the supported configuration defaults into the config file so users
can see the available knobs without searching through docs or environment
variables. When a user changes settings through supported setup or TUI flows,
the same config file should be updated. TOML should be used instead of YAML
because the configuration is mostly typed scalar values, lists, and simple
tables, and TOML avoids YAML's surprising parsing behavior without adding
another dependency. The generated config file should include short comments that
explain each setting.
