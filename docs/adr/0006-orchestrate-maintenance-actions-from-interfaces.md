# Orchestrate Maintenance Actions from Interfaces

Streamlit and TUI interfaces may recommend, schedule, trigger, and monitor maintenance actions, but they should not run heavy table mutations such as compaction inside the UI process. Maintenance operations should go through an execution backend with preview, explicit confirmation, permissions, run identifiers, status tracking, and auditability.
