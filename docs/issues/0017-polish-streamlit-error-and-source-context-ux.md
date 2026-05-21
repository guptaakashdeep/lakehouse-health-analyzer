# Polish Streamlit error and source-context UX

Labels: `ready-for-agent`, `UX`

## Parent

#1

## What to build

Polish Streamlit operator UX by improving how failures and data-source context are presented. Keep calculation behavior unchanged and focus on clearer interaction guidance.

## Acceptance criteria

- [ ] Metadata-file and catalog-table result views clearly show source context (mode and selected source identity).
- [ ] User-facing failure states avoid dumping raw traceback text by default; actionable error summaries are shown first.
- [ ] Optional detailed diagnostics are still available when needed (for debugging), but are visually secondary.
- [ ] Empty/invalid-source states include concise next-step guidance for operators.
- [ ] Tests cover source-context rendering and error-state behavior through existing Streamlit public surfaces.

## Blocked by

- #15
