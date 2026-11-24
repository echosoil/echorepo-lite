# R7-3 Phase 5 — initial technical and manual usability observations

Operator-supplied reports: usability.json and usability.md, generated 24 November 2026 at 13:59:21 UTC. Reporting checkout 6d9e8ae90346f86b3fb40064482628d9bf442da9; Chromium 159.0.8154.6, axe-core 4.10.3. The reporting commit is not proof of the deployed image revision. Retain original reports in controlled evidence storage.

| Page | Viewport | Locale | Status | Violation rules/nodes | Incomplete rules | Page overflow px |
|---|---|---|---|---|---:|---:|
| /explore | 1366x900 | en | REVIEW | 3/8 | 3 | 0 |
| /explore | 390x844 | en | REVIEW | 2/3 | 3 | 76 |
| /search | 1366x900 | en | REVIEW | 1/5 | 1 | 0 |
| /search | 390x844 | en | REVIEW | 0/0 | 1 | 0 |
| /login | 1366x900 | en | REVIEW | 1/5 | 1 | 0 |
| /login | 390x844 | en | REVIEW | 0/0 | 1 | 0 |
| /search | 1366x900 | es | REVIEW | 1/5 | 1 | 0 |

Seven of seven return HTTP 200, expected document language, title and one main landmark. Both map views initialize; all three mobile navigation toggles open; zero page errors and critical HTTP errors reported. Five of seven skip-link observations pass, both login cases do not. Three search cases have no H1. Across configurations there are 26 violation-node observations (20 color-contrast, 4 label, 2 select-name), not 26 distinct defects. Incomplete observations are not confirmed violations. All seven scenarios remain REVIEW, including two with zero violations, because other observations remain. The tool blocked 32 critical requests; the report does not identify the resources, and blocked requests cannot be attributed to the application. A repeat scan with improved diagnostic categories is needed.

## Manual observations

Operator confirmed on 24 November 2026: UX-M01 keyboard navigation PASS; UX-M02 keyboard-only search PASS; UX-M03 mobile usability PASS; UX-M04 zoom PASS; UX-M05 localization PASS; UX-M06 screen reader NOT_ASSESSED. Browser/device and per-check timestamps not supplied. The manual pass judgments are bounded task observations; they do not negate the automated overflow or login skip-link observations. No WCAG conformance claim.

## Follow-up

Source review identifies map control names, a nonwrapping header, login autofocus, search heading level and potential shared navigation contrast issues. Proposed template changes and narrowly scoped request diagnostics are not yet deployment-verified. Check the exact affected nodes on rerun and review remaining color contrast, ARIA and link differentiation. Do not globally suppress axe rules. Preserve before/after evidence and document the changed request policy. No full screen reader, expanded mobile menu accessibility or cross-browser assessment.

R73-I02 remains PARTIAL. The user feedback register remains PARTIAL; survey published per operator, results not yet available. No satisfaction statistic is inferred from browser tests.

## Diagnostic baseline and post-remediation verification

Operator-supplied diagnostic reports (14:16:30 UTC) and after-remediation reports (14:51:01 UTC), 24 November 2026, both reporting checkout `deaa132ec0d0c74397654022f99bdd61d8b75a4a`, Chromium 159.0.8154.6, axe-core 4.10.3, identical axe script SHA-256 `880970c081707360e64f34cea25ff91892f5bc95675b0776925b9709dd8a68bb`. Preserve all three original report pairs separately. The diagnostic baseline isolates the changed request policy before application deployment; the reporting revision is not proof of deployed image identity.

| Indicator | Initial | Diagnostic | After |
|---|---:|---:|---:|
| Completed public page cases | 7/7 | 7/7 | 7/7 |
| Color contrast violation-node observations | 20 | 20 | 0 |
| Label violation-node observations | 4 | 4 | 0 |
| Select-name violation-node observations | 2 | 2 | 0 |
| Total violation-node observations | 26 | 26 | 0 |
| Incomplete node observations | 103 | 103 | 102 |
| Mobile map page overflow (px) | 76 | 76 | 0 |
| Skip-link success | 5/7 | 5/7 | 7/7 |
| Primary H1 present | 4/7 | 4/7 | 7/7 |
| Blocked critical requests | 32 | 0 | 0 |
| Page errors | 0 | 0 | 0 |
| Overall REVIEW cases | 7/7 | 7/7 | 7/7 |

Both diagnostic and after reports record 224 total requests, zero blocked requests or critical failures. Both map cases initialize and all three mobile navigation toggles open. All seven cases now show zero detected violations and zero page-wide overflow. The targeted remediation is observed within these states. The remaining incomplete observations (100 contrast, two link-in-text-block) need human review; the earlier two incomplete aria-prohibited-attr observations are absent. Node observations may repeat across states. REVIEW is retained; no WCAG conformance or screen-reader coverage claimed. Manual results above remain operator-reported; not a new post-remediation manual test. No further production release is needed solely to change the status label.

Historical co-design evidence is summarized in R7_3_CODESIGN_TRACEABILITY.md. Satisfaction responses and external clarifications remain pending. R73-I02 and I04 remain PARTIAL with substantially strengthened evidence.
