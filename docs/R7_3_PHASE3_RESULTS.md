# R7-3 Phase 3 — security verification and corrective measures

Assessment date: 24 November 2026 (UTC). Status: observed security assessment and first remediation cycle recorded; remaining review and maintenance actions tracked separately.

This is an aggregate summary of operator-supplied Phase 3 Markdown/JSON results and subsequent manual verification. It is not an independent penetration test, ISO conformity declaration or GDPR assessment. Retain original reports privately; this document intentionally excludes sensitive operational details and unresolved finding locations.

## Observed before/after evidence

| Measure | Initial (including supplementary OIDC verification) | Post-remediation |
|---|---:|---:|
| Live checks passed | 7/8 | 8/8 |
| Selected HTTP header protections present | 0/6 | 6/6 |
| Configuration checks passed | 7 | 8 |
| Installed Python packages audited | 82 | 83 |
| Packages skipped | 0 | 0 |
| Packages with known vulnerability findings | 3 | 0 |
| Vulnerability records (including repetitions) | 7 | 0 |
| Static analysis high / medium / low severity | 0 / 30 / 20 | 0 / 30 / 20 |
| Static scan errors | 0 | 0 |
| Static source lines scanned | 12,774 | 12,804 |

The 23 automated observations move from 14 PASS, 7 REVIEW, 2 OBSERVED (including supplementary discovery) to 17 PASS, 4 REVIEW, 2 OBSERVED. These are observation classifications, not a security score. The repeated audit establishes absence of reported known dependency findings at that date, not absence of unknown vulnerabilities. Header checks establish presence and selected values, not a comprehensive CSP review.

Initial reporting checkout: 6a278ea49405ac9f5e051358d008e789e6c50d50. Initial reports 09:10–09:31 UTC; supplementary OIDC 09:42:07 UTC. Repeated live/config/privacy reporting checkout: a16bacf556ffc35fa9b00bd9043d2ac604307e41 (10:28:58–10:29:51 UTC); dependency/source audit reporting checkout: 9f57873c5d3b7d567db78bfcdbf5d826ac349909 (10:38:33 UTC). Reporting checkout is not proof of deployed image identity. The repeated static report recorded exit 0 despite nonzero findings; the REVIEW classification is retained and scanner invocation/exit capture should be reconciled privately.

## Operator-confirmed manual observations

On 24 November 2026 the operator confirmed all seven of the following production checks as successful. This is self-reported acceptance evidence, not independently executed testing; exact individual test times and browser/version were not supplied.

| ID | Observed check | Result |
|---|---|---|
| P3M01 | Normal Keycloak login | PASS |
| P3M02 | Authorized user's personal dashboard and samples | PASS |
| P3M03 | Logged-in cookie Secure, HttpOnly, SameSite=Lax | PASS |
| P3M04 | Normal logout | PASS |
| P3M05 | Subsequent access to /my requires authentication | PASS |
| P3M06 | Authenticated canonical ZIP download | PASS |
| P3M07 | Authorized Jupyter research workspace access | PASS |

No cookie values, identity records or screenshots are included. These observations complement, but do not replace, expiry/replay tests, two-account role separation, infrastructure and privacy review. They do not imply that all seven differently scoped MAN01–MAN07 items in the original review register are completed.

## Privacy and residual review

The repeated local export screening inspected 11,329 sample rows and 28,683 image rows, with no email-like collector values detected. The SHA-256 matches the earlier assessed snapshot. Persistent identifiers may still be personal data; provenance, publication authorization, images, free text and coordinate privacy require separate owner review.

Static findings were reviewed at the audited revision by rule and context: 28 SQL construction observations (22 in API/web/export code, six in legacy database helpers), three non-secret password-like literals, 13 exception fallbacks, two skipped exceptions, two assertions and two temporary-path observations. Parameterized values and application-controlled SQL fragments support bounded contextual dispositions; no complete injection or authorization audit was performed. Targeted maintenance addresses identifier quoting and runtime validation; other observations remain for risk-based review. Do not suppress findings globally.

The operator reported a subsequent credential maintenance operation and service recovery. The final redacted configuration measurement has not yet been supplied; do not mark that observation PASS until evidence is received. Keep deployment-specific details, raw scans and remaining security findings private. Maintenance changes are source proposals until tested and deployed.

Next: Phase 4 interoperability, Phase 5 actual user feedback, then consolidated validation. Security follow-up remains open where evidence is incomplete.
