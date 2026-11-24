# Application runtime hardening — rollout and regression checks

This change updates selected Python package pins, checks build consistency, adds baseline browser response headers, restricts development diagnostics to development-like environments, and improves authentication-state cleanup. It does not alter data schemas, external IAM configuration or the database password. It is a staged source change, not confirmation of a deployed fix.

## Before release

- Review the application image and offline regression workflows. The setuptools upgrade removes the legacy pkg_resources module; verify imports and representative application paths before deploying. A build or import failure is a release blocker, not a reason to silently retain an obsolete package.
- Rebuild and recreate the **development** application and worker using the existing Compose project and development environment. Do not run a production release script merely to test this branch. Confirm disk headroom and keep the previous image/configuration for rollback.
- Run normal login/logout, authorized API access, public map and search, image rendering, export and optional research-service smoke checks. Verify same-origin frames work, and review any intentional external framing.
- Browser header policy is intentionally limited (object-src, base-uri and frame-ancestors); it does not restrict script/style origins or claim full XSS protection. Review resource sources before stronger CSP enforcement. No reverse-proxy configuration is changed.
- Production Compose disables Flask debug and development diagnostic routes. Development remains configurable. HSTS uses a short one-day max-age in the production override (no includeSubDomains or preload); configure only for an HTTPS deployment with working redirects. Set ECHOREPO_HSTS_MAX_AGE=0 to disable emission before deployment; previously cached HSTS remains until its expiry or an HTTPS max-age=0 response.

## After normal deployment

Record actual deployed package versions from the application container, run pip check, repeat the previously approved Phase 3 observations and audit a new installed inventory. Keep original and repeat evidence separately. Review vulnerability advisories and applicability with the technical owner. Do not publish live configuration details, credentials, raw scanner output or unfinished remediation details. Rotate deployment credentials separately using an approved maintenance window, backups and coordinated service configuration; editing an environment variable alone does not change an existing database role password.

See R7_3_PHASE3.md for the evidence procedure. Remediation is only considered verified after appropriate deployed checks and audit results.
