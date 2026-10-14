# Security Policy

## Supported versions

| Version | Status |
|---------|--------|
| `main` branch | ✅ Security fixes accepted |
| Tagged releases | ✅ Security fixes accepted |

The project has no formal release yet; security fixes land on `main` and are recorded in the
[CHANGELOG](CHANGELOG.md).

## Reporting a vulnerability

**Please do not report security vulnerabilities through public issues or discussions.**

Report privately through one of these channels:

- GitHub **Security → Report a vulnerability** (private security advisory), or
- direct contact with a maintainer (see the repository front page).

Please include:

| Item | Description |
|------|-------------|
| Vulnerability class | Authorization bypass, injection, auth bypass, data leak, DoS, ... |
| Affected component | e.g. `internal/api`, `pkg/cache` |
| Reproduction | Full request or minimal proof-of-concept |
| Impact | Affected versions and required configuration |
| Severity | Your assessment (critical / high / medium / low) |

## Response timeline

| Stage | Target |
|-------|--------|
| Acknowledge receipt | within 72 hours |
| Triage and remediation plan | within 7 days |
| Fix released | 14 days for critical issues |

We will credit you in the release notes if you wish, and can coordinate disclosure timing.

## Security notes for operators

Mneme is **internal memory infrastructure**. The defaults target local development; harden them
before production use.

1. **Admin console authentication**
   - Always set a non-empty `admin.token`; requests must then carry
     `Authorization: Bearer <token>`.
   - When `token` is empty, admin endpoints have **no authentication at all** and anyone can delete
     data.
   - `/admin` is enabled by default in the sample config — keep it on an internal network or behind
     gateway authentication.

2. **Data plane authentication**
   - `/api/v1/*` performs **no authentication**; tenants are only identified by the `X-Tenant-ID`
     header.
   - Put authentication, authorization, and rate limiting in a gateway or reverse proxy. Never
     expose the service directly to the public internet.

3. **Transport and storage**
   - ClickHouse, Redis, and Pulsar all start without TLS in the sample compose file. Enable TLS and
     use least-privilege accounts for cross-host deployments.
   - Inject `token`, `password`, `access_key`, and `secret_key` through environment variables or a
     secret manager. **Never commit them.**

4. **Data protection**
   - Mneme stores raw conversation content and derived profiles, which is sensitive data. Set
     appropriate `cache.*_ttl` values and ClickHouse TTLs to honour retention and deletion
     obligations.
   - Hard deletes (`?hard=true`) are irreversible — confirm your audit and backup strategy first.

5. **Default configuration caveats**
   - Redis runs without a password and ClickHouse uses an empty-password `default` user. Both are
     development-only settings.
