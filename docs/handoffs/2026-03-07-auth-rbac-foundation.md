# Handoff: Auth + RBAC Foundation (2026-03-07)

## Objective Completed
Implemented a production-oriented authentication/authorization foundation with role-based access control and organization/farm scoping, while preserving canonical ingestion and live connector architecture.

## What Was Implemented

### 1. Persistence and schema
Added auth/authz tables in DB migration path:
- `users`
- `roles`
- `permissions`
- `role_permissions`
- `user_roles`
- `user_organizations`
- `user_farms`
- `audit_log`

Files:
- `packages/db/sqlite_store.py`
- `packages/db/migrations/2026-03-07-auth-foundation.sql`

### 2. Role and permission model
Seeded baseline roles and permissions and role-permission mappings.
Seeded dev users for local safety/testing:
- `DEV-ADMIN` (`platform_admin`)
- `DEV-MANAGER` (`dairy_manager`, scoped to `ORG-001` / `FARM-001`)

### 3. Auth abstraction layer
Added centralized auth module:
- `apps/api/auth.py`

Includes:
- auth config flags (enabled/dev/provider)
- `AuthPrincipal`
- JWT verification hook path (HS256 baseline)
- dev auth fallback mode
- reusable authorization checks

### 4. Service/API layer enforcement
Integrated deny-by-default checks in service layer for all relevant read/manage operations.
Implemented per-request auth extraction in API server and proper `401/403` responses.
Added auth helper routes:
- `GET /auth/me`
- `GET /auth/permissions`
- `GET /auth/scope`

Files:
- `apps/api/service.py`
- `apps/api/main.py`

### 5. UI session integration
Added Streamlit auth session bootstrap and role-aware navigation filtering:
- `app/auth_session.py`
- `app/main.py`

Behavior:
- dev-mode user selector in sidebar
- clear auth mode indicator
- navigation only shows pages permitted for current role
- backend authorization remains authoritative

### 6. Auditing hook
Added audit log write path for authorization allow/deny events in service-level checks.

## Tests Added
- `tests/test_authz_foundation.py`
  - role-permission resolution
  - farm scope enforcement
  - unauthorized denial
  - dev auth fallback + nav visibility
  - no-token denial when dev mode is off

## Known Limitations
- Built-in JWT verifier currently supports HS256 secret-based tokens; JWKS integration for external IdPs remains a follow-up hardening step.
- Regional scope abstraction for government users is represented as aggregate-only behavior rather than explicit region entity bindings.
- Existing endpoints are protected at service/API layer, but finer-grained field-level redaction can be expanded later.

## Non-goals Preserved
- No connector redesign.
- No ingestion flow redesign.
- Canonical model and live data foundation behavior preserved.

## Next Recommended Ticket
`authz-hardening-jwks-and-admin-mapping-ui`
- add JWKS-based verifier and key rotation support
- add user/role/org/farm mapping admin workflow
- add explicit regional scope entities/policies
- expand access-denied audit analytics in Data Quality
