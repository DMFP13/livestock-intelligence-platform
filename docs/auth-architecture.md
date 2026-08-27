# Authentication and Authorization Architecture

## Goals
- External-IdP-friendly authentication foundation (JWT-oriented abstraction).
- Centralized authorization at service/API layer.
- Multi-tenant scope enforcement using organizations and farms.
- Safe local development via explicit dev auth mode.

## Core Components
- `apps/api/auth.py`
  - `AuthConfig`: env-driven auth runtime config
  - `AuthService`: token/dev authentication + RBAC/scope authorization
  - `AuthPrincipal`: resolved user identity, roles, permissions, scopes
  - Exceptions: `UnauthorizedError`, `ForbiddenError`
- `apps/api/service.py`
  - centralized permission checks (`_require(...)`)
  - scoped data access across farm/org-aware methods
  - auth helper endpoint payload via `current_user(...)`
- `apps/api/main.py`
  - per-request auth extraction from headers
  - `401/403` handling
  - `/auth/me`, `/auth/permissions`, `/auth/scope`
- `app/auth_session.py`
  - Streamlit session auth bootstrap
  - role-aware navigation visibility
  - dev-mode indicator

## Data Model Additions
Added tables:
- `users`
- `roles`
- `permissions`
- `role_permissions`
- `user_roles`
- `user_organizations`
- `user_farms`
- `audit_log`

Migration reference:
- `packages/db/migrations/2026-03-07-auth-foundation.sql`

## Baseline Roles
- `platform_admin`
- `org_admin`
- `dairy_manager`
- `veterinarian`
- `processor_analyst`
- `government_analyst`
- `research_analyst`
- `viewer`

## Baseline Permissions
- `view_portfolio`
- `view_farm_profile`
- `view_animal_profile`
- `view_market_signals`
- `view_disease_signals`
- `view_data_quality`
- `manage_connectors`
- `manage_source_configs`
- `manage_users`
- `export_data`

## Scope Rules
Authorization combines permission + tenant scope:
- platform admin: unrestricted
- scoped roles: restricted by `user_organizations` and/or `user_farms`
- deny-by-default when permission missing
- deny/limit animal-level for roles that require aggregate-only behavior

## Authentication Modes
Config flags (env):
- `AUTH_ENABLED` (default `true`)
- `AUTH_DEV_MODE` (default `true`)
- `AUTH_PROVIDER` (`dev`, `jwt`, `auth0`, `supabase`)
- `AUTH_JWT_SECRET` (required for built-in HS256 JWT verification)
- `AUTH_JWT_ISSUER` (optional)
- `AUTH_JWT_AUDIENCE` (optional)
- `AUTH_DEV_USER_ID` (default `DEV-ADMIN`)

Behavior:
- if auth enabled + bearer token: token verification path
- if auth enabled + no token + dev mode: dev principal fallback
- if auth disabled and dev mode false: deny by default

## Dev Auth Mode
Seeded dev users:
- `DEV-ADMIN` (`platform_admin`)
- `DEV-MANAGER` (`dairy_manager`, scoped to `ORG-001` / `FARM-001`)

Streamlit displays a clear dev auth indicator and user selector in sidebar.

## Production Integration Path
Current implementation supports JWT verification hooks and principal mapping. For production IdP integration:
1. Set provider to external JWT-compatible mode (`auth0` / `supabase` / `jwt`).
2. Configure issuer/audience and secret or swap verifier for JWKS-based verification.
3. Map external subject/email to platform `users` rows.
4. Assign role + org/farm scope via mapping tables.
5. Keep service-layer authorization as the enforcement point.
