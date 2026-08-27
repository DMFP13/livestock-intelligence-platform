from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parseaddr
from typing import Any

from packages.db.sqlite_store import SQLiteStore


PERMISSION_KEYS = [
    "view_portfolio",
    "view_farm_profile",
    "view_animal_profile",
    "view_market_signals",
    "view_disease_signals",
    "view_data_quality",
    "manage_connectors",
    "manage_source_configs",
    "manage_users",
    "export_data",
]


class AuthError(RuntimeError):
    pass


class UnauthorizedError(AuthError):
    pass


class ForbiddenError(AuthError):
    pass


@dataclass(frozen=True)
class AuthPrincipal:
    user_id: str
    email: str | None
    display_name: str | None
    roles: tuple[str, ...]
    permissions: frozenset[str]
    organization_ids: frozenset[str]
    farm_ids: frozenset[str]
    is_dev_mode: bool = False

    def has_role(self, role_key: str) -> bool:
        return role_key in self.roles

    def has_permission(self, permission_key: str) -> bool:
        return permission_key in self.permissions


@dataclass(frozen=True)
class AuthConfig:
    enabled: bool
    dev_mode: bool
    provider: str
    jwt_secret: str | None
    jwt_issuer: str | None
    jwt_audience: str | None
    dev_user_id: str

    @staticmethod
    def from_env() -> "AuthConfig":
        def _truthy(name: str, default: bool) -> bool:
            raw = os.getenv(name)
            if raw is None:
                return default
            return str(raw).strip().lower() in {"1", "true", "yes", "on"}

        return AuthConfig(
            enabled=_truthy("AUTH_ENABLED", True),
            dev_mode=_truthy("AUTH_DEV_MODE", True),
            provider=str(os.getenv("AUTH_PROVIDER", "dev")).strip().lower(),
            jwt_secret=os.getenv("AUTH_JWT_SECRET"),
            jwt_issuer=os.getenv("AUTH_JWT_ISSUER"),
            jwt_audience=os.getenv("AUTH_JWT_AUDIENCE"),
            dev_user_id=str(os.getenv("AUTH_DEV_USER_ID", "DEV-ADMIN")),
        )


class AuthService:
    def __init__(self, store: SQLiteStore, config: AuthConfig | None = None):
        self.store = store
        self.config = config or AuthConfig.from_env()

    def authenticate_headers(self, headers: dict[str, Any]) -> AuthPrincipal:
        if not self.config.enabled:
            if self.config.dev_mode:
                return self.get_dev_principal()
            raise UnauthorizedError("authentication disabled without dev mode; access denied")

        auth_header = str(headers.get("Authorization") or headers.get("authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1].strip()
            claims = self._verify_token(token)
            return self._principal_from_claims(claims)

        if self.config.dev_mode:
            return self.get_dev_principal()
        raise UnauthorizedError("missing bearer token")

    def get_dev_principal(self, user_id: str | None = None) -> AuthPrincipal:
        uid = str(user_id or self.config.dev_user_id)
        principal = self.build_principal_by_user_id(uid, is_dev_mode=True)
        if principal is None:
            raise UnauthorizedError(f"dev user not found: {uid}")
        return principal

    def build_principal_by_user_id(self, user_id: str, *, is_dev_mode: bool = False) -> AuthPrincipal | None:
        user = self.store.fetch_user_by_id(str(user_id))
        if user is None:
            return None
        if int(user.get("is_active") or 0) != 1:
            return None
        roles = tuple(self.store.list_user_roles(str(user_id)))
        perms = self.store.list_role_permissions(list(roles))
        org_ids = self.store.list_user_organization_ids(str(user_id))
        farm_ids = self.store.list_user_farm_ids(str(user_id))
        return AuthPrincipal(
            user_id=str(user["id"]),
            email=user.get("email"),
            display_name=user.get("display_name"),
            roles=roles,
            permissions=frozenset(perms),
            organization_ids=frozenset(org_ids),
            farm_ids=frozenset(farm_ids),
            is_dev_mode=bool(is_dev_mode),
        )

    def resolve_actor_scope(self, principal: AuthPrincipal) -> tuple[set[str], set[str]]:
        if principal.has_role("platform_admin") or principal.has_role("policy_maker"):
            return set(), set()
        return set(principal.organization_ids), set(principal.farm_ids)

    def authorize(
        self,
        principal: AuthPrincipal | None,
        *,
        permission: str,
        organization_id: str | None = None,
        farm_id: str | None = None,
        allow_aggregate_only: bool = False,
    ) -> AuthPrincipal:
        if principal is None:
            raise UnauthorizedError("authentication required")
        if permission not in PERMISSION_KEYS:
            raise ForbiddenError(f"unknown permission: {permission}")
        if not principal.has_permission(permission):
            raise ForbiddenError(f"missing permission: {permission}")

        if principal.has_role("platform_admin") or principal.has_role("policy_maker"):
            return principal

        org_scope, farm_scope = self.resolve_actor_scope(principal)
        if organization_id and org_scope and str(organization_id) not in org_scope:
            raise ForbiddenError("organization scope denied")
        if farm_id and farm_scope and str(farm_id) not in farm_scope:
            raise ForbiddenError("farm scope denied")

        if principal.has_role("processor_analyst") and permission == "view_animal_profile":
            raise ForbiddenError("processor_analyst cannot access animal-level detail")
        if principal.has_role("government_analyst") and permission == "view_animal_profile":
            raise ForbiddenError("government_analyst cannot access animal-level detail")
        if principal.has_role("government_analyst") and not allow_aggregate_only and permission in {
            "view_farm_profile",
            "view_disease_signals",
        }:
            # Government analysts should primarily consume aggregate/regional outputs.
            raise ForbiddenError("government_analyst access restricted to aggregate views")

        return principal

    def can_view_nav(self, principal: AuthPrincipal, nav_key: str) -> bool:
        nav_to_perm = {
            "Portfolio Overview": "view_portfolio",
            "Farm Profile": "view_farm_profile",
            "Animal Profile": "view_animal_profile",
            "Feed & Environment": "view_farm_profile",
            "Market & Finance": "view_market_signals",
            "Data Quality": "view_data_quality",
        }
        perm = nav_to_perm.get(nav_key)
        if perm is None:
            return False
        return principal.has_permission(perm)

    def _principal_from_claims(self, claims: dict[str, Any]) -> AuthPrincipal:
        subject = str(claims.get("sub") or "").strip()
        email = str(claims.get("email") or "").strip()
        user = None
        if subject:
            user = self.store.fetch_user_by_external_subject(subject)
        if user is None and email:
            _, parsed_email = parseaddr(email)
            if parsed_email:
                user = self.store.fetch_user_by_email(parsed_email)
        if user is None:
            raise UnauthorizedError("token subject not mapped to an active platform user")
        principal = self.build_principal_by_user_id(str(user["id"]))
        if principal is None:
            raise UnauthorizedError("inactive or invalid user")
        return principal

    def _verify_token(self, token: str) -> dict[str, Any]:
        provider = self.config.provider
        if provider in {"dev", "none"}:
            if self.config.dev_mode:
                return {"sub": self.config.dev_user_id, "email": "dev-admin@local"}
            raise UnauthorizedError("token auth provider is disabled")
        if provider in {"jwt", "auth0", "supabase"}:
            return self._verify_hs256_jwt(token)
        raise UnauthorizedError(f"unsupported auth provider: {provider}")

    def _verify_hs256_jwt(self, token: str) -> dict[str, Any]:
        if not self.config.jwt_secret:
            raise UnauthorizedError("AUTH_JWT_SECRET is required for jwt provider")
        parts = token.split(".")
        if len(parts) != 3:
            raise UnauthorizedError("invalid jwt format")

        header = self._b64_json(parts[0])
        payload = self._b64_json(parts[1])
        signature = parts[2]

        if str(header.get("alg") or "").upper() != "HS256":
            raise UnauthorizedError("only HS256 is supported in built-in verifier")

        message = f"{parts[0]}.{parts[1]}".encode("utf-8")
        expected = hmac.new(self.config.jwt_secret.encode("utf-8"), message, hashlib.sha256).digest()
        expected_b64 = base64.urlsafe_b64encode(expected).decode("utf-8").rstrip("=")
        if not hmac.compare_digest(signature, expected_b64):
            raise UnauthorizedError("invalid jwt signature")

        now_ts = int(datetime.now(UTC).timestamp())
        if "exp" in payload and int(payload["exp"]) < now_ts:
            raise UnauthorizedError("jwt expired")
        if "nbf" in payload and int(payload["nbf"]) > now_ts:
            raise UnauthorizedError("jwt not active")
        if self.config.jwt_issuer and str(payload.get("iss") or "") != self.config.jwt_issuer:
            raise UnauthorizedError("jwt issuer mismatch")
        if self.config.jwt_audience:
            aud = payload.get("aud")
            if isinstance(aud, list):
                ok = self.config.jwt_audience in [str(v) for v in aud]
            else:
                ok = str(aud or "") == self.config.jwt_audience
            if not ok:
                raise UnauthorizedError("jwt audience mismatch")

        return payload

    @staticmethod
    def _b64_json(value: str) -> dict[str, Any]:
        pad = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode((value + pad).encode("utf-8"))
        parsed = json.loads(raw.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise UnauthorizedError("jwt section is not an object")
        return parsed
