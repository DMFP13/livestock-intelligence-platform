from __future__ import annotations

import streamlit as st

from apps.api.auth import AuthPrincipal, ForbiddenError, UnauthorizedError
from apps.api.service import PlatformService


def _permission_for_page(page: str) -> str:
    mapping = {
        "Portfolio Overview": "view_portfolio",
        "Farm Profile": "view_farm_profile",
        "Animal Profile": "view_animal_profile",
        "Feed & Environment": "view_farm_profile",
        "Market & Finance": "view_market_signals",
        "Data Quality": "view_data_quality",
    }
    return mapping.get(page, "")


def init_streamlit_auth_session(service: PlatformService) -> AuthPrincipal:
    if "auth_user_id" not in st.session_state:
        st.session_state["auth_user_id"] = service.auth.config.dev_user_id

    principal = None
    auth_error = None
    user_id = str(st.session_state.get("auth_user_id") or service.auth.config.dev_user_id)

    if service.auth.config.dev_mode:
        seeded_dev_users = [
            "DEV-ADMIN",
            "DEV-MANAGER",
            "DEV-OWNER",
            "DEV-POLICY",
        ]
        options = []
        for uid in seeded_dev_users:
            if service.store.fetch_user_by_id(uid):
                options.append(uid)
        if not options:
            options = ["DEV-ADMIN"]
        labels = {
            "DEV-ADMIN": "Admin",
            "DEV-MANAGER": "Manager",
            "DEV-OWNER": "Owner",
            "DEV-POLICY": "Policy",
        }
        with st.popover(f"Signed in as {labels.get(user_id, user_id)}", use_container_width=False):
            st.caption("Development access")
            selected = st.selectbox(
                "View as",
                options=options,
                format_func=lambda x: labels.get(str(x), str(x)),
                index=options.index(user_id) if user_id in options else 0,
                key="dev_auth_user_selector",
            )
        st.session_state["auth_user_id"] = selected
        user_id = selected
        try:
            principal = service.auth.get_dev_principal(user_id)
        except Exception as exc:  # noqa: BLE001
            auth_error = str(exc)
    else:
        token = st.text_input("Bearer token", value="", type="password", key="auth_bearer_token")
        if token:
            try:
                principal = service.auth._principal_from_claims(service.auth._verify_token(token))
            except Exception as exc:  # noqa: BLE001
                auth_error = str(exc)
        else:
            auth_error = "Authentication required. Provide a bearer token."

    if principal is None:
        st.error(auth_error or "Authentication failed")
        raise UnauthorizedError(auth_error or "authentication failed")

    service.set_current_principal(principal)
    return principal


def visible_pages_for_principal(service: PlatformService, principal: AuthPrincipal) -> list[str]:
    pages = [
        "Portfolio Overview",
        "Farm Profile",
        "Animal Profile",
        "Feed & Environment",
        "Market & Finance",
        "Data Quality",
    ]
    out: list[str] = []
    for p in pages:
        perm = _permission_for_page(p)
        if not perm:
            continue
        try:
            service.auth.authorize(principal, permission=perm, allow_aggregate_only=True)
            out.append(p)
        except (UnauthorizedError, ForbiddenError):
            continue
    return out
