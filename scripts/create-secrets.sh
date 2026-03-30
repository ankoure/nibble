#!/usr/bin/env bash
# Creates all Kubernetes secrets required by the transit-agencies Helm chart.
# Safe to re-run — uses apply semantics so existing secrets are updated in place.
#
# Usage:
#   NAMESPACE=transit-agencies ./scripts/create-secrets.sh
#
# Reads credentials from .env if present, otherwise expects them in the environment.

set -euo pipefail

# ── kubectl shim (microk8s compat) ─────────────────────────────────────────
if ! command -v kubectl &>/dev/null && command -v microk8s &>/dev/null; then
  kubectl() { microk8s kubectl "$@"; }
  export -f kubectl
fi

# ── Load .env ──────────────────────────────────────────────────────────────
if [ -f .env ]; then
  # Parse key=value lines only — avoids sourcing URLs with special shell chars
  while IFS='=' read -r key value; do
    [[ "$key" =~ ^[[:space:]]*# ]] && continue  # skip comments
    [[ -z "$key" ]] && continue                  # skip blank lines
    key="${key// /}"                              # trim spaces
    export "$key"="$value"
  done < <(grep -v '^\s*#' .env | grep '=')
fi

NAMESPACE=${NAMESPACE:-transit-agencies}

# ── Helpers ────────────────────────────────────────────────────────────────

# Ensure the namespace exists before trying to create secrets in it
kubectl get namespace "$NAMESPACE" &>/dev/null \
  || kubectl create namespace "$NAMESPACE"

# Creates or updates a secret idempotently
apply_secret() {
  kubectl create secret generic "$@" \
    --dry-run=client -o yaml \
    | kubectl apply -n "$NAMESPACE" -f -
}

# ── aws-credentials ────────────────────────────────────────────────────────
# Required by nibble, gobble, and s3-upload containers in every Pod.

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be set (check your .env)}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be set (check your .env)}"

apply_secret aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

echo "✓ aws-credentials"

# ── Per-agency auth secrets ────────────────────────────────────────────────
# One entry per agency in chart/values.yaml that has auth configured.
# Secret name must match the nibble-<slug>-secrets pattern in deployment.yaml.

create_auth_secret() {
  local slug="$1"
  local value="$2"
  apply_secret "nibble-${slug}-secrets" --from-literal=AUTH_SECRET="$value"
  if [ -n "$value" ]; then
    echo "✓ nibble-${slug}-secrets"
  else
    echo "✓ nibble-${slug}-secrets (empty — set the env var to populate)"
  fi
}

# Massachusetts
create_auth_secret pvta "${PVTA_GTFS_RT_API_KEY:-}"

# California — 511 SF Bay (shared key: ACE, Caltrain, Capitol Corridor, SMART)
create_auth_secret ace              "${ACE_AUTH_SECRET:-}"
create_auth_secret caltrain         "${CALTRAIN_AUTH_SECRET:-}"
create_auth_secret capitol-corridor "${CAPITOL_CORRIDOR_AUTH_SECRET:-}"
create_auth_secret smart            "${SMART_AUTH_SECRET:-}"


# Illinois
create_auth_secret metra            "${METRA_AUTH_SECRET:-}"

# Washington — OBA Puget Sound (all agencies share OBA_PUGET_SOUND_API_KEY)
OBA_KEY="${OBA_PUGET_SOUND_API_KEY:-}"
for slug in king-county-metro seattle-streetcar pierce-transit community-transit \
            intercity-transit wsf amtrak-wa sound-transit seattle-monorail \
            everett-transit kitsap-transit sounder; do
  create_auth_secret "$slug" "$OBA_KEY"
done

# Oregon
create_auth_secret trimet           "${TRIMET_AUTH_SECRET:-}"

echo ""
echo "Done. Secrets created in namespace: $NAMESPACE"
