#!/usr/bin/env bash
# Migrates gobble ConfigMap names to the new convention:
#   gobble-config-<slug>  →  gobble-<slug>-config
#
# Safe to run before `helm upgrade`: copies each old CM to the new name so
# rolling pods can find it, then deletes the old CM.
#
# Usage:
#   ./scripts/migrate-gobble-configmaps.sh [-n <namespace>]

set -euo pipefail

NAMESPACE="default"
while getopts "n:" opt; do
  case $opt in
    n) NAMESPACE="$OPTARG" ;;
    *) echo "Usage: $0 [-n namespace]" >&2; exit 1 ;;
  esac
done

if command -v kubectl &>/dev/null; then
  KUBECTL=(kubectl)
elif command -v microk8s &>/dev/null; then
  KUBECTL=(microk8s kubectl)
else
  echo "ERROR: neither kubectl nor microk8s found in PATH" >&2; exit 1
fi
JQ=$(command -v jq) || { echo "ERROR: jq not found in PATH" >&2; exit 1; }

echo "=== Migrating gobble ConfigMaps in namespace: $NAMESPACE ==="

mapfile -t OLD_CMS < <(
  "${KUBECTL[@]}" get configmap -n "$NAMESPACE" --no-headers \
    -o custom-columns=':metadata.name' \
  | grep '^gobble-config-'
)

if [[ ${#OLD_CMS[@]} -eq 0 ]]; then
  echo "No old-style gobble ConfigMaps found — nothing to do."
  exit 0
fi

for old in "${OLD_CMS[@]}"; do
  slug="${old#gobble-config-}"
  new="gobble-${slug}-config"
  echo "  $old → $new"
  "${KUBECTL[@]}" get configmap "$old" -n "$NAMESPACE" -o json \
    | "$JQ" --arg name "$new" '
        .metadata.name = $name
        | del(
            .metadata.resourceVersion,
            .metadata.uid,
            .metadata.creationTimestamp,
            .metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]
          )' \
    | "${KUBECTL[@]}" apply -n "$NAMESPACE" -f -
  "${KUBECTL[@]}" delete configmap "$old" -n "$NAMESPACE"
done

echo ""
echo "=== Done. Run 'helm upgrade <release> ./chart' to reconcile. ==="
