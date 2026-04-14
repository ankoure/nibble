#!/usr/bin/env bash
# Generates routes.json for each gobble agency and creates Kubernetes ConfigMaps.
#
# For each unique gobble agency, runs generate_agency_routes.py in the gobble repo,
# then applies a ConfigMap named gobble-<agency>-routes.
#
# Usage:
#   ./scripts/generate-gobble-routes.sh -g <gobble-repo-path> [-n <namespace>]

set -euo pipefail

NAMESPACE="default"
GOBBLE_DIR=""

while getopts "g:n:" opt; do
  case $opt in
    g) GOBBLE_DIR="$OPTARG" ;;
    n) NAMESPACE="$OPTARG" ;;
    *) echo "Usage: $0 -g <gobble-repo-path> [-n <namespace>]" >&2; exit 1 ;;
  esac
done

if [[ -z "$GOBBLE_DIR" ]]; then
  echo "ERROR: -g <gobble-repo-path> is required" >&2
  exit 1
fi

if [[ ! -f "$GOBBLE_DIR/scripts/generate_agency_routes.py" ]]; then
  echo "ERROR: generate_agency_routes.py not found in $GOBBLE_DIR/scripts/" >&2
  exit 1
fi

if command -v kubectl &>/dev/null; then
  KUBECTL=(kubectl)
elif command -v microk8s &>/dev/null; then
  KUBECTL=(microk8s kubectl)
else
  echo "ERROR: neither kubectl nor microk8s found in PATH" >&2; exit 1
fi

# Each entry: "<agency_name> <gtfs_static_url>"
# Agencies that share a gobble agency (e.g. all nyct-* → nyc_subway) appear once.
declare -a AGENCIES=(
  # ── Massachusetts ────────────────────────────────────────────────────────────
  "mwrta        http://vc.mwrta.com/gtfs/google_transit.zip"
  "brta         http://data.trilliumtransit.com/gtfs/berkshire-ma-us/berkshire-ma-us.zip"
  "ccrta        https://data.trilliumtransit.com/gtfs/capecod-ma-us/capecod-ma-us.zip"
  "frta         https://data.trilliumtransit.com/gtfs/frta-ma-us/frta-ma-us.zip"
  "gatra        http://data.trilliumtransit.com/gtfs/gatra-ma-us/gatra-ma-us.zip"
  "lrta         http://data.trilliumtransit.com/gtfs/lowell-ma-us/lowell-ma-us.zip"
  "mart         http://data.trilliumtransit.com/gtfs/montachusett-ma-us/montachusett-ma-us.zip"
  "meva         http://data.trilliumtransit.com/gtfs/merrimackvalley-ma-us/merrimackvalley-ma-us.zip"
  "pvta         https://www.pvta.com/g_trans/google_transit.zip  header:pvta"
  "vta          http://data.trilliumtransit.com/gtfs/marthasvineyard-ma-us/marthasvineyard-ma-us.zip"
  "wrta         http://data.trilliumtransit.com/gtfs/wrta-ma-us/wrta-ma-us.zip"
  # ── Maine ────────────────────────────────────────────────────────────────────
  "gptd         https://gtfs.gptd.cadavl.com/GPTD/GTFS/GTFS_GPTD.zip"
  # ── NYC (subway slugs nyct-* all share nyc_subway) ───────────────────────────
  "nyc_subway   https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
  "lirr         http://web.mta.info/developers/data/lirr/google_transit.zip"
  "mnr          http://web.mta.info/developers/data/mnr/google_transit.zip"
  # ── Connecticut ──────────────────────────────────────────────────────────────
  "ctdot        https://www.cttransit.com/sites/default/files/gtfs/googlect_transit.zip"
  # ── Utah ─────────────────────────────────────────────────────────────────────
  "uta          https://gtfsfeed.rideuta.com/GTFS.zip"
  # ── Colorado ─────────────────────────────────────────────────────────────────
  "denver_rtd   https://www.rtd-denver.com/files/gtfs/google_transit.zip"
  # ── Illinois ─────────────────────────────────────────────────────────────────
  "metra        https://schedules.metrarail.com/gtfs/schedule.zip"
  # ── Maryland ─────────────────────────────────────────────────────────────────
  "marc         https://mdotmta-gtfs.s3.amazonaws.com/mdotmta_gtfs_marc.zip"
  # ── Pennsylvania ─────────────────────────────────────────────────────────────
  "septa_regionalrail https://www3.septa.org/developer/gtfs_public.zip inner:google_rail.zip"
  "septa_bus          https://www3.septa.org/developer/gtfs_public.zip inner:google_bus.zip"
  # ── Virginia ─────────────────────────────────────────────────────────────────
  "vre          https://gtfs.vre.org/containercdngtfsupload/google_transit.zip"
  # ── Tennessee ────────────────────────────────────────────────────────────────
  "wegostar     https://www.wegotransit.com/googleexport/google_transit.zip"
  # ── Oregon ───────────────────────────────────────────────────────────────────
  "trimet       http://developer.trimet.org/schedule/gtfs.zip"
  # ── Washington (sound-transit and sounder both use soundtransit) ─────────────
  "community_transit https://www.soundtransit.org/GTFS-CT/current.zip"
  "soundtransit https://www.soundtransit.org/GTFS-rail/40_gtfs.zip"
  "everett_transit https://gtfs.sound.obaweb.org/prod/97_gtfs.zip"
  "kitsap_transit https://gtfs.sound.obaweb.org/prod/20_gtfs.zip"
)

echo "=== Generating gobble routes and creating ConfigMaps in namespace: $NAMESPACE ==="
echo ""

pushd "$GOBBLE_DIR" > /dev/null

declare -a FAILED=()

for entry in "${AGENCIES[@]}"; do
  agency=$(echo "$entry" | awk '{print $1}')
  gtfs_url=$(echo "$entry" | awk '{print $2}')
  # Tokens 3+ are optional and identified by prefix:
  #   header:<slug> | query_param:<slug>:<param>  → auth
  #   inner:<filename>                            → nested zip inside the download
  auth_spec=""
  inner_zip=""
  for tok in $(echo "$entry" | awk '{for (i=3; i<=NF; i++) print $i}'); do
    case "$tok" in
      inner:*) inner_zip="${tok#inner:}" ;;
      header:*|query_param:*) auth_spec="$tok" ;;
    esac
  done
  routes_file="config/${agency}_routes.json"
  cm_name="gobble-${agency//_/-}-routes"

  echo "── $agency"

  # Always pre-download with curl: more reliable than Python's urllib for
  # following redirects and avoids servers that block the Python User-Agent.
  # When auth is required, fetch the secret and include credentials.
  local_gtfs=$(mktemp --suffix=.zip)
  declare -a CURL_ARGS=(-fsSL "$gtfs_url" -o "$local_gtfs")

  if [[ -n "$auth_spec" ]]; then
    auth_type=$(echo "$auth_spec" | cut -d: -f1)
    slug=$(echo "$auth_spec" | cut -d: -f2)
    param_name=$(echo "$auth_spec" | cut -d: -f3)   # only used by query_param
    secret_name="nibble-${slug}-secrets"

    echo "  Fetching auth secret from $secret_name..."
    secret_val=$("${KUBECTL[@]}" get secret "$secret_name" -n "$NAMESPACE" \
      -o jsonpath='{.data.AUTH_SECRET}' | base64 -d)

    if [[ "$auth_type" == "header" ]]; then
      CURL_ARGS+=(-H "Authorization: $secret_val")
    elif [[ "$auth_type" == "query_param" ]]; then
      CURL_ARGS[1]="${gtfs_url}?${param_name}=${secret_val}"
    else
      echo "  SKIPPED: unknown auth_type '$auth_type' (expected header or query_param)" >&2
      FAILED+=("$agency")
      rm -f "$local_gtfs"
      echo ""
      continue
    fi
  fi

  echo "  Downloading $gtfs_url..."
  if ! curl "${CURL_ARGS[@]}"; then
    echo "  SKIPPED: download failed for $agency" >&2
    FAILED+=("$agency")
    rm -f "$local_gtfs"
    echo ""
    continue
  fi

  if [[ -n "$inner_zip" ]]; then
    echo "  Extracting nested $inner_zip..."
    inner_dir=$(mktemp -d)
    if ! unzip -j -o "$local_gtfs" "$inner_zip" -d "$inner_dir" > /dev/null; then
      echo "  SKIPPED: failed to extract $inner_zip from $local_gtfs" >&2
      FAILED+=("$agency")
      rm -rf "$local_gtfs" "$inner_dir"
      echo ""
      continue
    fi
    rm -f "$local_gtfs"
    local_gtfs="$inner_dir/$(basename "$inner_zip")"
  fi

  if ! uv run python scripts/generate_agency_routes.py "$local_gtfs" "$agency" --format json; then
    echo "  SKIPPED: generate step failed for $agency" >&2
    FAILED+=("$agency")
    rm -f "$local_gtfs"
    echo ""
    continue
  fi

  rm -f "$local_gtfs"

  if ! "${KUBECTL[@]}" create configmap "$cm_name" \
    --from-file=routes.json="$routes_file" \
    --dry-run=client -o yaml \
    -n "$NAMESPACE" \
    | "${KUBECTL[@]}" apply -n "$NAMESPACE" -f -; then
    echo "  SKIPPED: kubectl apply failed for $cm_name" >&2
    FAILED+=("$agency")
  fi
  echo ""
done

popd > /dev/null

if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "=== Done with errors. The following agencies need manual attention: ==="
  for a in "${FAILED[@]}"; do echo "  - $a"; done
  exit 1
else
  echo "=== Done. All routes ConfigMaps created. ==="
fi
