#!/usr/bin/env bash
set -euo pipefail
umask 077

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
command -v podman >/dev/null || { echo 'E1 requires Podman; no host tools are installed automatically.' >&2; exit 1; }
[[ ! -L "$root/target" ]] || { echo 'E1 refuses a symlinked target directory.' >&2; exit 1; }
mkdir -p "$root/target"
output=$(mktemp -d "$root/target/ci-linux-XXXXXX")
name="rpgmigrate-e1-${output##*/}"
created=false

trap 'if [[ "$created" == true ]]; then podman rm --force "$name" >/dev/null; fi' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

podman build --iidfile "$output/image.id" --file "$root/tools/ci/Containerfile" "$root/tools/ci"
image_id=$(cat "$output/image.id")
podman create --name "$name" --label rpgmigrate.task=e1-ci \
    --cap-drop=ALL --security-opt=no-new-privileges --pids-limit=512 \
    --network=bridge --env "E1_IMAGE_ID=$image_id" \
    --volume "$root:/source:ro" "$image_id" >/dev/null
created=true
status=0
podman start --attach "$name" || status=$?
podman cp "$name:/work/target/ci-artifacts/report.json" "$output/report.json" || status=1
[[ -s "$output/report.json" ]] || status=1
printf 'E1 evidence: %s/report.json\n' "$output"
exit "$status"
