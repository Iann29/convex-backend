#!/usr/bin/env bash
# Build the AMAGE Convex C7 backend image from this checkout, reproducibly.
#
#   self-hosted/build-candidate7.sh                       # clean committed tree
#   CANDIDATE_DATE=20260920 self-hosted/build-candidate7.sh   # repeat an earlier version string
#   ALLOW_DIRTY=1 self-hosted/build-candidate7.sh         # uncommitted tree; the manifest records it
#   CARGO_BUILD_JOBS=6 self-hosted/build-candidate7.sh    # default 6
#   SKIP_BUILD=1 self-hosted/build-candidate7.sh          # only re-record an image already built
#
# Identity inputs come from self-hosted/amage-candidate7.json: base, patchCommit,
# cliCommit, patchPaths, variant, protocol. The script hashes the patch with the
# exact command self-hosted/test-candidate.mjs uses (base vs working tree, a/ b/
# prefixes), builds with `sudo -n docker build`, tags
# amage-convex-backend:amage-<date>-candidate.<variant>-<patchSha12> (local tag
# only, never pushed) and writes the observed identity back into the manifest:
# patchSha256, version, buildArgs, image, imageId (docker's .Id: the OCI index
# digest on a containerd image store, the config digest on the classic one),
# amd64ManifestDigest and configDigest (walked from `docker image save`; the
# config digest is content-addressed and survives push/pull), labels,
# binarySha256, binaryVersion, buildSourceTree/headCommit/buildTreeDirty, builtAt.
# Needs: git, python3, sudo -n docker (BuildKit). ~10 min with warm cache mounts.
set -euo pipefail

cd "$(dirname "$0")/.."
MANIFEST=self-hosted/amage-candidate7.json

field() {
  python3 -c 'import json,sys
v = json.load(open(sys.argv[1]))[sys.argv[2]]
print(" ".join(v) if isinstance(v, list) else v)' "$MANIFEST" "$1"
}
BASE=$(field base)
PATCH_COMMIT=$(field patchCommit)
CLI_COMMIT=$(field cliCommit)
VARIANT=$(field variant)
PROTOCOL=$(field protocol)
read -ra PATCH_PATHS <<<"$(field patchPaths)"

DIRTY=$(git status --porcelain)
if [[ -n "$DIRTY" && "${ALLOW_DIRTY:-0}" != 1 ]]; then
  printf 'refusing to build a dirty tree (ALLOW_DIRTY=1 overrides and records it):\n%s\n' "$DIRTY" >&2
  exit 1
fi
for commit in "$PATCH_COMMIT" "$CLI_COMMIT"; do
  git merge-base --is-ancestor "$commit" HEAD || { echo "$commit is not an ancestor of HEAD" >&2; exit 1; }
done

patch_sha() {
  git diff --binary --no-ext-diff --no-color --src-prefix="$1" --dst-prefix="$2" \
    --abbrev=9 "$BASE" -- "${PATCH_PATHS[@]}" | sha256sum | cut -d' ' -f1
}
PATCH_SHA=$(patch_sha a/ b/)      # harness + image label
PATCH_SHA_CI=$(patch_sha c/ i/)   # AMAGE.md C6 command (diff.mnemonicPrefix)
DATE=${CANDIDATE_DATE:-$(date -u +%Y%m%d)}
VERSION="amage-${DATE}-candidate.${VARIANT}+base.${BASE:0:12}.patch.${PATCH_SHA:0:12}"
IMAGE="amage-convex-backend:amage-${DATE}-candidate.${VARIANT}-${PATCH_SHA:0:12}"
TIMESTAMP=$(date -u -d "@$(git show -s --format=%ct "$BASE")" +%Y-%m-%dT%H:%M:%SZ)
JOBS=${CARGO_BUILD_JOBS:-6}

echo "patch  $PATCH_SHA (a/ b/) | $PATCH_SHA_CI (c/ i/)"
echo "build  $VERSION -> $IMAGE"
[[ "${SKIP_BUILD:-0}" == 1 ]] || sudo -n docker build --platform linux/amd64 \
  -f self-hosted/docker-build/Dockerfile.backend -t "$IMAGE" \
  --build-arg CARGO_BUILD_JOBS="$JOBS" \
  --build-arg VERGEN_GIT_SHA="$BASE" \
  --build-arg VERGEN_GIT_COMMIT_TIMESTAMP="$TIMESTAMP" \
  --build-arg AMAGE_PATCH_SHA256="$PATCH_SHA" \
  --build-arg AMAGE_PATCH_COMMIT="$PATCH_COMMIT" \
  --build-arg AMAGE_PROTOCOL="$PROTOCOL" \
  --build-arg CONVEX_RELEASE_VERSION="$VERSION" .

IMAGE_ID=$(sudo -n docker image inspect --format '{{.Id}}' "$IMAGE")
LABELS=$(sudo -n docker image inspect --format '{{json .Config.Labels}}' "$IMAGE")
CREATED=$(sudo -n docker image inspect --format '{{.Created}}' "$IMAGE")
SAVED=$(mktemp -d)
# shellcheck disable=SC2024  # redirect on purpose: the tar stays owned by the invoking user
sudo -n docker image save "$IMAGE" > "$SAVED/image.tar"
read -r MANIFEST_DIGEST CONFIG_DIGEST < <(python3 - "$SAVED/image.tar" <<'PY'
import json, sys, tarfile
tar = tarfile.open(sys.argv[1])
blob = lambda digest: json.load(tar.extractfile("blobs/sha256/" + digest.split(":", 1)[1]))
manifest = json.load(tar.extractfile("index.json"))["manifests"][0]
doc = blob(manifest["digest"])
if doc["mediaType"].endswith(".index.v1+json"):  # containerd store: index -> platform manifest
    manifest = next(m for m in doc["manifests"]
                    if m.get("platform", {}).get("os") == "linux" and m["platform"].get("architecture") == "amd64")
    doc = blob(manifest["digest"])
print(manifest["digest"], doc["config"]["digest"])
PY
)
rm -rf "$SAVED"
# --cpuset-cpus=0: the build host has an unstable TSC; pin every backend start.
BINARY_SHA=$(sudo -n docker run --rm --cpuset-cpus=0 --entrypoint sha256sum "$IMAGE" /convex/convex-local-backend | cut -d' ' -f1)
BINARY_VERSION=$(sudo -n docker run --rm --cpuset-cpus=0 --entrypoint ./convex-local-backend "$IMAGE" --version)
[[ "$BINARY_VERSION" == "local_backend $VERSION" ]] || { echo "binary reports '$BINARY_VERSION'" >&2; exit 1; }
python3 -c 'import json,sys; l=json.loads(sys.argv[1]); assert l["org.amage.patch-sha256"]==sys.argv[2] and l["org.opencontainers.image.version"]==sys.argv[3] and l["org.amage.patch-commit"]==sys.argv[4] and l["org.amage.verified-deploy.protocol"]==sys.argv[5], l' \
  "$LABELS" "$PATCH_SHA" "$VERSION" "$PATCH_COMMIT" "$PROTOCOL"

MANIFEST="$MANIFEST" PATCH_SHA="$PATCH_SHA" PATCH_SHA_CI="$PATCH_SHA_CI" VERSION="$VERSION" \
IMAGE="$IMAGE" IMAGE_ID="$IMAGE_ID" MANIFEST_DIGEST="$MANIFEST_DIGEST" CONFIG_DIGEST="$CONFIG_DIGEST" \
LABELS="$LABELS" BINARY_SHA="$BINARY_SHA" \
BINARY_VERSION="$BINARY_VERSION" JOBS="$JOBS" TIMESTAMP="$TIMESTAMP" DIRTY="$DIRTY" CREATED="$CREATED" \
TREE="$(git rev-parse 'HEAD^{tree}')" HEAD_COMMIT="$(git rev-parse HEAD)" \
python3 - <<'EOF'
import json, os
e = os.environ
path = e["MANIFEST"]
m = json.load(open(path))
m.update({
    "patchSha256": e["PATCH_SHA"],
    "patchSha256MnemonicPrefixes": e["PATCH_SHA_CI"],
    "version": e["VERSION"],
    "buildSourceTree": e["TREE"],
    "headCommit": e["HEAD_COMMIT"],
    "buildTreeDirty": e["DIRTY"].splitlines(),
    "builtAt": e["CREATED"],
    "buildArgs": {
        "CARGO_BUILD_JOBS": e["JOBS"],
        "VERGEN_GIT_SHA": m["base"],
        "VERGEN_GIT_COMMIT_TIMESTAMP": e["TIMESTAMP"],
        "AMAGE_PATCH_SHA256": e["PATCH_SHA"],
        "AMAGE_PATCH_COMMIT": m["patchCommit"],
        "AMAGE_PROTOCOL": m["protocol"],
        "CONVEX_RELEASE_VERSION": e["VERSION"],
    },
    "image": e["IMAGE"],
    "imageId": e["IMAGE_ID"],
    "amd64ManifestDigest": e["MANIFEST_DIGEST"],
    "configDigest": e["CONFIG_DIGEST"],
    "labels": json.loads(e["LABELS"]),
    "binarySha256": e["BINARY_SHA"],
    "binaryVersion": e["BINARY_VERSION"],
})
with open(path, "w") as f:
    json.dump(m, f, indent=2, ensure_ascii=False)
    f.write("\n")
EOF
echo "image    $IMAGE"
echo "imageId  $IMAGE_ID"
echo "manifest $MANIFEST_DIGEST (linux/amd64)"
echo "config   $CONFIG_DIGEST"
echo "binary   $BINARY_SHA"
echo "version  $BINARY_VERSION"
echo "manifest $MANIFEST updated"
