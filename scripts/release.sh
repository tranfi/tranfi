#!/bin/bash
set -e

# Build optimized CLI binary and create a GitHub release.
# Usage: ./scripts/release.sh [VERSION]
# Example: ./scripts/release.sh 0.1.1

VERSION="${1:-$(grep 'set(PROJECT_VERSION' CMakeLists.txt | grep -oP '"\K[^"]+')}"
if [ -z "$VERSION" ]; then
  echo "Usage: $0 VERSION"
  exit 1
fi

TAG="v${VERSION}"
ARCH=$(uname -m)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')

# Normalize arch names
case "$ARCH" in
  x86_64)  ARCH="x64" ;;
  aarch64) ARCH="arm64" ;;
esac

ARTIFACT="tranfi-${OS}-${ARCH}"

echo "Building tranfi ${VERSION} for ${OS}-${ARCH}..."

# Build
mkdir -p build-release
cd build-release
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF > /dev/null 2>&1
make -j$(nproc) tranfi_cli 2>&1 | tail -1
cd ..

# Verify
./build-release/tranfi -v

# Package
mkdir -p dist
cp build-release/tranfi dist/${ARTIFACT}
tar -czf dist/${ARTIFACT}.tar.gz -C dist ${ARTIFACT}
rm dist/${ARTIFACT}

echo "Created dist/${ARTIFACT}.tar.gz"
ls -lh dist/${ARTIFACT}.tar.gz

# Create GitHub release
if command -v gh &> /dev/null; then
  echo "Creating GitHub release ${TAG}..."
  gh release create "${TAG}" \
    --title "${TAG}" \
    --notes "tranfi ${VERSION}" \
    dist/${ARTIFACT}.tar.gz \
    2>&1 || echo "Release ${TAG} may already exist. Uploading asset..."
  # If release exists, just upload the asset
  gh release upload "${TAG}" dist/${ARTIFACT}.tar.gz --clobber 2>/dev/null || true
  echo "Done: https://github.com/tranfi/tranfi/releases/tag/${TAG}"
else
  echo "gh CLI not found. Upload dist/${ARTIFACT}.tar.gz manually."
fi
