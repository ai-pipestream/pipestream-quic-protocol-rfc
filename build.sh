#!/usr/bin/env bash
set -euo pipefail

DRAFT_VERSION="${1:-01}"
DRAFT_NAME="draft-krickert-pipestream-${DRAFT_VERSION}"

# Add Homebrew Ruby gem binaries to PATH (kdrfc, kramdown-rfc2629, etc.)
export PATH="/opt/homebrew/lib/ruby/gems/4.0.0/bin:$PATH"

cd "$(dirname "$0")"

echo "==> Building ${DRAFT_NAME}..."

# 1. Convert Markdown source to IETF XML v3
echo "  [1/4] kramdown-rfc: draft-template.md -> XML"
kdrfc draft-template.md

# 2. Rename to official draft name
echo "  [2/4] Renaming to ${DRAFT_NAME}.xml"
mv draft-template.xml "${DRAFT_NAME}.xml"

# 3. Generate TXT and HTML
echo "  [3/4] xml2rfc: generating TXT and HTML"
xml2rfc "${DRAFT_NAME}.xml" --text --html

# 4. Validate
echo "  [4/4] idnits: validating ${DRAFT_NAME}.txt"
idnits --verbose "${DRAFT_NAME}.txt" || true

echo ""
echo "==> Build complete!"
echo "    XML:  ${DRAFT_NAME}.xml"
echo "    TXT:  ${DRAFT_NAME}.txt"
echo "    HTML: ${DRAFT_NAME}.html"
echo ""
echo "Preview at http://localhost:8000/${DRAFT_NAME}.html"
