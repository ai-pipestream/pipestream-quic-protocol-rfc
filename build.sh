#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-core}"

case "${TARGET}" in
  core)
    DRAFT_VERSION="${2:-04}"
    TEMPLATE_MD="draft-template.md"
    OUTPUT_DIR="."
    DRAFT_NAME="draft-krickert-pipestream-${DRAFT_VERSION}"
    ;;
  docproc)
    DRAFT_VERSION="${2:-00}"
    TEMPLATE_MD="docproc/draft-template.md"
    OUTPUT_DIR="docproc"
    DRAFT_NAME="draft-krickert-pipestream-docproc-${DRAFT_VERSION}"
    ;;
  *)
    # Backward compatibility: allow `./build.sh 02` for core drafts.
    DRAFT_VERSION="${TARGET}"
    TEMPLATE_MD="draft-template.md"
    OUTPUT_DIR="."
    DRAFT_NAME="draft-krickert-pipestream-${DRAFT_VERSION}"
    ;;
esac

TEMPLATE_XML="${TEMPLATE_MD%.md}.xml"
OUTPUT_XML="${OUTPUT_DIR}/${DRAFT_NAME}.xml"
OUTPUT_TXT="${OUTPUT_DIR}/${DRAFT_NAME}.txt"
OUTPUT_HTML="${OUTPUT_DIR}/${DRAFT_NAME}.html"

cd "$(dirname "$0")"

if command -v bundle >/dev/null 2>&1; then
  BUNDLE=(bundle)
elif command -v bundle3.3 >/dev/null 2>&1; then
  BUNDLE=(bundle3.3)
else
  echo "Bundler is required. Install it with: gem install bundler" >&2
  exit 1
fi

echo "==> Building ${DRAFT_NAME}..."

# 1. Convert Markdown source to IETF XML v3
echo "  [1/4] kramdown-rfc: ${TEMPLATE_MD} -> XML"
"${BUNDLE[@]}" exec kdrfc "${TEMPLATE_MD}"

# 2. Rename to official draft name
echo "  [2/4] Renaming to ${OUTPUT_XML}"
mv "${TEMPLATE_XML}" "${OUTPUT_XML}"

# 3. Generate TXT and HTML
echo "  [3/4] xml2rfc: generating TXT and HTML"
uvx --from xml2rfc==3.34.0 xml2rfc "${OUTPUT_XML}" --text --html

# 4. Validate
echo "  [4/4] idnits: validating ${OUTPUT_TXT}"
idnits --verbose "${OUTPUT_TXT}"

echo ""
echo "==> Build complete!"
echo "    XML:  ${OUTPUT_XML}"
echo "    TXT:  ${OUTPUT_TXT}"
echo "    HTML: ${OUTPUT_HTML}"
echo ""
echo "Preview at http://localhost:8000/${OUTPUT_HTML#./}"
