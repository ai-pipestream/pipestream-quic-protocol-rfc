# Appendix C: Example Processing Patterns

This appendix is non-normative.

## Text Extraction

A text-extraction stage transforms binary document content into textual
or layout-aware intermediate representations. Typical outputs include
page text, OCR results, or format-specific structural markup.

## NLP Enrichment

An enrichment stage adds semantic metadata such as chunking,
embeddings, named entities, classifications, or relation annotations.
These results are commonly encoded in SemanticLayer payloads.

## Structured Table Extraction

A table-extraction stage identifies structured tabular regions and emits
normalized table representations suitable for indexing or analytics.

## Image Processing

An image-processing stage derives metadata or features from document
images, such as OCR overlays, captions, detections, or classification
results.

## Example Sink Patterns

Common sink patterns include search indexing, archival persistence, and
workflow notification. Backend-specific integrations such as particular
search engines, object stores, or message buses are deployment choices,
not protocol requirements of this profile.
