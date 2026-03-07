# Document Processing Pipeline Conventions

This section describes common conventions used by document-processing
deployments that implement this profile. It is intentionally
non-prescriptive: implementations MAY realize these stages using
different internal service boundaries, model stacks, and sink targets.

## PARSE Stage (Document Decomposition)

In this profile, the PARSE stage commonly performs document
decomposition. A root document entity MAY be split into child entities
representing pages, embedded documents, archive members, images, or
other logical subcomponents. The decomposition strategy is
implementation-specific but MUST preserve enough lineage metadata to
allow rehydration at later stages.

## PROCESS Stage

The PROCESS stage transforms document entities between profile-defined
layer representations. Typical examples include text extraction, OCR,
semantic chunking, embedding generation, entity recognition, and
structured field or table extraction. Example processing patterns are
described in Appendix C.

## SINK Stage

The SINK stage represents terminal consumption of document-processing
results. Common sink patterns include indexing, archival storage, and
workflow notification, but this profile does not require a fixed sink
registry or specific backend products.
