# Complete CDDL Schema

This appendix provides the profile's consolidated CDDL
{{RFC8610}} definitions.

The `file-storage-reference` and `encryption-metadata` types are defined
by reference in PipeStream Core Appendix C and are reused here without
modification.

~~~~ cddl
pipe-doc = {
  profile-version: uint,
  doc-id: tstr,
  entity-id: uint,
  ? search-metadata: search-metadata,
  ? blob-bag: blob-bag,
  ? semantic-result: semantic-processing-result,
  ? layer2-payload: layer2-payload,
  ? custom-entity: any,
  ? ownership: ownership-context,
}

search-metadata = {
  ? title: tstr,
  ? keywords: [* tstr],
  ? description: tstr,
  ? custom-fields: { * tstr => tstr },
}

blob-bag = blob / blobs

blobs = {
  blobs: [* blob],
}

blob = {
  blob-id: tstr,
  ? drive-id: tstr,
  content: bstr / file-storage-reference,
  ? mime-type: tstr,
  ? filename: tstr,
  ? size-bytes: int,
  ? checksum: tstr,
  ? checksum-type: checksum-type,
}

checksum-type = &(
  unspecified: 0,
  md5: 1,
  sha1: 2,
  sha256: 3,
  sha512: 4,
)

semantic-processing-result = {
  ? chunks: [* semantic-chunk],
  ? chunking-strategy: tstr,
  ? processing-metadata: { * tstr => tstr },
}

semantic-chunk = {
  chunk-id: tstr,
  ? chunk-number: int,
  ? embedding-info: chunk-embedding,
  ? metadata: { * tstr => any },
  ? annotations: [* nlp-annotation],
}

chunk-embedding = {
  text-content: tstr,
  ? vector: [* float],
  ? model-id: tstr,
  ? original-char-start-offset: int,
  ? original-char-end-offset: int,
}

nlp-annotation = {
  type: tstr,
  label: tstr,
  ? start-offset: int,
  ? end-offset: int,
  ? confidence: float,
  ? attributes: { * tstr => tstr },
}

parsed-metadata = {
  parser-id: tstr,
  ? fields: { * tstr => any },
  ? tables: [* table-data],
  ? raw-output: tstr,
}

layer2-payload = {
  ? parsed-metadata: { * tstr => parsed-metadata },
  ? structured-data: any,
}

table-data = {
  table-id: tstr,
  ? headers: [* tstr],
  ? rows: [* table-row],
}

table-row = {
  cells: [* tstr],
}

ownership-context = {
  ? tenant-id: tstr,
  ? owner-id: tstr,
  ? acl: [* tstr],
}
~~~~
