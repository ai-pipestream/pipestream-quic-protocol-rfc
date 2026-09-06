# Entity Payload Model

This section describes the payload model visible to the core protocol.
PipeStream distinguishes between the wire-level Entity Header carried on
Entity Streams and any application-level envelope that may be embedded
within the payload bytes.

## Entity Header

The normative wire-level Entity Header is defined in Section 6.8.2. It
provides transport-level identification and routing metadata, including
`entity-id`, `parent-id`, `scope-id`, `parent-scope-id`, `layer`, `payload-length`, and
optional integrity metadata. The core protocol interprets these fields
for stream processing, flow control, and recursive coordination, but it
does not define application-specific payload structure.

## Data Layers

PipeStream defines a 2-bit Data Layer field (values 0-3) in the Entity
Header that identifies the payload encoding class. The semantics of each
layer value are defined by Application Profile specifications. The core
protocol does not assign meaning to specific layer values; it ensures
that the layer field is faithfully transmitted and that receivers can
route entities to appropriate handlers based on layer.

Application Profiles SHOULD define at most four data layers. Common
patterns include:

| Layer | Conventional Meaning | Purpose |
|-------|----------------------|---------|
| 0 | Raw or binary payload | Unprocessed input |
| 1 | Annotated or enriched payload | Intermediate processing artifacts |
| 2 | Structured or normalized payload | Final extracted or transformed output |
| 3 | Application-specific extension | Domain extension point |

This layering enables pipeline stages to progressively transform
entities from raw input to structured output while maintaining type
distinction through the layer field.

## Application-Level Envelope

The Entity Header (Section 6.8.2) provides transport-level
identification (`entity-id`, `scope-id`, and `layer`). Applications that
require additional identification, such as a stable input
identifier that persists across pipeline runs, multi-tenancy context, or
domain-specific metadata, SHOULD define an application-level envelope
message carried within the entity payload.

If an application-level envelope carries its own entity identifier, that
identifier MUST match the `entity-id` in the enclosing Entity Header.
The definition of application-level envelopes is outside the scope of
this specification. See [PIPESTREAM-DOCPROC] for an example companion
profile that defines such an envelope for document-processing workloads.

## Common Storage References

PipeStream defines a common storage reference type for entities that
refer to externally stored data. Application Profiles are not required
to use this type but SHOULD prefer it over ad hoc storage reference
formats to improve interoperability between pipeline stages from
different implementations.

~~~~ cddl
file-storage-reference = {
  provider: tstr,                ; Storage provider identifier
  bucket: tstr,                  ; Bucket/container name
  key: tstr,                     ; Object key/path
  ? region: tstr,                ; Optional region hint
  ? attrs: { * tstr => tstr },   ; Provider-specific attributes
  ? encryption: encryption-metadata,
}

encryption-metadata = {
  algorithm: tstr,               ; "AES-256-GCM", "AES-256-CBC"
  ? key-provider: tstr,          ; "aws-kms", "azure-keyvault",
                                 ; "gcp-kms", "vault"
  ? key-id: tstr,                ; Key ARN/URI/ID
  ? wrapped-key: bstr,           ; Client-side encrypted DEK
  ? iv: bstr,                    ; Initialization vector
  ? context: { * tstr => tstr }, ; Encryption context
}
~~~~

The algorithm and provider strings above are illustrative, not a negotiated
cryptographic suite. In particular, a CBC label alone does not supply message
authentication. A profile using encrypted references must specify the complete
authenticated construction and encodings under Section 10.7; Core provides
no default cipher, tag placement, or key-wrapping format.
