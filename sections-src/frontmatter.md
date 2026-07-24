---
title: "PipeStream: A Recursive Entity Streaming Protocol for Distributed Processing over QUIC"
abbrev: "PipeStream"
docname: draft-krickert-pipestream-03
category: std
submissiontype: IETF
number:
date: 2026-07-23
consensus: true
v: 3
area: "Applications and Real-Time"
workgroup: "Individual Submission"
keyword:
 - quic
 - streaming
 - recursive
 - distributed-processing
 - scatter-gather
 - consistency
github: ai-pipestream/pipestream-quic-protocol-rfc
venue:
  group: Individual
  mail: kristian.rickert@pipestream.ai

author:
 -
    fullname: Kristian Rickert
    organization: PipeStream AI
    email: kristian.rickert@pipestream.ai

normative:
  RFC2119:
  RFC8174:
  RFC9000:
  RFC8446:
  RFC8126:
  RFC8949:
  RFC8610:
  RFC5234:
  RFC3986:
  RFC7595:
  RFC7301:
  FIPS-180-4:
    title: "Secure Hash Standard (SHS)"
    author:
      org: National Institute of Standards and Technology
    date: 2015-08
    seriesinfo:
      FIPS: PUB 180-4

informative:
  RFC7942:
  PIPESTREAM-DOCPROC:
    title: "PipeStream Document Processing Profile"
    author:
      - ins: "K. Rickert"
        name: "Kristian Rickert"
    date: 2026-03
    target: https://github.com/ai-pipestream/pipestream-quic-protocol-rfc/tree/main/docproc
  RFC9114:
  RFC9297:
  RFC9308:
    title: "Applicability of the QUIC Transport Protocol"
    author:
      - ins: "M. Kuehlewind"
        name: "Mirja Kuehlewind"
      - ins: "B. Trammell"
        name: "Brian Trammell"
    date: 2022-09
    seriesinfo:
      RFC: "9308"
    target: https://www.rfc-editor.org/rfc/rfc9308
  RFC9250:
  RFC9260:
    title: "Stream Control Transmission Protocol"
    author:
      - ins: "R. Stewart"
        name: "Randall Stewart"
      - ins: "M. Tuexen"
        name: "Michael Tuexen"
      - ins: "K. Nielsen"
        name: "Kirsty Nielsen"
    date: 2022-06
    seriesinfo:
      RFC: "9260"
    target: https://www.rfc-editor.org/rfc/rfc9260
  RFC7574:
  RFC7696:
  MOQT: I-D.ietf-moq-transport
  scatter-gather:
    title: "The Scatter-Gather Design Pattern"
    author:
      - ins: "D. Lea"
        name: "Doug Lea"
    date: 1996
    seriesinfo:
      DOI: 10.1007/978-1-4612-1260-6

--- abstract
