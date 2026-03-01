---
title: "PipeStream: A Recursive Entity Streaming Protocol for Distributed Processing over QUIC"
abbrev: "PipeStream"
docname: draft-krickert-pipestream-01
category: std
submissiontype: IETF
number:
date: 2026-03-01
consensus: true
v: 3
area: "Applications and Real-Time"
workgroup: "Individual Submission"
keyword:
 - quic
 - streaming
 - recursive
 - document-processing
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

informative:
  FIPS-180-4:
    title: "Secure Hash Standard (SHS)"
    author:
      org: National Institute of Standards and Technology
    date: 2015-08
    seriesinfo:
      FIPS: PUB 180-4
  scatter-gather:
    title: "The Scatter-Gather Design Pattern"
    author:
      - ins: "D. Lea"
        name: "Doug Lea"
    date: 1996
    seriesinfo:
      DOI: 10.1007/978-1-4612-1260-6

--- abstract
