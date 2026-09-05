//! Independent expected values for the exemplar's local receipt format.
//! These are application artifacts, not normative SCOPE_DIGEST payloads.

use sha2::{Digest, Sha256};

type Hash = [u8; 32];

fn hash(parts: &[&[u8]]) -> Hash {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn processed(payload: &[u8]) -> Hash {
    hash(&[b"pipestream-processed-v1", payload])
}

fn rehydrated(scope: u32, parent: u32, children: &[(u32, u32, Hash)]) -> Hash {
    let mut bytes = b"pipestream-rehydrated-v1".to_vec();
    bytes.extend_from_slice(&scope.to_be_bytes());
    bytes.extend_from_slice(&parent.to_be_bytes());
    for (scope, id, digest) in children {
        bytes.extend_from_slice(&scope.to_be_bytes());
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(digest);
    }
    hash(&[&bytes])
}

fn scope_digest(count: u32) -> Hash {
    let mut level: Vec<_> = (1..=count)
        .map(|id| hash(&[&[0], &id.to_be_bytes(), &[3]]))
        .collect();
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|pair| {
                if pair.len() == 1 {
                    pair[0]
                } else {
                    hash(&[&[1], &pair[0], &pair[1]])
                }
            })
            .collect();
    }
    level[0]
}

struct Row<'a> {
    scope: u32,
    id: u32,
    depth: u8,
    parent: Option<(u32, u32)>,
    payload: &'a [u8],
    output: Hash,
}

fn lineage(session: &str, rows: &[Row<'_>], scopes: &[(u32, Hash)]) -> Hash {
    let mut bytes = b"pipestream-lineage-v1".to_vec();
    bytes.extend_from_slice(&(session.len() as u64).to_be_bytes());
    bytes.extend_from_slice(session.as_bytes());
    for row in rows {
        bytes.extend_from_slice(&row.scope.to_be_bytes());
        bytes.extend_from_slice(&row.id.to_be_bytes());
        bytes.extend_from_slice(&[3, row.depth, 0]); // COMPLETE, depth, data layer
        if let Some((scope, id)) = row.parent {
            bytes.push(1);
            bytes.extend_from_slice(&scope.to_be_bytes());
            bytes.extend_from_slice(&id.to_be_bytes());
        } else {
            bytes.push(0);
        }
        bytes.extend_from_slice(&hash(&[row.payload]));
        bytes.push(1);
        bytes.extend_from_slice(&row.output);
    }
    for (scope, digest) in scopes {
        bytes.extend_from_slice(&scope.to_be_bytes());
        bytes.extend_from_slice(digest);
    }
    hash(&[&bytes])
}

pub fn recursive(session: &str) -> Hash {
    let ga = processed(b"grandchild-a");
    let gb = processed(b"grandchild-b");
    let ca = processed(b"child-a");
    let cb = rehydrated(1, 2, &[(2, 1, ga), (2, 2, gb)]);
    let cc = processed(b"child-c");
    let root = rehydrated(0, 1, &[(1, 1, ca), (1, 2, cb), (1, 3, cc)]);
    lineage(
        session,
        &[
            Row {
                scope: 0,
                id: 1,
                depth: 0,
                parent: None,
                payload: b"root",
                output: root,
            },
            Row {
                scope: 1,
                id: 1,
                depth: 1,
                parent: Some((0, 1)),
                payload: b"child-a",
                output: ca,
            },
            Row {
                scope: 1,
                id: 2,
                depth: 1,
                parent: Some((0, 1)),
                payload: b"child-b",
                output: cb,
            },
            Row {
                scope: 1,
                id: 3,
                depth: 1,
                parent: Some((0, 1)),
                payload: b"child-c",
                output: cc,
            },
            Row {
                scope: 2,
                id: 1,
                depth: 2,
                parent: Some((1, 2)),
                payload: b"grandchild-a",
                output: ga,
            },
            Row {
                scope: 2,
                id: 2,
                depth: 2,
                parent: Some((1, 2)),
                payload: b"grandchild-b",
                output: gb,
            },
        ],
        &[(1, scope_digest(3)), (2, scope_digest(2))],
    )
}

pub fn recovery(session: &str) -> Hash {
    let token = hash(&[b"pipestream-continuation-v1", b"durable-payload"]);
    let output = hash(&[b"pipestream-resumed-v1", &token]);
    lineage(
        session,
        &[Row {
            scope: 0,
            id: 1,
            depth: 0,
            parent: None,
            payload: b"durable-payload",
            output,
        }],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipts_bind_the_session_and_scenario() {
        assert_ne!(recursive("one"), recursive("two"));
        assert_ne!(recursive("one"), recovery("one"));
        assert_ne!(recovery("one"), recovery("two"));
    }
}
