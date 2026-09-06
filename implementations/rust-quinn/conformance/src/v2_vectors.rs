//! Frozen successor framing, hash and CDDL checks, without a protocol codec.
//! Semantic refusal expectations are inputs to later independent codec tests;
//! this module does not claim those state/authentication rules are implemented.

use super::{decode_hex, executable, hex, path, run_checked_owned, schema};
use anyhow::{Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

fn body<'a>(framing: &str, root: &str, wire: &'a [u8]) -> Result<&'a [u8]> {
    let prefix = if let Some(kind) = framing.strip_prefix("control:") {
        let kind = u8::from_str_radix(kind, 16)?;
        let expected = match kind {
            1 => "v2-capabilities",
            2 => "v2-session-message",
            3 => "v2-scope-message",
            4 => "v2-work-message",
            5 => "v2-result-request",
            6 => "v2-drain-message",
            7 => "v2-refusal",
            _ => bail!("unknown frozen control type"),
        };
        ensure!(
            root == expected,
            "control vector uses the wrong schema root"
        );
        ensure!(
            wire.first() == Some(&kind),
            "control type differs from index"
        );
        5
    } else {
        match framing {
            "input-header" => {
                ensure!(root == "v2-input-header", "wrong input schema");
                4
            }
            "result-header" => {
                ensure!(root == "v2-result-header", "wrong result schema");
                4
            }
            "record" => {
                ensure!(
                    matches!(root, "v2-result-manifest" | "v2-scope-summary"),
                    "wrong record schema"
                );
                return Ok(wire);
            }
            _ => bail!("unknown vector framing {framing}"),
        }
    };
    ensure!(wire.len() >= prefix, "truncated frozen length prefix");
    let length = u32::from_be_bytes(wire[prefix - 4..prefix].try_into()?) as usize;
    ensure!(
        length == wire.len() - prefix,
        "frozen frame length differs from body"
    );
    Ok(&wire[prefix..])
}

pub fn verify(repository: &Path) -> Result<()> {
    let machine = fs::read_to_string(repository.join("cddl/pipestream-v2.cddl"))?;
    schema::synchronized(
        &machine,
        &fs::read_to_string(repository.join("sections-src/appendix-f.md"))?,
    )?;
    let corpus = fs::read_to_string(repository.join("test-vectors/v2/wire.tsv"))?;
    let mut lines = corpus.lines();
    ensure!(
        lines.next() == Some("name\troot\tframing\tcddl\texpectation\terror\tsha256\thex"),
        "invalid version-2 corpus header"
    );
    let temp = tempfile::Builder::new()
        .prefix("pipestream-v2-cddl-")
        .tempdir()?;
    let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    let mut names = BTreeSet::new();
    let mut semantic_refusals = 0;
    let mut cddl_refusals = 0;
    for line in lines {
        let f = line.split('\t').collect::<Vec<_>>();
        ensure!(f.len() == 8, "malformed version-2 vector row");
        ensure!(
            !f[0].is_empty() && f[0].bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "invalid vector name"
        );
        ensure!(names.insert(f[0]), "duplicate version-2 vector name");
        ensure!(
            matches!(f[4], "accept" | "refuse"),
            "invalid expected codec result"
        );
        ensure!(
            (f[4] == "accept" && f[5] == "-") || (f[4] == "refuse" && f[5] != "-"),
            "missing named refusal"
        );
        let wire = decode_hex(f[7])?;
        ensure!(
            hex(&Sha256::digest(&wire)) == f[6],
            "frozen bytes changed: {}",
            f[0]
        );
        let body = body(f[2], f[1], &wire)?;
        if f[3] == "skip" {
            ensure!(
                f[4] == "refuse",
                "accepted wire cannot skip schema validation"
            );
            semantic_refusals += 1;
            continue;
        }
        ensure!(
            matches!(f[3], "valid" | "invalid"),
            "invalid CDDL classification"
        );
        if f[3] == "invalid" {
            cddl_refusals += 1;
            ensure!(f[4] == "refuse", "schema-invalid input cannot be accepted");
        } else if f[4] == "refuse" {
            semantic_refusals += 1;
        }
        let destination = temp.path().join(format!("{}-{}.cbor", f[3], f[0]));
        fs::write(&destination, body)?;
        groups.entry(f[1].to_owned()).or_default().push(destination);
    }
    ensure!(
        !names.is_empty() && cddl_refusals > 0 && semantic_refusals > 0,
        "incomplete version-2 corpus categories"
    );
    let bundle = executable(&["bundle", "bundle3.3"])?;
    // Invoke the pinned CDDL library with CBOR decoding only. The library's CLI
    // also accepts JSON fallback; that path is not part of these wire checks.
    let ruby = r#"
source = File.read(ARGV.shift)
ARGV.each do |file|
  warn("checking #{File.basename(file)}")
  parser = CDDL::Parser.new(source)
  instance = CBOR.decode(File.binread(file))
  matched = !!parser.validate(instance, false)
  expected = File.basename(file).start_with?('valid-')
  abort("CDDL expectation differs: #{File.basename(file)}") unless matched == expected
end
"#;
    for (root, instances) in groups {
        let schema_file = temp.path().join(format!("{root}.cddl"));
        fs::write(&schema_file, format!("v2-fixture-root = {root}\n{machine}"))?;
        let mut command = vec![
            bundle.clone(),
            "exec".into(),
            "ruby".into(),
            "-rcddl".into(),
            "-rcbor-diagnostic".into(),
            "-e".into(),
            ruby.into(),
            path(&schema_file),
        ];
        command.extend(instances.iter().map(|p| path(p)));
        run_checked_owned(repository, &command, Duration::from_secs(30))?;
    }
    let commitments = fs::read_to_string(repository.join("test-vectors/v2/commitments.tsv"))?;
    let commitment_count = verify_commitments(&commitments)?;
    println!(
        "verified {} frozen version-2 framing/CDDL vectors and {commitment_count} domain-separated commitments",
        names.len()
    );
    println!(
        "version-2 boundary: {cddl_refusals} schema refusals checked; {semantic_refusals} additional semantic/canonical refusal expectations frozen, not yet implementation conformance"
    );
    Ok(())
}

fn verify_commitments(source: &str) -> Result<usize> {
    let mut lines = source.lines();
    ensure!(
        lines.next() == Some("name\tdomain\tinput-hex\tsha256"),
        "invalid commitment corpus header"
    );
    let mut names = BTreeSet::new();
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == 4 && !fields[0].is_empty(),
            "malformed commitment vector"
        );
        ensure!(names.insert(fields[0]), "duplicate commitment vector");
        ensure!(
            fields[1].starts_with("pipestream-") && fields[1].is_ascii(),
            "invalid commitment domain"
        );
        let mut digest = Sha256::new();
        digest.update(fields[1].as_bytes());
        digest.update(decode_hex(fields[2])?);
        ensure!(
            hex(&digest.finalize()) == fields[3],
            "commitment changed: {}",
            fields[0]
        );
    }
    ensure!(!names.is_empty(), "empty commitment corpus");
    Ok(names.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_framing_and_root_cannot_be_weakened_by_metadata() {
        assert_eq!(
            body("control:02", "v2-session-message", &[2, 0, 0, 0, 1, 0]).unwrap(),
            &[0]
        );
        assert!(body("control:02", "v2-message", &[2, 0, 0, 0, 1, 0]).is_err());
        assert!(body("control:02", "v2-session-message", &[2, 0, 0, 0, 2, 0]).is_err());
        assert!(body("control:02", "v2-session-message", &[1, 0, 0, 0, 1, 0]).is_err());
        assert!(body("input-header", "v2-input-header", &[0, 0, 0]).is_err());
    }

    #[test]
    fn changed_commitment_bytes_are_detected_independently() {
        let source = include_str!("../../../../test-vectors/v2/commitments.tsv");
        assert_eq!(verify_commitments(source).unwrap(), 12);
        assert!(
            verify_commitments(&source.replacen(
                "pipestream-operation-v2",
                "pipestream-operation-v3",
                1
            ))
            .is_err()
        );
    }
}
