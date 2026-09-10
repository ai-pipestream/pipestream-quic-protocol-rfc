//! Mechanical independence gate (h): a source scan, not a build-graph query.
//! The conformance crate must drive subjects only through their compiled
//! binaries; importing a production crate here is a hard failure.

#[cfg(test)]
mod tests {
    use std::path::Path;

    fn forbidden_tokens() -> Vec<String> {
        // Built dynamically so this gate file itself never contains the
        // forbidden literal crate names.
        ["core", "quic", "quinn"]
            .iter()
            .map(|suffix| format!("pipestream_{suffix}"))
            .chain(
                ["quic", "quinn"]
                    .iter()
                    .map(|suffix| format!("pipestream-{suffix}")),
            )
            .collect()
    }

    #[test]
    fn conformance_sources_import_no_production_crates() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders: Vec<String> = Vec::new();
        crate::visit_files(&root, &mut |file| {
            let contents = std::fs::read_to_string(file)
                .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
            for (index, line) in contents.lines().enumerate() {
                // The gate targets crate imports (`use ...` / `extern crate`),
                // not binary path strings such as target/release/<name>.
                let import = line.contains("use ") || line.contains("extern crate");
                if !import {
                    continue;
                }
                for token in forbidden_tokens() {
                    if line.contains(&token) {
                        offenders.push(format!(
                            "{}:{} contains forbidden production-crate import {token}: {line}",
                            file.display(),
                            index + 1
                        ));
                    }
                }
            }
            Ok(())
        })
        .expect("scan conformance sources");
        assert!(
            offenders.is_empty(),
            "conformance crate must stay free of production dependencies:\n{}",
            offenders.join("\n")
        );
    }
}
