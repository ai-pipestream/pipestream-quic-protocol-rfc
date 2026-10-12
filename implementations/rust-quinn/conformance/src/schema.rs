use anyhow::{Context, Result, ensure};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

fn definitions(source: &str) -> BTreeMap<String, String> {
    let mut definitions = BTreeMap::new();
    let mut name: Option<String> = None;
    for line in source.lines() {
        let line = line.split(';').next().unwrap_or("").trim();
        if line.starts_with("~~~~") {
            name = None;
            continue;
        }
        if let Some((key, value)) = line.split_once('=')
            && !key.trim().is_empty()
            && key
                .trim()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            name = Some(key.trim().to_owned());
            definitions.insert(key.trim().to_owned(), value.to_owned());
        } else if let Some(name) = &name {
            definitions.get_mut(name).unwrap().push_str(line);
        }
    }
    for value in definitions.values_mut() {
        *value = value
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
            .replace(",)", ")")
            .replace(",}", "}");
    }
    definitions
}

pub fn synchronized(machine: &str, appendix: &str) -> Result<()> {
    let normative = definitions(appendix);
    let machine = definitions(machine);
    ensure!(
        machine.len() > 2,
        "machine-readable schema has no definitions"
    );
    for (name, value) in machine {
        if matches!(
            name.as_str(),
            "pipestream-message" | "pipestream-layer0-message"
        ) {
            continue;
        }
        ensure!(
            normative.get(&name) == Some(&value),
            "CDDL definition {name} differs from its normative appendix"
        );
    }
    Ok(())
}

/// Every sections-src markdown file except the authoritative appendix, sorted
/// by file name, as (file name, source) pairs.
pub fn section_sources(root: &Path) -> Result<Vec<(String, String)>> {
    let mut sources = Vec::new();
    for entry in fs::read_dir(root.join("sections-src")).context("read sections-src")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "appendix-c.md" || !name.ends_with(".md") {
            continue;
        }
        sources.push((name, fs::read_to_string(entry.path())?));
    }
    sources.sort();
    Ok(sources)
}

/// Compare every inline CDDL copy in the section sources against the
/// authoritative appendix. Only names defined in both places are compared;
/// a definition that exists in a single place is not a copy.
pub fn inline_copies_synchronized(appendix: &str, sections: &[(String, String)]) -> Result<()> {
    let normative = definitions(appendix);
    for (file, source) in sections {
        for (name, value) in definitions(source) {
            if let Some(authoritative) = normative.get(&name) {
                ensure!(
                    *authoritative == value,
                    "CDDL definition {name} in {file} differs from the authoritative Appendix C copy"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_source_drift_is_not_hidden_by_fixture_validation() {
        let machine = "a = uint\nb = { id: uint, }\nc = float16 / float32";
        let appendix =
            "~~~~ cddl\na = uint ; width\nb = {\n id: uint,\n}\nc = float16 / float32\n~~~~";
        synchronized(machine, appendix).unwrap();
        assert!(synchronized(machine, &appendix.replace("float16 / float32", "float32")).is_err());
    }

    #[test]
    fn inline_section_copies_match_the_authoritative_appendix() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize()
            .unwrap();
        let appendix = fs::read_to_string(root.join("sections-src/appendix-c.md")).unwrap();
        let machine = fs::read_to_string(root.join("cddl/pipestream-layer0.cddl")).unwrap();
        synchronized(&machine, &appendix).unwrap();
        inline_copies_synchronized(&appendix, &section_sources(&root).unwrap()).unwrap();
    }
}
