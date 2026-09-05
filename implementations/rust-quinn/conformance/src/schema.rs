use anyhow::{Result, ensure};
use std::collections::BTreeMap;

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
            "CDDL definition {name} differs from Appendix C"
        );
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
}
