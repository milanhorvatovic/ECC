use super::*;

#[test]
fn sequential_empty_after_preserves_legacy_serialization() -> Result<()> {
    let step = OrchestrationTemplateStepConfig {
        name: Some("operator".into()),
        task: "task".into(),
        ..Default::default()
    };
    let saved = toml::to_string_pretty(&step)?;
    assert!(!saved.contains("after"), "{saved}");
    assert_eq!(saved, "name = \"operator\"\ntask = \"task\"\n");
    Ok(())
}

#[test]
fn sequential_duplicate_prerequisite_is_rejected() {
    let result = resolve(
        "[[orchestration_templates.chain.steps]]\nname = 'A'\ntask = 'one'\n\
         [[orchestration_templates.chain.steps]]\nname = 'B'\ntask = 'two'\nafter = ['A', 'A']",
    );
    let error = result.expect_err("duplicate prerequisite was accepted").to_string();
    assert!(error.contains("template chain step B"), "{error}");
    assert!(error.contains("duplicate prerequisite A"), "{error}");
}

fn resolve(source: &str) -> Result<ResolvedOrchestrationTemplate> {
    let cfg: Config = toml::from_str(source)?;
    cfg.resolve_orchestration_template("chain", &BTreeMap::new())
}

#[test]
fn sequential_config_parses_after_and_resolves_default_and_variable_names() -> Result<()> {
    let cfg: Config = toml::from_str(
        r#"
[[orchestration_templates.chain.steps]]
task = "first"
[[orchestration_templates.chain.steps]]
name = "{{name}}"
task = "second"
after = ["step 1"]
[[orchestration_templates.chain.steps]]
task = "third"
after = ["{{name}}"]
"#,
    )?;
    let template = cfg.resolve_orchestration_template(
        "chain",
        &BTreeMap::from([("name".to_string(), "review".to_string())]),
    )?;
    assert_eq!(template.steps[1].after, vec!["step 1"]);
    assert_eq!(template.steps[2].after, vec!["review"]);
    Ok(())
}

#[test]
fn sequential_config_rejects_unknown_self_ambiguous_and_cycles() {
    for (names, dependency, expected) in [
        (["A", "B", "C"], "missing", "unknown step"),
        (["A", "B", "C"], "C", "itself"),
        (["A", "A", "C"], "A", "ambiguous step"),
    ] {
        let source = format!(
            "[[orchestration_templates.chain.steps]]\nname = {:?}\ntask = 'one'\n\
             [[orchestration_templates.chain.steps]]\nname = {:?}\ntask = 'two'\n\
             [[orchestration_templates.chain.steps]]\nname = {:?}\ntask = 'three'\nafter = [{dependency:?}]",
            names[0], names[1], names[2]
        );
        let error = resolve(&source).unwrap_err().to_string();
        println!("{error}");
        assert!(error.contains("template chain step C"), "{error}");
        assert!(error.contains(expected), "{error}");
    }
    let error = resolve(
        "[[orchestration_templates.chain.steps]]\nname = 'A'\ntask = 'one'\nafter = ['B']\n\
         [[orchestration_templates.chain.steps]]\nname = 'B'\ntask = 'two'\nafter = ['A']",
    )
    .unwrap_err()
    .to_string();
    println!("{error}");
    assert!(error.contains("template chain step A"), "{error}");
    assert!(error.contains("cycle"), "{error}");
}

#[test]
fn sequential_config_load_rejects_literal_graph_and_save_preserves_after() -> Result<()> {
    let root = std::env::temp_dir().join(format!("ecc2-template-config-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root)?;
    let config_path = root.join("ecc2.toml");
    std::fs::write(
        &config_path,
        "[[orchestration_templates.chain.steps]]\nname = 'A'\ntask = '{{task}}'\nafter = ['missing']",
    )?;
    let error = Config::load_from_paths(&[], std::slice::from_ref(&config_path))
        .unwrap_err()
        .to_string();
    println!("config load: {error}");
    assert!(error.contains("template chain step A"), "{error}");
    assert!(error.contains("unknown step missing"), "{error}");
    std::fs::write(
        &config_path,
        "[[orchestration_templates.chain.steps]]\nname = 'A'\ntask = '{{task}}'\n\
         [[orchestration_templates.chain.steps]]\nname = 'B'\ntask = 'review'\nafter = ['A']",
    )?;
    let loaded = Config::load_from_paths(&[], std::slice::from_ref(&config_path))?;
    loaded.save()?;
    let saved = std::fs::read_to_string(Config::config_path())?;
    assert!(saved.contains("after = [\"A\"]"), "{saved}");
    let reloaded: Config = toml::from_str(&saved)?;
    assert_eq!(
        loaded.orchestration_templates,
        reloaded.orchestration_templates
    );
    let resolved = reloaded.resolve_orchestration_template(
        "chain",
        &BTreeMap::from([("task".into(), "build".into())]),
    )?;
    assert_eq!(resolved.steps[1].after, vec!["A"]);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
