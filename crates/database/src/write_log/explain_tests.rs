use common::components::CanonicalizedComponentFunctionPath;

use super::*;

fn sources() -> anyhow::Result<Vec<WriteSource>> {
    let id = Arc::new(UdfIdentifier::Function(
        CanonicalizedComponentFunctionPath {
            component: "child".parse()?,
            udf_path: "messages:write".parse()?,
        },
    ));
    Ok(vec![
        WriteSource::Udf(id.clone()),
        WriteSource::SystemUdf(id),
        WriteSource::system("cleanup"),
        WriteSource::system("different_cleanup"),
    ])
}

#[test]
fn diagnostic_identity_preserves_source_equality_in_both_directions() -> anyhow::Result<()> {
    let sources = sources()?;
    for (i, left) in sources.iter().enumerate() {
        for (j, right) in sources.iter().enumerate() {
            let explained_left = left.clone().with_execution_id(ExecutionId::new());
            let explained_right = right.clone().with_execution_id(ExecutionId::new());
            assert_eq!(explained_left == *right, i == j);
            assert_eq!(*left == explained_right, i == j);
            assert_eq!(explained_left == explained_right, i == j);
        }
    }
    Ok(())
}

#[test]
fn provenance_retains_latest_id_without_changing_public_source_metadata() -> anyhow::Result<()> {
    for source in sources()? {
        let first_id = ExecutionId::new();
        let latest_id = ExecutionId::new();
        let explained = source.clone().with_execution_id(first_id);
        let wrapped = explained.clone().with_execution_id(latest_id);
        assert_eq!(source.execution_id(), None);
        assert_eq!(explained.execution_id(), Some(first_id));
        assert_eq!(wrapped.execution_id(), Some(latest_id));
        for candidate in [&explained, &wrapped] {
            assert_eq!(candidate, &source);
            assert_eq!(format!("{candidate:?}"), format!("{source:?}"));
            assert_eq!(candidate.display_name(), source.display_name());
            assert_eq!(candidate.is_udf(), source.is_udf());
            assert!(candidate.udf_identifier() == source.udf_identifier());
        }
    }
    let sources = sources()?;
    assert_eq!(
        sources[0].display_name().as_deref(),
        Some("child/messages.js:write")
    );
    assert!(sources[0].is_udf() && sources[0].udf_identifier().is_some());
    assert!(!sources[1].is_udf() && sources[1].udf_identifier().is_none());
    assert!(!sources[2].is_udf() && sources[2].udf_identifier().is_none());
    Ok(())
}
