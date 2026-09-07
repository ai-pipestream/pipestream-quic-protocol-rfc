use clap::CommandFactory;

#[test]
fn all_command_groups_have_consistent_argument_definitions() {
    crate::Cli::command().debug_assert();
}
