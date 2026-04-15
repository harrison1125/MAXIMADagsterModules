def test_pytest_cov_branch_option_is_enabled(pytestconfig) -> None:
    cov_branch_enabled = bool(pytestconfig.getoption("cov_branch", default=False))

    assert cov_branch_enabled, "Coverage must be run with --cov-branch enabled for CI enforcement"
