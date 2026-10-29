# loans-platform

Example repo to test Databricks Declarative Automation Bundles and CI/CD with Claude.

# loans_platform
Owner: Data Engineering · Business owner: Lending · Steward: data_engineering
Purpose: Clean loan data and a portfolio summary for lending reporting
Source: corebank_replica.dbo.loans (core banking replica)
Outputs: <env>.silver.loans, <env>.gold.loan_portfolio_summary
PII: ssn (masked for readers in prod, tokenized in non-prod)
SLA (example): gold refreshed by 7:00 AM; quarantine rate under 5%
Runbook: on failure, check quarantine reasons; fix source or code; start a new run
Environments: dev (engineers), test (CI only), prod (approved releases only)