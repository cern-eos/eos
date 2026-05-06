# Security Policy

## Reporting a Vulnerability

We take security issues seriously and appreciate responsible disclosure from the community.

If you believe you have found a security vulnerability in this project, please report it privately. Do **not** open a public GitHub issue for security vulnerabilities.

## How to Report

Please use one of the following private reporting channels:

1. **CERN GitLab — primary and preferred reporting channel**

   Use CERN GitLab for vulnerability reports whenever possible. This is the primary development repository and the preferred place to submit security reports.

   https://gitlab.cern.ch/dss/eos/-/security/vulnerability_report

2. **GitHub — for external collaborators without CERN GitLab access**

   Use GitHub private vulnerability reporting only if you are an external collaborator and do not have access to CERN GitLab.

   https://github.com/cern-eos/eos/security/

Please do not submit the same report through multiple channels unless requested by the maintainers.

When reporting a vulnerability, please include as much detail as possible, including:

- A description of the issue
- Steps to reproduce the vulnerability
- Affected versions, branches, or configurations
- Any proof-of-concept code, logs, screenshots, or error messages
- The potential impact of the vulnerability
- Any suggested remediation, if available

## Use of AI Coding Assistants in Vulnerability Research

We welcome thoughtful security reports that are discovered, analyzed, validated, or documented with the help of AI coding assistants, automated scanners, or similar tools. These tools can be useful for reviewing code, identifying suspicious patterns, improving reports, and suggesting possible remediations.

To help maintainers understand and reproduce the finding, please mention any AI coding assistant or automated tool that substantially contributed to the report. Where applicable, include:

- The name of the AI coding assistant or tool used
- The model, version, or service, if known
- How the tool was used, such as discovery, code review, exploit analysis, remediation suggestions, or report writing
- Any relevant prompts, generated output, scanner results, or tool findings needed to understand the issue
- The steps you took to verify the vulnerability and confirm its security impact

AI-assisted reports are most helpful when they include clear reasoning, reproducible steps, and evidence that the issue is real and exploitable. Reports do not need to be perfect, but they should provide enough information for maintainers to independently assess the finding.

Please avoid submitting private project data, secrets, credentials, personal data, or other sensitive information to AI tools unless you are authorized to do so and the tool is approved for that use.

## Use of AI Agents in Security Reporting

We also welcome reports that are prepared, submitted, triaged, or followed up on with the help of AI agents, automation agents, or other delegated systems. Agent-assisted reporting can be useful when it improves clarity, consistency, and responsiveness.

If an agent was used in preparing or managing a security report, please disclose this so that maintainers understand how the report and any follow-up communication are being handled. Where applicable, include:

- The name or type of agent used
- The platform, framework, or service provider, if known
- Whether the agent was used for vulnerability discovery, code analysis, exploit testing, report generation, submission, or follow-up communication
- Whether the agent acted autonomously or under human supervision
- The identity or contact details of the human or organization responsible for the report
- Any relevant limitations of the agent’s findings or communication

If follow-up communication is handled by an agent, please make sure that:

- The agent can provide accurate and relevant responses to maintainer questions
- A human reviewer can join the conversation when requested
- Communication remains focused, respectful, and useful
- Vulnerability details are kept private and shared only with authorized parties
- Requests for disclosure, credit, or compensation are handled appropriately and respectfully

When agents are used to identify or submit multiple findings, please keep the volume of simultaneous reports to a manageable level. Reports should be prioritized and submitted in order of highest criticality and security impact first. Where several related findings exist, consider grouping them into a single report when this helps maintainers understand the overall issue and reduces duplicate or overlapping communication.

Reports submitted or managed with the help of agents remain welcome under this policy, provided they follow the same responsible disclosure expectations as any other report and are backed by a responsible human contact.

## Supported Versions

Security updates are provided for the following versions:

| Version | Supported |
|---|---|
| Latest stable release | Yes |
| Older releases | No |
| Development branches | Best effort |

Only the latest stable release is guaranteed to receive security fixes unless otherwise stated.

## Disclosure Process

After a vulnerability is reported, we will make reasonable efforts to:

1. Acknowledge receipt of the report within 5 business days.
2. Investigate and validate the issue.
3. Work on a fix or mitigation if the issue is confirmed.
4. Coordinate disclosure timing with the reporter when appropriate.
5. Publish a security advisory or release notes after a fix is available, if needed.

We ask reporters to give us a reasonable amount of time to investigate and address the issue before making any information public.

## Responsible Disclosure Guidelines

We ask that security researchers and users:

- Do not publicly disclose the vulnerability until we have had a chance to investigate and address it.
- Do not access, modify, or delete data that does not belong to you.
- Do not perform testing that could degrade, disrupt, or damage the project, services, users, or infrastructure.
- Do not use social engineering, phishing, spam, physical attacks, or denial-of-service attacks.
- Provide enough information for us to reproduce and understand the issue.

Reports made in good faith under this policy are appreciated.

## Scope

This policy applies to the code and documentation in this repository.

The following are generally out of scope unless they demonstrate a clear security impact:

- Vulnerabilities in unsupported versions
- Issues requiring physical access to a user’s device
- Social engineering attacks
- Denial-of-service attacks
- Reports from automated scanners without evidence of exploitability
- Missing security headers without a demonstrated impact
- Issues in third-party dependencies that are not directly exploitable through this project

## Security Updates

Security fixes may be released as:

- A patch release
- A commit to the default branch
- A GitHub Security Advisory
- Updated documentation or configuration guidance

Users are encouraged to keep their deployments up to date with the latest stable release.

## No Bug Bounty

Unless explicitly stated otherwise, this project does not offer a paid bug bounty program. We are grateful for responsible reports but cannot guarantee monetary rewards.

## Contact

For security-related questions or vulnerability reports, please use the private reporting channels listed above.