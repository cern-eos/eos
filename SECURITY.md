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

Security reports may include findings discovered, analyzed, or validated with the help of AI coding assistants or other automated tools. We welcome these reports when they are submitted responsibly and include enough information for maintainers to independently verify the issue.

If an AI coding assistant, automated scanner, or similar tool was used, please disclose the tool usage in your report. Include, where applicable:

- The name of the AI coding assistant or tool used
- The model or version, if known
- Whether the tool was used for discovery, code review, exploit generation, remediation suggestions, or report writing
- Any relevant prompts, generated output, or tool findings needed to understand the report
- The steps you personally took to verify that the vulnerability is real and exploitable

Reports generated entirely by AI or automated tools must still include a clear explanation, reproducible steps, and evidence of security impact. We may close reports that contain unverified AI-generated claims, hallucinated issues, or generic scanner output without demonstrated exploitability.

Do not submit private project data, secrets, credentials, personal data, or other sensitive information to AI tools unless you are authorized to do so and the tool is approved for that use.

## Use of AI Agents in Security Reporting

Security reports may be created, submitted, triaged, or followed up on with the assistance of AI agents, automation agents, or other delegated systems. This is allowed, provided that the report remains accurate, verifiable, and attributable to a responsible human reporter or organization.

If an agent was used in preparing or managing a security report, please disclose this in the report. Include, where applicable:

- The name or type of agent used
- The platform, framework, or service provider, if known
- Whether the agent was used for vulnerability discovery, code analysis, exploit testing, report generation, submission, or follow-up communication
- Whether the agent acted autonomously or under human supervision
- The identity or contact details of the human or organization responsible for the report
- Any limitations of the agent’s findings that maintainers should be aware of

If follow-up communication is handled by an agent, the reporter must ensure that:

- The agent can respond accurately to maintainer questions
- A human reviewer can be involved when requested
- The agent does not send excessive, repetitive, or irrelevant messages
- The agent does not pressure maintainers for immediate action, disclosure, credit, or compensation
- The agent does not disclose vulnerability details publicly or to unauthorized parties

Reports submitted or managed by agents must still follow this security policy, including responsible disclosure requirements. We may pause or close reports where agent-generated communication is misleading, unverifiable, abusive, spam-like, or not backed by a responsible human contact.

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