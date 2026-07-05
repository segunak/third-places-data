# Third Places Data

Third Places Data is a collection of data operations used to aggregate information about local third places—cafes, coffee shops, libraries, and more—into a single platform. This repository abstracts the data-fetching and transformation logic so it can be reused for various city-based third place directories.

It was branched from [Charlotte Third Places](https://github.com/segunak/charlotte-third-places/) after I realized it doesn't need to be coupled with the front end of that website.

## GitHub Agentic Workflows

This repository uses GitHub Agentic Workflows through `gh-aw`.

Safe outputs are gh-aw's controlled write path. The AI agent asks for a GitHub action, such as creating a PR, issue, or comment, and a separate GitHub Actions job performs that write with the configured token. The agent job itself stays read-only.

- Install the CLI extension with `gh extension install github/gh-aw`.
- Initialize or refresh repo support with `gh aw init`.
- Use `.github/workflows/<workflow>.md` as the source file.
- Treat `.github/workflows/<workflow>.lock.yml` as generated output. Do not edit lock files by hand.
- Compile workflow source with `gh aw compile <workflow-id>` after frontmatter changes.
- Keep the checked-in hook active locally with `git config core.hooksPath .githooks`.
- Use `permissions: copilot-requests: write` for Copilot inference.
- Include `safe-outputs.github-token: ${{ secrets.GH_AW_GITHUB_TOKEN }}` in every gh-aw workflow source file.
- `GH_AW_GITHUB_TOKEN` is the single broad GitHub operations token for safe outputs.

Authoritative gh-aw references:

- [Authentication](https://github.github.com/gh-aw/reference/auth/)
- [Permissions](https://github.github.com/gh-aw/reference/permissions/)
- [Safe Outputs](https://github.github.com/gh-aw/reference/safe-outputs/)
- [Safe Outputs for Pull Requests](https://github.github.com/gh-aw/reference/safe-outputs-pull-requests/)
- [Triggering CI](https://github.github.com/gh-aw/reference/triggering-ci/)
