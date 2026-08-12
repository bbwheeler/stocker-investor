## BEFORE YOU START
Before you begin anything, follow these steps:
1. Make sure all existing changes have been checked in; if there are existing changes, check them in.
2. Do a git fetch so that you have all of the latest changes.
3. Create a branch appropriate for the changes (e.g., `feat/` or `fix/` prefix)

## WHEN FINISHED
Once you complete anything, follow these steps:
1. Check the code into your branch using git
2. Push the branch to the remote (`git push origin <branch-name>`)
3. Open a Merge Request on git.wheeli.ca and add me (brian) as a reviewer

### How to create a PR and add yourself as reviewer
```bash
# Create a new MR via the Forgejo API:
curl -X POST https://git.wheeli.ca/api/v1/repos/brian/stocker-investor/mr \
  -H 'Authorization: token <ACCESS_TOKEN>' \
  -d '{"title":"<MR title>","source_branch":"<your branch name>","target_branch":"main"}'

# Add yourself as reviewer:
curl -X POST https://git.wheeli.ca/api/v1/repos/brian/stocker-investor/mr/<MR number>/reviewers \
  -H 'Authorization: token <ACCESS_TOKEN>' \
  -d '{"reviewers":["brian"]}'
```

Credentials for git.wheeli.ca are in the parent directory (../credentials.md).
