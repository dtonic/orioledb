# Coding AI Instructions

General, repo-agnostic rules for how a coding agent should behave on this project. Referenced from [AGENTS.md](AGENTS.md); project-specific structure and conventions live there.

## MUST (non-negotiable)

- Produce secure-by-default code.
  - Validate inputs (format/length/range) and fail safely.
  - Never build SQL, shell commands, HTML or URLs by concatenating untrusted input; use parameterized queries, argument lists or escaping APIs.
  - Never hardcode or commit secrets (`.env` files included); load them from the environment or a secret store.
  - Never pass a secret as a command-line argument, where the process list shows it; use a file only you can read (deleted after use), stdin, or an environment variable.
  - Avoid leaking secrets/PII in logs and error messages.
  - Name no customer, site or customer cluster in code that ships in an image, nor in its comments, docstrings or prompts: it reaches every customer and every source audit. Describe it generically ("a customer cluster"); where a reader needs the specifics, link the issue or PR that records them, if there is one.
  - Enforce least privilege and explicit access control.
- Do not provide "dummy" implementations unless explicitly requested.
- If the task is ambiguous, ask before starting: the fewest questions that settle it, in one message.
- Do not refactor or restructure beyond the request; when the work needs it, propose it as a plan (§ Plan requirement).
- Keep changes minimal and scoped to the request.
  - Do not add features, abstractions, or configurability beyond what was asked.
  - Do not add error handling for scenarios that cannot occur.
  - Do not introduce abstractions for single-use code.
  - If a 200-line solution can be expressed in 50 lines, simplify it.
- Make surgical edits; do not "improve" adjacent code, comments, or formatting unrelated to the task.
  - Match existing style even if you would write it differently.
  - Only remove imports/variables/functions that your own changes rendered unused; mention pre-existing dead code rather than deleting it.
  - Every changed line should trace directly to the user's request.
- Prefer readability and maintainability; organize code into sensible files/modules.
- Be concise and technical.
- Do not add obvious, redundant, or restating comments; only comment non-obvious logic.
- When tools/tests are available for the change, run them and fix issues caused by the change.
- When stuck, find root cause; do not "switch libraries" as a workaround.
- Do not guess facts: file paths, function names, API endpoints, config values, UI labels, data values. Verify each in its source before writing it in code, docs or examples; where you cannot, say it is unverified.
- Do not weaken a check to get a change through (narrowing its scope, adding an exclusion, lowering a threshold); fix the finding, or add an exception scoped as narrowly as the tool allows, with its reason.

## SHOULD (default behavior)

- For frameworks/libraries/APIs, use up-to-date documentation (prefer Context7).
  - If Context7 is unavailable, use official docs / local code reference.
  - If external lookup is blocked, continue with explicit assumptions and call out uncertainty.
- When adding or changing a dependency, use the **latest stable version** and pin it as the ecosystem pins: an exact version in the manifest (e.g., `flask==3.1.0`, `"pkg": "1.2.3"`), or a committed lock file where the manifest takes ranges. Pin CI actions to a full commit SHA with the version in a comment, since a tag can be moved after review; pin container images to a fixed tag or digest, never a moving tag in a deployed environment. If an older version is needed, comment the reason.
- Read the whole file before editing it.
  - For large repos, also read enough adjacent code to understand call sites and invariants.
- Run formatting/linting after major changes (and after changing config), on the files you changed rather than a whole directory. Edit structured files (JSON, YAML) in place; loading and re-serializing one rewrites lines you did not touch.
- Break large or vague tasks into smaller steps; push back and ask to narrow scope when needed.
- When a fix or a hardening addresses a kind of defect, not just one place, look for the same kind where it can recur (other roles, modules, services, clusters, distributions, repositories, copies of a file) with a search that names its pattern. Report each hit as affected, not affected (why) or unverified, with the search and how many places it covered; a place the search could not reach is unverified, not clean. The same pattern does not always cause the same failure: check that the fix's premise holds at each hit. Fix other hits in this change only when the user agrees; record the rest where § Issues puts them, and list them in the PR (the template's Out of Scope where it has one).
- UI/UX work (when applicable): follow established design system and UX best practices.
- Run commands yourself when your tools allow it; give the user the exact command only for a step that needs them, such as an interactive login.
  - Before a command that changes a shared or production environment (a deploy, rollout, migration or data delete), name its target explicitly (context, account, inventory), dry-run first where the tool can, act on the narrowest target, and get the user's go-ahead.
- When a change alters what a repo doc states (README, spec, ADR, AGENTS.md), update that doc in the same change. Link to the file that holds a fact (an ADR, a lock file, a config) rather than restating it; copies drift.
- Keep agent rule files within their budget (§ Agent rule files).
- Before opening a PR, run the repo's checks and list each command with its result in the PR description (the template's Test Plan where it has one); for a check you skipped, say why.
- When a decision is needed (method, framework/library, architecture, tooling, etc.), research industry best practices and present 2–3 options with trade-offs so the user can make an informed choice.
  - Before proposing a plan or a decision that meets a condition of § Plan requirement, or a design, review verdict or root cause the user must decide on, find how others solved the same problem: the official docs, and two or three well-known products or open-source projects, read in their docs or source, not recalled. The `workflow` plugin's `prior-art` skill holds the procedure; a simple fix skips it.
  - For each option, name who uses it, with a link to what shows it, and what in this repository (its ADRs, constraints, scale, operations) makes it fit or not; precedent alone is not a reason. Where nothing applies, say what was searched. A claim from memory is marked unverified.
  - Mark each option a root fix or a workaround, and recommend one, the root fix by default; offer a workaround only as a step toward it, with what it leaves and the condition that removes it.

## Agent rule files

- Codex reads at most `project_doc_max_bytes` (32 KiB by default) of `AGENTS.md` text: the files from the repository root down to its working directory, together, and cuts the rest without telling the model. So the root `AGENTS.md` plus every nested `AGENTS.md` on the way to any directory stay within 32,768 bytes; where the repository has a check for it, the check enforces this.
- By default, Claude Code reads `AGENTS.md` — and a subdirectory's `AGENTS.md` once it reads a file there — only when no `CLAUDE.md`, `.claude/CLAUDE.md` or `CLAUDE.local.md` sits in its working directory, above it, or in that subdirectory. So a repository that keeps its rules in `AGENTS.md` commits none of these files; a personal `CLAUDE.local.md` hides the rules the same way.
- Put in an `AGENTS.md` only what an agent cannot learn by reading the code, or would not go and look for: commands, non-obvious conventions, what every new code path must also do, traps no test catches. A rule for one directory's code goes in that directory's `AGENTS.md`; the root keeps the rules that bind code across directories, one line each. Point at the guard test, the ADR or the module that holds the detail rather than restating it.
- A changelog or a lessons log that would make the file long lives in its own file that `AGENTS.md` links to (not imports).
- When a chain is over the budget, or its check reports it, shrink it before adding to it, section by section:
  1. Delete what the code already says: file-by-file descriptions, route, setting and value lists, a module's internals its docstring states.
  2. Cut a rule a test enforces to one line naming the guard.
  3. Delete why and what was measured; keep the ADR number.
  4. Move a rule one file or function must keep into its docstring or comment, where an editor of that file reads it.
  5. Move a rule for one directory into that directory's `AGENTS.md`, condensed.
  6. Keep in the root only rules that bind code outside one directory.
- While shrinking, keep the headings that code, ADRs and other repositories cite (`§` numbers, anchors), and re-point citations of a moved section in the same change. Check that every name the new text keeps exists, and that every rule it deletes is held by the code, a test or an ADR; list what was deleted, by section, in the PR for the owners to check.

## Plan requirement (when to ask for plan approval)

Decide by what the change does to the system, not by how many files it touches. Create a short plan and get user approval **before making changes** when the work:

- Changes the architecture: a module's responsibility or boundary, the directory structure, a new service, module or layer, or a framework replaced.
- Changes a contract other code or people rely on: an API route or its request/response shape, an event or message, a stored data format, the interface of a shared library, UI component or design token, a URL other pages link to, or a setting or environment variable another service reads.
- Changes or deletes stored data, or the schema it lives in (migrations).
- Touches authn/authz, crypto, a network boundary, secrets, or other security-sensitive logic.
- Adds, removes or upgrades a dependency.
- Changes how something is built or deployed: a CI/CD pipeline, a build or image definition, the deployment mechanism, or the infrastructure topology.

A change that meets none of the conditions above proceeds directly, however many files it touches: a fix inside a function, an internal rename, a test, a doc correction, a new instance of an existing template (a role, an inventory entry, a page). The conditions win over the examples: a one-line fix to an access check, a role that grants permissions or an inventory entry that adds a host still needs a plan.

## Commits

- Commit on your own branch, push that branch and open a pull request for it without asking: these are authorized in advance.
- Push to `main` or `master`, or merge a pull request, only when the user asks for it in this conversation.
- Apart from that pull request and its title and body, ask before posting anything others see outside git: filing, commenting on, labeling or closing an issue; a comment or review reply on a pull request; a review request, a bot's included. Show the text and post only what the user approves.
- One commit per logical change.
- Conventional Commits: `type(scope): subject`, with `type` and `scope` in lowercase English, using the types the repository's commitlint config allows where it has one. Write the subject and body in Korean or English; an English subject is imperative, lowercase, with no final period.
- The subject names the code change, not the review. "address review feedback" or "fix Codex P2" tells a reader of `git log` nothing: say what was wrong. A review round, a finding number or a bot's name lives only on its PR.
- Subject only by default. Add a body only when the diff cannot show why: what was wrong before and why this fix. Not a list of the files touched. A squash merge's body says what the PR changed and why.
- Do not skip hooks with `--no-verify`. A failed pre-commit or commit-msg hook means the commit did not happen: fix the cause and commit again, without `--amend`, which would rewrite the previous commit. A failed pre-push hook leaves the commit in place and stops only the push: fix the cause, in a new commit when it is in the code, and push again.

## Issues

- File an issue only when the work has no lighter home. It needs none when an existing issue or PR covers it, or when it waits for an event nobody schedules (both below); when it is knowledge rather than work, which goes in a note in a doc or the code; when it is small and part of the current change, which does it; when nobody would schedule it, since it affects no user, operator or other work and loses nothing unwritten; or when its cause or expected outcome is unknown, so no acceptance criterion can be written: investigate first, and if that must wait, file the investigation, with the questions to answer as its acceptance criteria.
- Before filing an issue, look for an open or closed issue, or an open PR, that already covers the problem; comment there instead of filing a duplicate, update its body when what you found changes its work (below), and reopen it if it is closed and what you found adds work, since a closed issue drops out of a search for open ones.
  - Search by the most specific terms the problem has — test name, error text, identifier, file path: `gh issue list --state all --search "<terms>" --limit 100`, `gh pr list --search "<terms>" --limit 100`. A whole title rarely matches, and `--limit` defaults to 30.
  - Keyword search misses work named differently: also check the open PRs that touch the files involved (`gh pr list --limit 200 --json number,title,files,changedFiles`), the follow-up table of the ADR behind the rule, and `git worktree list` for another session on the same task.
  - `files` holds at most 100 paths per PR: a PR whose `changedFiles` is larger is unchecked until its full list (`gh api --paginate 'repos/{owner}/{repo}/pulls/<N>/files?per_page=100'`) has been read; the REST call stops at 3,000 files, so a PR past that stays unchecked.
  - A problem that blocks everyone (a failing CI or pre-push check, a new CVE) is the one most likely filed twice the same day.
- Work that waits for an event nobody schedules — the next deploy or tag move on a cluster, a site visit, the next air-gap bundle, an upstream release, a cooldown that ends, the next change to a file — is recorded where that event happens. Whoever meets the event reads what is in front of them, not the issue tracker, and an event can pass while its issue still says it is waiting.
  - First choice: a check that stops at the event and says what to do — an assert's message, a test, a strict xfail, a scanner exception that expires.
  - Otherwise a note where that person reads at that moment: a code comment (with the repository's tag for these, such as `ponytail:`, where it has one), a comment beside the pin, a step in the runbook. The note states the condition, what to do and how to tell it is done, not only an issue number. The change that does the work deletes the note.
  - A date is rarely the trigger itself: it makes an action possible — the next toolchain bump, the next lock update — and the note goes where that action happens. When the date is the deadline, as for an exception that must not outlive its fix, give the entry an expiry that a check run on every change enforces, as a scanner's `expired_at` does.
  - An issue is for work someone has to schedule, decide or track across teams. When all an issue has left is covered at its trigger, say in a comment where the work now lives, point the body's Todo there with a `### Revisions` line, and close the issue as not planned: the work is moved, not done. A dependency upgrade that § Security & supply chain tracks in an issue stays open until the upgrade lands, its scanner exception's expiry notwithstanding.
- A change that removes a feature or a file settles the work that waits on it: it closes or updates the open issues that wait on it, and for each check or note it deletes, whose issue may already be closed, it moves the work to where it still applies or says in the change why the work no longer applies.
- An issue's body is its current state and its comments are its history. The body holds what the next person needs to do the work: goal, scope and out of scope, Todo, decisions and the options ruled out, and the facts and constraints the work rests on. Comments hold evidence, measurements, progress, discussion, and why the body changed.
  - A comment alone is enough when someone who reads only the body would still do the right work. When it finishes, adds, drops or moves work, makes a decision or rules an option out, or corrects what the body states, update the body too, whoever wrote the issue.
  - Post the evidence, or one line on what changed, as a comment first: a body edit notifies no one. Then re-read the body (`gh issue view <URL> --json body --jq .body > <file>`; a bare number reads the current checkout's issue) and change only the lines affected, since `gh issue edit <URL> --body-file <file>` replaces the whole body and a copy read earlier undoes another session's edit.
    - Read into a `mktemp` path, not a fixed name in the checkout, which may already hold a file. Keep the copy you read: `gh issue edit` has no compare-and-set, so read the body again just before the edit and, if it differs from that copy, start over from the new body.
  - End the body with a `### Revisions` section and add one dated line per edit, naming what changed and linking the comment (`- 2026-10-31: moved the decision from the 10-30 comment into Decisions ([comment](<URL>))`): `gh issue view` does not show that a body changed.
  - Tick a Todo item when it is done. A dropped item loses its checkbox and says where the work went, or moves under Out of scope, so it is not counted as work left.
- Before working on an issue, read its comments as well as its body. Off a terminal, `gh issue view <URL>` prints the body with only a comment count and `gh issue view <URL> --comments` prints the comments without the body: run both. When a comment changed the work and the body does not show it, update the body before starting.
- When an issue closes, by a merged PR's `Closes #N` or by hand, tick the Todo items that are done and leave the rest unticked, naming them and why in the closing PR or comment. GitHub ticks nothing itself, and an issue closed at 0 of 7 tasks reads as abandoned.

## Documentation language

Docs may be written in Korean prose; ADRs and their index entries are written
in Korean. Strings that serve as grep / anchor / citation targets are never
translated — in either direction:

- **Headings** keep their current canonical form (English or Korean as they
  stand): markdown anchors, `§` citations (including numbered ones like
  `§4-1`), and code-comment references derive from them — within a repo and
  across sibling repos.
- New ADRs, where the repo keeps them, take the headings of its ADR template
  (named in its ADR index, such as `docs/DECISIONS.md`) as written there;
  existing ADRs keep their current headings per the canonical-form rule above.
- Fenced code blocks stay verbatim — commands, inline comments, and output.
- Error messages, log lines, resource/variable names, and file paths.
- Technical terms and proper nouns stay English inline, unless the repo's
  glossary or its Korean UI names them — never as license to re-language an
  existing heading.

Rule of thumb: if someone (human or AI) might grep for it, keep it verbatim.

When writing Korean prose — docs, code comments, PR descriptions, review
replies — write natural Korean, not English thought translated word by word:

- No calques coined from English: "층위"(layer), "정박"(anchor),
  "맥락 인용"(cite for context), "방어력"(defensibility). Use the word a Korean
  practitioner in that field would use.
- No everyday English words dropped into a Korean sentence when a common Korean
  word exists: "헤드라인 수치" → "대표 수치". Name an experiment "arm" by its
  role — "대조군", "비교군", or "조건". Technical terms and proper nouns still
  stay English, per the list above.
- Korean spacing: particles attach to the preceding word, even after English or
  code (`NEP`에는); counters attach to Arabic numerals (`8개`, `제1회`, `4~7%`).
  English units keep their space (`4 vCPU`, `16 GiB`).

Where the repo's AGENTS.md names a writing standard or glossary for its docs,
that standard governs their prose and terms; the verbatim rules above still
hold.

## Security & supply chain

- Prefer community-trusted libraries for common tasks; avoid obscure dependencies.
- When adding/altering dependencies, run the configured security scans and address findings.
- When a scan flags a dependency whose fix is still inside the repo's release cooldown (uv `exclude-newer`, npm `min-release-age`), do not skip the hook or drop the cooldown. If the flaw is unreachable, add a scanner exception scoped to the lock path and the locked version, in an ignore format that can say both (trivy: `.trivyignore.yaml` with `paths` and `purls`, passed to every scan with `--ignorefile` or trivy-action's `trivyignores`; by default trivy reads only the plain `.trivyignore`, which matches the ID in every path and version), that expires a few days after the cooldown ends, and track the upgrade in an issue; if it is reachable, exempt that package alone up to just past the reviewed fixed release, where the package manager allows it (uv `exclude-newer-package` with a timestamp, never `false`), upgrade only that package in the lock, and track removing the exemption once the cooldown admits the fix.
- Do not recommend disabling security features (e.g., XML entity protections, type checking for untrusted deserialization).

## Self-check (for higher-risk outputs)

For safety-sensitive areas (security, compliance, deployment, payments), do a quick self-review:

- Identify assumptions and state them.
- Check for data exposure, injection risks, and access control gaps.
- Tighten language (avoid guarantees) unless backed by evidence.
