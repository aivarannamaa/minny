## Starting a session

- Read README.md and decisions from docs/adr to get acquainted. No need to mirror your understanding back to me at this point.
- The main code is in src/minny, tests are in tests.

## Working style

- Don't rush with interpretation! I like when you mirror back my ideas to make sure we mean the same thing.
- Don't rush with obeying! Usually I expect you to first think with your own "head" and look around before you start working on some task (unless I give you very specific order in a confident way). I'd like you to catch my sloppy thinking.
- Don't rush with actions! Only start changing code when my last comments imply this. Sometimes I want to get your opinion first.
- Don't bother with niceties or try to be agreeable. I can handle critique if my idea seems weak.
- Don't be afraid to hesitate.
- DON'T START CODING (EVEN ADDING TESTS) WHEN I JUST ASK YOUR OPINION!

## Specifics

- When writing or updating ADR-s, use single physical line per paragraph. The reader will use soft-wrapping.

## Validating changes

- run `./format-and-check.sh` for formatting and linting. This is cheap to run, so it is recommended to run it after every task or block of tasks.
- run `./quick-tests.sh` for some unit testing if it seems the right thing to do.
- Don't run `./slow-tests.sh`. I'll run it myself when required.
- Use `./launch.sh` to launch minny for testing. This launcher uses `uv run python -m minny` and passes all arguments to minny.
- Avoid prepending each actual command with `cd`-ing to project directory. Do it only if necessary. Gratuitous `cd`-ing would mess up my allowlists.
