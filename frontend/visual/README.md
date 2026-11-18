# Workbench visual comparison

This harness renders the production React components with local, read-only data taken from the approved HTML prototype. It does not replace components or contact a model, account service, or development environment.

From `frontend`:

```bash
npx vite --config vite.visual.config.ts --port 5196
```

Scenes: `/?theme=bright`, `/?theme=warm`, `/?theme=dark`, `/?scene=empty`, and `/?scene=question`.

The optional reference mode requires the local approved prototype at `docs/prototypes/qa-ui-improvements/index.html`:

```bash
npx vite --config vite.visual.config.ts --mode reference --port 5195
```

Regenerate the scoped style contract and data snapshots with `node visual/extract-reference.cjs` when that local reference is available.

`approved-style-contract.json` records the original declarations and reference SHA-256. `approved-cases.json` contains example data only; neither enters the packaged application. Production style values are scoped in `src/styles/approved-workbench.css`; `approved-workbench-adapters.css` resets conflicting legacy and native component declarations without replacing application behavior.

Compare at 1600×1000 and 390×844. Inspect the right-side icons, Environment rows, Context bar/table, Outputs, file tabs, split/maximized workspace, plan/question cards, project and Secret dialogs, theme menus, and the mobile navigation portal. The message input must start at one line, grow with content, scroll internally at its cap, and return to one line when cleared.
