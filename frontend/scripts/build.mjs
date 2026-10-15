import { mkdir, copyFile, readFile, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const target = resolve(root, "../src/krim_agent/web");
await mkdir(target, { recursive: true });
for (const file of ["index.html", "host.html", "styles.css"]) {
  await copyFile(resolve(root, file), resolve(target, file));
}
for (const file of ["api", "app", "agents-ui", "tasks-ui", "workspace-ui", "workspace-changes-ui", "directory-changes-ui", "tree-moves-ui", "tree-exchanges-ui", "generated-tree-undos-ui", "commands-ui", "workspace-trees-ui", "background-streams", "host", "host-shell", "prompt-context", "prompt-context-ui", "i18n", "skills-ui", "links", "drag-drop", "change-list-ui", "checkpoint-ui", "native-checkpoint-ui", "directory-checkpoint-ui", "attachments-ui", "mcp-ui", "oauth-ui"]) {
  const source = await readFile(resolve(root, `src/${file}.ts`), "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  });
  await writeFile(resolve(target, `${file}.js`), compiled.outputText, "utf8");
}
