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
for (const file of ["api", "app", "host", "host-shell", "prompt-context", "prompt-context-ui", "i18n", "skills-ui", "links", "drag-drop"]) {
  const source = await readFile(resolve(root, `src/${file}.ts`), "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  });
  await writeFile(resolve(target, `${file}.js`), compiled.outputText, "utf8");
}
