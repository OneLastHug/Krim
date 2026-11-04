import { build } from "vite";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  unlink,
  writeFile,
} from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cache = process.env.KRIM_AGENT_CORE_BUILD ?? resolve(root, "../.cache/agent-core/ac-m2-final");
const webRoot = resolve(root, "../src/krim_agent/web");
const fontDirectory = "assets/fonts";
const generatedFontManifest = ".vite/font-assets.json";
const packagedFontManifest = "font-manifest.json";
const fontPathPattern = /^assets\/fonts\/[A-Za-z0-9][A-Za-z0-9._-]*-[A-Za-z0-9_-]{6,64}\.woff2$/;
const sha256Pattern = /^[0-9a-f]{64}$/;
await mkdir(cache, { recursive: true });
const out = await mkdtemp(resolve(cache, "build-"));
let modules = [];

async function files(path, prefix = "") {
  const found = [];
  for (const name of await readdir(path)) {
    const full = resolve(path, name);
    const info = await lstat(full);
    if (info.isSymbolicLink()) throw new Error(`构建输出不能包含符号链接：${full}`);
    if (info.isDirectory()) found.push(...await files(full, `${prefix}${name}/`));
    else if (info.isFile()) found.push(`${prefix}${name}`);
    else throw new Error(`构建输出含非普通文件：${full}`);
  }
  return found;
}

async function sourceHashes() {
  const hashes = {};
  const names = [
    ...(await files(resolve(root, "src"))).map(name => `src/${name}`),
    ...(await files(resolve(root, "scripts"))).map(name => `scripts/${name}`),
    "index.html",
    "package.json",
    "package-lock.json",
    "tsconfig.json",
    "vite.config.ts",
  ];
  for (const name of names.sort()) {
    hashes[name] = createHash("sha256").update(await readFile(resolve(root, name))).digest("hex");
  }
  return hashes;
}

function parseManagedFontManifest(text, label) {
  let value;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw new Error(`${label}不是有效JSON：${error}`);
  }
  if (!value || Array.isArray(value) || typeof value !== "object"
    || Object.keys(value).sort().join(",") !== "fonts,version"
    || value.version !== 1 || !Array.isArray(value.fonts) || value.fonts.length > 1024) {
    throw new Error(`${label}不符合字体清单schema`);
  }
  const paths = new Set();
  const foldedPaths = new Set();
  for (const item of value.fonts) {
    if (!item || Array.isArray(item) || typeof item !== "object"
      || Object.keys(item).sort().join(",") !== "path,sha256,size_bytes"
      || typeof item.path !== "string" || !fontPathPattern.test(item.path)
      || !Number.isSafeInteger(item.size_bytes) || item.size_bytes <= 0
      || typeof item.sha256 !== "string" || !sha256Pattern.test(item.sha256)) {
      throw new Error(`${label}含无效字体条目`);
    }
    const folded = item.path.toLowerCase();
    if (paths.has(item.path) || foldedPaths.has(folded)) {
      throw new Error(`${label}含重复字体路径：${item.path}`);
    }
    paths.add(item.path);
    foldedPaths.add(folded);
  }
  const canonical = `${JSON.stringify({ version: 1, fonts: value.fonts }, null, 2)}\n`;
  if (text !== canonical) throw new Error(`${label}不是构建器生成的规范字体清单`);
  return value;
}

async function existingFontManifestNames() {
  const manifestPath = resolve(webRoot, packagedFontManifest);
  let text;
  try {
    const info = await lstat(manifestPath);
    if (info.isSymbolicLink() || !info.isFile()) {
      throw new Error(`既有字体清单必须是普通文件：${manifestPath}`);
    }
    if (info.size > 1_048_576) throw new Error("既有字体清单超过允许大小");
    text = await readFile(manifestPath, "utf8");
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }

  const fontDir = resolve(webRoot, fontDirectory);
  const assetsDir = resolve(webRoot, "assets");
  for (const directory of [assetsDir, fontDir]) {
    try {
      const info = await lstat(directory);
      if (info.isSymbolicLink() || !info.isDirectory()) {
        throw new Error(`既有字体目录必须是普通目录：${directory}`);
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }

  if (text === undefined) {
    try {
      const entries = await readdir(fontDir, { withFileTypes: true });
      if (entries.length) {
        throw new Error("没有旧字体清单，拒绝删除或覆盖未知字体目录内容");
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    return [];
  }

  const old = parseManagedFontManifest(text, "既有字体清单");
  const expected = new Set(old.fonts.map(item => item.path.split("/").at(-1)));
  try {
    for (const entry of await readdir(fontDir, { withFileTypes: true })) {
      if (entry.isSymbolicLink() || !entry.isFile() || !expected.has(entry.name)) {
        throw new Error(`字体目录含未被旧清单管理的内容，拒绝清理：${entry.name}`);
      }
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  return old.fonts.map(item => item.path);
}

await build({
  configFile: resolve(root, "vite.config.ts"),
  root,
  build: { outDir: out, emptyOutDir: true },
  plugins: [{
    name: "agent-ui-provenance",
    generateBundle(_options, bundle) {
      modules = [...new Set(Object.values(bundle).flatMap(item =>
        item.type === "chunk" ? Object.keys(item.modules) : []))];
    },
  }],
});

const outputFilesBeforeFontManifest = await files(out);
const fontFiles = outputFilesBeforeFontManifest
  .filter(name => name.startsWith(`${fontDirectory}/`))
  .sort();
const fonts = [];
for (const name of fontFiles) {
  if (!fontPathPattern.test(name)) throw new Error(`字体构建路径不符合约定：${name}`);
  const bytes = await readFile(resolve(out, name));
  if (bytes.subarray(0, 4).toString("ascii") !== "wOF2") {
    throw new Error(`字体构建产物不是WOFF2：${name}`);
  }
  fonts.push({
    path: name,
    size_bytes: bytes.length,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  });
}
const fontManifest = { version: 1, fonts };
await mkdir(resolve(out, ".vite"), { recursive: true });
await writeFile(resolve(out, generatedFontManifest), `${JSON.stringify(fontManifest, null, 2)}\n`);

const actual = (await files(out)).sort();
const product = ["index.html", "assets/app.js", "assets/styles.css"];
const buildManifests = [".vite/manifest.json", ".vite/licenses.json", generatedFontManifest];
const allowed = [...product, ...buildManifests, ...fontFiles].sort();
if (JSON.stringify(actual) !== JSON.stringify(allowed)) {
  throw new Error("构建资源集合不符：" + JSON.stringify({ actual, allowed }));
}
const index = await readFile(resolve(out, "index.html"), "utf8");
if (!index.includes("/ui/assets/app.js") || !index.includes("/ui/assets/styles.css")
  || /https?:\/\//.test(index)) {
  throw new Error("入口资源引用不符");
}
const sourceSnapshot = await sourceHashes();
if (JSON.stringify(sourceSnapshot) !== JSON.stringify(await sourceHashes())) {
  throw new Error("构建期间前端源码发生变化；生成物未复制，请重新构建");
}

const licenses = JSON.parse(await readFile(resolve(out, ".vite/licenses.json"), "utf8"));
for (const name of ["tw-animate-css", "tailwindcss"]) {
  const meta = JSON.parse(await readFile(resolve(root, "node_modules", name, "package.json"), "utf8"));
  licenses.push({
    name,
    version: meta.version,
    identifier: meta.license,
    text: await readFile(resolve(root, "node_modules", name, "LICENSE"), "utf8"),
  });
}
for (const name of [
  "@fontsource-variable/geist",
  "@fontsource-variable/geist-mono",
  "@fontsource-variable/noto-sans-sc",
]) {
  const packageRoot = resolve(root, "node_modules", name);
  const meta = JSON.parse(await readFile(resolve(packageRoot, "package.json"), "utf8"));
  if (meta.license !== "OFL-1.1") throw new Error(`字体包许可不是OFL-1.1：${name}`);
  const text = await readFile(resolve(packageRoot, "LICENSE"), "utf8");
  if (!text.includes("SIL OPEN FONT LICENSE Version 1.1")) {
    throw new Error(`字体包缺少原始OFL-1.1许可文本：${name}`);
  }
  licenses.push({ name, version: meta.version, identifier: meta.license, text });
}
const marker = "\n\nBundled dependencies\n\n";
const sourceNotice = await readFile(resolve(root, "THIRD-PARTY-NOTICES.txt"), "utf8");
const sourcePrefix = sourceNotice.split(marker)[0];
if (!sourcePrefix.includes("shadcn/ui") || !sourcePrefix.includes("MIT License")
  || !sourceNotice.includes(marker)) {
  throw new Error("缺少受审阅的 shadcn 许可前缀");
}
const notices = sourcePrefix + marker + licenses.map(item =>
  `${item.name}@${item.version}\nLicense: ${item.identifier}\n${item.text}\n`,
).join("\n--------------------\n\n");
if (licenses.some(item => !item.identifier || !item.text)) {
  throw new Error("依赖许可缺失，须核实：" + JSON.stringify(
    licenses.filter(item => !item.identifier || !item.text)));
}

const oldFontPaths = await existingFontManifestNames();
await writeFile(resolve(root, "THIRD-PARTY-NOTICES.txt"), notices);
await mkdir(webRoot, { recursive: true });
for (const name of product) {
  await copyFile(resolve(out, name), resolve(webRoot, name));
}
for (const oldPath of oldFontPaths) {
  try {
    await unlink(resolve(webRoot, oldPath));
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}
for (const name of fontFiles) {
  const destination = resolve(webRoot, name);
  await mkdir(dirname(destination), { recursive: true });
  await copyFile(resolve(out, name), destination);
}
await writeFile(
  resolve(webRoot, packagedFontManifest),
  `${JSON.stringify(fontManifest, null, 2)}\n`,
);
await copyFile(resolve(root, "THIRD-PARTY-NOTICES.txt"), resolve(webRoot, "THIRD-PARTY-NOTICES.txt"));

const hashes = {};
for (const name of [...product, ...fontFiles]) {
  hashes[name] = createHash("sha256").update(await readFile(resolve(out, name))).digest("hex");
}
await writeFile(resolve(out, ".vite/source-modules.json"), JSON.stringify(modules, null, 2));
await writeFile(resolve(out, ".vite/proof.json"), JSON.stringify({
  out,
  modules,
  source_hashes: sourceSnapshot,
  licenses: licenses.map(item => ({ name: item.name, version: item.version, license: item.identifier })),
  font_assets: fonts,
  hashes,
  imports: modules.filter(id => relative(root, id).startsWith("src/")),
}, null, 2));
console.log(JSON.stringify({
  out,
  products: [...product, ...fontFiles, packagedFontManifest, "THIRD-PARTY-NOTICES.txt"],
  font_assets: fonts.length,
  modules: modules.length,
}));
