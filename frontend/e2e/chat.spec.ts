import { test, expect, type Page } from "@playwright/test";
import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createConnection } from "node:net";

const SOURCE = resolve(import.meta.dirname, "../.."), EVIDENCE = process.env.KRIM_AGENT_CORE_EVIDENCE!;
const PYTHON = process.env.KRIM_AGENT_CORE_INSTALLED_PYTHON!;
const INTENTS = "krim.agent-core.intents/1", KNOWN = "krim.agent-core.known/1";
type Ready = { api_port: number; model_port: number; url: string; module: string; version: string; generation: number; namespace_local_pid: number };
function barrier() { let release!: () => void; const promise = new Promise<void>(r => { release = r; }); return { promise, release }; }
async function bounded<T>(promise: Promise<T>, ms = 8000): Promise<T> { let timer: ReturnType<typeof setTimeout>; try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("bounded wait expired")), ms); })]); } finally { clearTimeout(timer!); } }
async function waitFile(path: string) { return bounded((async () => { for (;;) { try { return JSON.parse(await readFile(path, "utf8")); } catch { await new Promise(r => setTimeout(r, 10)); } } })()); }
async function portClosed(port: number) { return new Promise<boolean>(resolve => { const socket = createConnection({ host: "127.0.0.1", port }); socket.once("error", () => { socket.destroy(); resolve(true); }); socket.once("connect", () => { socket.destroy(); resolve(false); }); socket.setTimeout(500, () => { socket.destroy(); resolve(false); }); }); }
async function startServer(root: string, database: string, generation: number, token: string, port = 0) {
  await mkdir(root, { recursive: true });
  const argv = ["-I", "-B", resolve(SOURCE, "tests/agent_core/fixtures/session_server.py"), "--database", database, "--evidence", root, "--port", String(port), "--token", token, "--generation", String(generation)];
  const env = { ...process.env }; delete env.PYTHONPATH; delete env.KRIM_GUIDE_SOURCE; delete env.KRIM_AGENT_CORE_SOURCE;
  for (const key of Object.keys(env)) if (key.startsWith("KRIM_AGENT_") && !key.startsWith("KRIM_AGENT_CORE_")) delete env[key];
  const child = spawn(PYTHON, argv, { cwd: root, env, stdio: ["ignore", "pipe", "pipe"] });
  const out = createWriteStream(resolve(root, "stdout.log")), err = createWriteStream(resolve(root, "stderr.log"));
  const finishedOut = new Promise<void>(r => out.once("finish", r)), finishedErr = new Promise<void>(r => err.once("finish", r)); child.stdout!.pipe(out); child.stderr!.pipe(err);
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r, reject) => { child.once("exit", (code, signal) => r({ code, signal })); child.once("error", reject); });
  let closed = false;
  const owner = { child, ready: null as Ready | null, async close() {
    if (closed) return; closed = true;
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    const exit = await bounded(exited); await Promise.all([finishedOut, finishedErr]);
    const state = await waitFile(resolve(root, "closed.json")); const ready = owner.ready;
    const ports = ready ? { api: await portClosed(ready.api_port), model: await portClosed(ready.model_port) } : null;
    await writeFile(resolve(root, "ownership.json"), JSON.stringify({ argv: [PYTHON, ...argv], cwd: root, owned_namespace_pid: child.pid, exit, ports, closed: state }, null, 2));
    expect(exit).toEqual({ code: 0, signal: null }); expect(state.unsafe_gc).toBe(0); expect(state.connections_pending).toEqual([]); expect(state.model_active).toBe(0); expect(state.provider_active).toBe(0); expect(state.second_writer).toBe(true); expect(ports).toEqual({ api: true, model: true });
  } };
  try {
    owner.ready = await waitFile(resolve(root, "ready.json")); expect(owner.ready!.namespace_local_pid).toBe(child.pid); expect(owner.ready!.version).toBe("0.4.0a1"); expect(owner.ready!.module).toBe(resolve(PYTHON,"../../lib/python3.12/site-packages/krim_agent/__init__.py")); return owner;
  } catch (error) { await owner.close(); throw error; }
}

async function connect(page: Page, token: string) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (!await drawer.isVisible()) {
    const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
    if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  }
  await page.getByRole("button", { name: "密钥身份", exact: true }).click(); await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click(); await page.locator("#api-key").fill(token); await page.locator("#connect-save").click(); await expect(page.locator(".toolbar-actions").getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
  // 连接完成后恢复抽屉开合状态，避免遮挡主区操作。
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}
async function openDrawerIfMobile(page: Page) {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (await drawer.isVisible()) return;
  const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
  if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
}
async function selectSession(page: Page, title: string) {
  await openDrawerIfMobile(page);
  await page.getByRole("button", { name: title, exact: true }).click();
}
async function navigateView(page: Page, view: "chat" | "run") {
  const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
  let container = page.locator(".desktop-sidebar");
  if (await mobileMenu.isVisible()) {
    await mobileMenu.click();
    container = page.locator(".mobile-drawer-content");
  }
  const button = container.locator(".sidebar-buttons").getByRole("button", { name: view === "run" ? "导航到运行记录" : "导航到对话", exact: true });
  await expect(button).toBeVisible();
  await button.focus(); await expect(button).toBeFocused(); await page.keyboard.press("Enter");
  await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe(view);
}
async function createSession(page: Page, title: string) { await page.locator(".toolbar-actions").getByRole("button", { name: "新建对话", exact: true }).click(); await page.locator("#session-title").fill(title); await page.getByRole("button", { name: "创建对话", exact: true }).click(); await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText(title); return new URL(page.url()).searchParams.get("session")!; }
async function send(page: Page, text: string) { await expect(page.locator("#chat-prompt")).toBeEnabled(); await page.locator("#chat-prompt").fill(text); await page.locator(".chat-composer button[type=submit]").click(); }
async function bottom(page: Page) { await page.locator(".chat-transcript").evaluate(el => { el.scrollTop = el.scrollHeight; }); }
async function turnStatus(page: Page, token: string) {
  const sessionId = new URL(page.url()).searchParams.get("session");
  const turnId = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
  expect(sessionId).toBeTruthy(); expect(turnId).toBeTruthy();
  const response = await page.request.get(new URL(
    `/v2/workspaces/local/sessions/${encodeURIComponent(sessionId!)}/turns/${encodeURIComponent(turnId!)}`,
    page.url(),
  ).toString(), { headers: { Authorization: "Bearer " + token } });
  expect(response.ok()).toBe(true);
  const turn = await response.json();
  expect(turn.id).toBe(turnId);
  return turn.status as string;
}
async function expectTurnStatus(page: Page, token: string, status: string) {
  await expect.poll(() => turnStatus(page, token)).toBe(status);
}
async function answer(page: Page, text: string, token: string) {
  await expect(page.locator(".chat-turn").last().locator(".chat-answer")).toHaveText(text);
  await expectTurnStatus(page, token, "completed");
}

async function capture(page: Page, root: string, name: string) {
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: resolve(root, name + ".png") });
  const data = await page.evaluate(() => {
    const rect = (el: Element | null) => { if (!el) return null; const b = el.getBoundingClientRect(); return { x: b.x, y: b.y, width: b.width, height: b.height, bottom: b.bottom }; };
    const prompt = document.querySelector<HTMLTextAreaElement>("#chat-prompt");
    const fixed: string[] = [], values: string[] = []; const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode()) { const el = node.parentElement; if (!el || !node.textContent?.trim() || el.closest("script,style,[hidden]") || !el.getClientRects().length || getComputedStyle(el).visibility === "hidden") continue; (el.closest("[data-ui-data]") ? values : fixed).push(node.textContent.trim()); }
    // canvas将浏览器支持的CSS色值（含oklch）转成实际sRGB像素。
    const canvas = document.createElement("canvas"), ctx = canvas.getContext("2d")!;
    const rgb = (color: string) => { ctx.clearRect(0,0,1,1); ctx.fillStyle = color; ctx.fillRect(0,0,1,1); return [...ctx.getImageData(0,0,1,1).data]; };
    const background = (el: Element | null): number[] => { if (!el) return [255,255,255,255]; const c = rgb(getComputedStyle(el).backgroundColor), b = background(el.parentElement), a = c[3]! / 255; return [0,1,2].map(i => c[i]! * a + b[i]! * (1-a)).concat(255); };
    const luminance = (c: number[]) => c.slice(0,3).map(v => { const n=v/255; return n<=.04045 ? n/12.92 : ((n+.055)/1.055)**2.4; }).reduce((a,v,i) => a+v*[.2126,.7152,.0722][i]!,0);
    const ratio = (a: number[], b: number[]) => { const x=luminance(a), y=luminance(b); return (Math.max(x,y)+.05)/(Math.min(x,y)+.05); };
    const contrast = [...document.querySelectorAll<HTMLElement>(".chat-workbench button,.chat-toolbar .toolbar-title h1,.chat-turn [role=status],.chat-turn-meta,.chat-answer,.chat-user-text,.chat-pending,.chat-notice")].filter(el => el.getClientRects().length && !el.closest("[hidden]") && getComputedStyle(el).visibility !== "hidden" && !el.matches(":disabled")).map(el => ({tag:el.tagName,text:el.innerText.slice(0,80),color:getComputedStyle(el).color,background:background(el),ratio:ratio(rgb(getComputedStyle(el).color),background(el))}));
    const targets = [...document.querySelectorAll<HTMLButtonElement>(".chat-workbench button")].filter(el => el.getClientRects().length && !el.disabled && !el.closest("[hidden]")).map(el => ({text:el.innerText,className:el.className,rect:rect(el)}));
    const active = document.activeElement as HTMLElement;
    const focusStyle = active ? getComputedStyle(active) : null;
    const focusEvidence = active?.matches(".chat-workbench :focus-visible") ? {width:parseFloat(focusStyle!.outlineWidth),contrast:ratio(rgb(focusStyle!.outlineColor),background(active.parentElement))} : null;
    return { contrast, targets, focusEvidence, viewport: { width: innerWidth, height: innerHeight }, overflow: document.documentElement.scrollWidth > innerWidth, prompt: prompt ? { value_length: prompt.value.length, clientHeight: prompt.clientHeight, scrollHeight: prompt.scrollHeight, scrollTop: prompt.scrollTop, rect: rect(prompt) } : null, heading: rect(document.querySelector(".chat-toolbar .toolbar-title h1")), status: document.querySelector(".chat-turn:last-of-type [role=status]")?.textContent, stop: rect(document.querySelector("#chat-stop")), find: [...document.querySelectorAll(".chat-pending button")].map(rect), focus: { tag: active?.tagName, id: active?.id, label: active?.getAttribute("aria-label"), outline: active ? getComputedStyle(active).outlineColor : null }, fixed, data: values, visible_text: document.body.innerText };
  });
  await writeFile(resolve(root, name + ".json"), JSON.stringify(data, null, 2));
  expect(data.overflow).toBe(false);
  for (const item of data.contrast) expect(item.ratio, JSON.stringify(item)).toBeGreaterThanOrEqual(4.5);
  const mobile = data.viewport.width <= 700;
  for (const item of data.targets) {
    const expectedSize = item.className.includes("message-copy-code") ? (mobile ? 40 : 24)
      : item.className.includes("chat-send") ? (mobile ? 40 : 28)
      : item.className.includes("chat-message-action") ? (mobile ? 40 : 30)
      : item.className.includes("chat-jump") ? 32 : null;
    if (expectedSize !== null) {
      expect(item.rect!.height, item.text).toBeGreaterThanOrEqual(expectedSize - 1);
      expect(item.rect!.height, item.text).toBeLessThanOrEqual(expectedSize + 1);
      expect(item.rect!.width, item.text).toBeGreaterThanOrEqual(expectedSize - 1);
      expect(item.rect!.width, item.text).toBeLessThanOrEqual(expectedSize + 1);
    } else {
      const minimumSize = mobile ? 40 : 24;
      expect(item.rect!.height, item.text).toBeGreaterThanOrEqual(minimumSize);
      expect(item.rect!.width, item.text).toBeGreaterThanOrEqual(minimumSize);
    }
  }
  if (data.focusEvidence) { expect(data.focusEvidence.width).toBeGreaterThanOrEqual(2); expect(data.focusEvidence.contrast).toBeGreaterThanOrEqual(3); }
  if (data.heading) expect(data.heading.bottom).toBeLessThanOrEqual(data.viewport.height); if (data.stop) expect(data.stop.bottom).toBeLessThanOrEqual(data.viewport.height); for (const item of data.find) if (item) expect(item.bottom).toBeLessThanOrEqual(data.viewport.height);
  await writeFile(resolve(root, name + ".json"), JSON.stringify(data, null, 2));
}

for (const viewport of [{ width: 1280, height: 900, name: "desktop" }, { width: 390, height: 844, name: "mobile" }]) {
  test(`同安装包Chat ${viewport.name}全链/未知/凭据代次/资源`, async ({ page, context }) => {
    test.setTimeout(90000); await page.setViewportSize(viewport);
    const root = resolve(EVIDENCE, viewport.name); await mkdir(root, { recursive: true });
    const database = resolve(root, "browser.sqlite"), token1 = "synthetic-generation-one", token2 = "synthetic-generation-two";
    const owners: Awaited<ReturnType<typeof startServer>>[] = [], gates: (() => void)[] = [], consoleRecords: unknown[] = [], pageErrors: string[] = [], posts: { path: string; body: Record<string, unknown> }[] = [];
    page.on("console", msg => { if (msg.type() === "error" || msg.type() === "warning") consoleRecords.push({ type: msg.type(), text: msg.text() }); }); page.on("pageerror", error => pageErrors.push(String(error)));
    page.on("request", req => { const path = new URL(req.url()).pathname; if (req.method() === "POST" && path.startsWith("/v2/")) posts.push({ path, body: req.postDataJSON() as Record<string, unknown> }); });
    try {
      const one = await startServer(resolve(root, "server-generation-1"), database, 1, token1); owners.push(one); const url = one.ready!.url;
      await page.goto(url + "/ui/?view=chat");
      const account = page.getByRole("button", { name: "密钥身份", exact: true });
      const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
      const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
      const isMobileNav = await mobileMenu.isVisible();
      if (isMobileNav) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
      await account.click(); await capture(page, root, "connection"); await account.focus(); await page.keyboard.press("Escape"); await expect(page.getByRole("dialog")).toHaveCount(0);
      // 桌面端焦点随 Escape 回到入口按钮；移动端抽屉随同一 Escape 关闭、按钮卸载。
      if (!isMobileNav) await expect(account).toBeFocused();
      await connect(page, token1); await capture(page, root, "default"); expect(posts).toHaveLength(0);
      const a = await createSession(page, "会话 A");
      // 首次POST以真实错误Bearer抵达后端：零Turn/模型进入，保留草稿后显式纠正提交。
      const refused = barrier(); const firstPath = `**/v2/workspaces/local/sessions/${a}/turns`;
      await page.route(firstPath, async route => {
        if (route.request().method() !== "POST") return route.continue();
        const response = await route.fetch({ headers: { ...route.request().headers(), authorization: "Bearer intentionally-invalid-synthetic" } });
        expect(response.status()).toBe(401); const body = await response.body();
        await writeFile(resolve(root, "first-post-401.json"), JSON.stringify({ submitted:route.request().postDataJSON(),status:response.status(),body:JSON.parse(body.toString()) },null,2));
        await route.fulfill({ response, body }); refused.release();
      });
      await send(page, "请记住基数17"); await bounded(refused.promise); await page.unroute(firstPath);
      await expect(page.locator(".chat-notice")).toContainText("密钥无效"); await expect(page.locator("#chat-prompt")).toHaveValue("请记住基数17");
      expect(await page.evaluate(key => JSON.parse(localStorage.getItem(key) ?? "[]").length,INTENTS)).toBe(0);
      const empty = await page.request.get(url + `/v2/workspaces/local/sessions/${a}/turns`, {headers:{Authorization:"Bearer "+token1}}); expect(await empty.json()).toEqual([]);
      expect((await waitFile(resolve(root,"server-generation-1/model-counts.json"))).requests).toHaveLength(0);
      await page.locator(".chat-composer button[type=submit]").click(); await answer(page, "请记住基数17", token1); await send(page, "与3相加"); await answer(page, "20", token1); await bottom(page); await capture(page, root, "complete");
      const last = page.locator(".chat-turn").last(); const tool = last.locator(".chat-tool").first(); await expect(tool.locator("summary")).toBeVisible(); await expect(tool.locator("summary strong")).not.toBeEmpty(); await tool.locator("summary").click(); await expect(tool.locator(".chat-tool-section pre").first()).toBeVisible(); await last.getByRole("button", { name: "更多操作", exact: true }).click(); await expect(last.locator(".chat-detail")).toContainText("Trace 记录完整"); await page.getByRole("button", { name: "查看 Trace", exact: true }).click(); await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("Trace"); await expect(page.locator(".trace-span-node").first()).toBeVisible(); await expect(page.locator(".trace-detail-heading h2")).toHaveText("会话 A"); await capture(page, root, "trace"); await page.getByRole("button", { name: "返回对话", exact: true }).click(); await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("会话 A"); await last.getByRole("button", { name: "更多操作", exact: true }).click();
      await navigateView(page, "run"); await navigateView(page, "chat"); await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("会话 A"); expect(posts.filter(p => p.path.endsWith("/turns"))).toHaveLength(3);
      await send(page, "/hold"); await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText("进行中"); await send(page, "/add 8 1"); await expect(page.locator(".chat-turn").last()).toContainText("排队中"); await bottom(page); await capture(page, root, "running-queued"); await page.locator("#chat-stop").click(); await answer(page, "9", token1);
      await send(page, "/note welcome"); await expect(page.locator(".chat-approval")).toBeVisible(); await bottom(page); await capture(page, root, "approval"); await page.getByRole("button", { name: "允许 lookup_note 本次调用" }).click(); await expectTurnStatus(page, token1, "completed");
      await send(page, "/fail"); await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText("失败"); await bottom(page); await expect(page.locator(".chat-turn").last()).toContainText("provider_http_error"); await capture(page, root, "failure");
      const b = await createSession(page, "会话 B"); await selectSession(page, "会话 A"); await expect(page.locator("#chat-prompt")).toBeEnabled();
      const postGate = barrier(), postDelivered = barrier(); gates.push(postGate.release); let accepted: { id: string; request_key: string } | null = null;
      await page.route(`**/v2/workspaces/local/sessions/${a}/turns`, async route => { if (route.request().method() !== "POST") return route.continue(); const response = await route.fetch(); const body = await response.body(); accepted = JSON.parse(body.toString()) as typeof accepted; await postGate.promise; await route.fulfill({ response, body }); postDelivered.release(); });
      await send(page, "/add 2 3"); await expect.poll(() => accepted).not.toBeNull(); await selectSession(page, "会话 B"); await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("会话 B");
      const original = await page.evaluate(key => JSON.parse(localStorage.getItem(key) ?? "[]")[0], INTENTS);
      postGate.release(); await bounded(postDelivered.promise); await page.unroute(`**/v2/workspaces/local/sessions/${a}/turns`); await expect.poll(() => page.evaluate(key => JSON.parse(localStorage.getItem(key) ?? "[]").length, INTENTS)).toBe(1); expect(new URL(page.url()).searchParams.get("session")).toBe(b); await expect(page.locator(".chat-pending strong")).toHaveText("提交结果未知"); await capture(page, root, "unknown-create");
      const getGate = barrier(), getDelivered = barrier(); gates.push(getGate.release); let getAccepted = false, attempts = 0;
      await page.route(`**/turns/by-key/${original.body.request_key}`, async route => { attempts++; if (attempts === 1) { const response = await route.fetch({ headers: { ...route.request().headers(), authorization: "Bearer intentionally-invalid-synthetic" } }); expect(response.status()).toBe(401); return route.fulfill({ response, body: await response.body() }); } const response = await route.fetch(); const body = await response.body(); getAccepted = true; await getGate.promise; await route.fulfill({ response, body }); getDelivered.release(); });
      await page.getByRole("button", { name: "查找原请求", exact: true }).click(); await expect(page.locator(".chat-notice")).toContainText("密钥无效"); expect(await page.evaluate(key => JSON.parse(localStorage.getItem(key)!)[0], INTENTS)).toEqual(original);
      await page.getByRole("button", { name: "查找原请求", exact: true }).click(); await expect.poll(() => getAccepted).toBe(true);
      await expect.poll(async () => { const response = await page.request.get(url + `/v2/workspaces/local/sessions/${a}/turns/by-key/${original.body.request_key}`, { headers: { Authorization: "Bearer " + token1 } }); return (await response.json()).status; }).toBe("completed");
      await one.close(); const two = await startServer(resolve(root, "server-generation-2"), database, 2, token2, one.ready!.api_port); owners.push(two);
      await connect(page, token2); getGate.release(); await bounded(getDelivered.promise); await page.unroute(`**/turns/by-key/${original.body.request_key}`);
      await expect(page.getByRole("button", { name: "查找原请求", exact: true })).toBeVisible(); expect(await page.evaluate(key => JSON.parse(localStorage.getItem(key)!)[0], INTENTS)).toEqual(original);
      expect(await page.evaluate(({ key, id }) => JSON.parse(localStorage.getItem(key) ?? "{}")[id], { key: KNOWN, id: original.id })).toBeUndefined();
      await page.getByRole("button", { name: "查找原请求", exact: true }).click(); await expect.poll(() => page.evaluate(key => JSON.parse(localStorage.getItem(key) ?? "[]").length, INTENTS)).toBe(0);
      expect(new URL(page.url()).searchParams.get("turn")).toBe((accepted as unknown as { id: string }).id); expect(posts.filter(p => p.body.request_key === original.body.request_key)).toHaveLength(1);
      const afterGet = await waitFile(resolve(root, "server-generation-2/model-counts.json")); expect(afterGet.requests).toHaveLength(0);
      await createSession(page, "长标题".repeat(35)); const long = "LONG_PARAMETER_".repeat(400); await page.locator("#chat-prompt").fill(long);
      const geometry = await page.locator("#chat-prompt").evaluate(el => ({ client: el.clientHeight, scroll: el.scrollHeight, value: (el as HTMLTextAreaElement).value })); expect(geometry.value).toBe(long); expect(geometry.client).toBeLessThanOrEqual(320); expect(geometry.scroll).toBeGreaterThan(geometry.client);
      await page.locator("#chat-prompt").evaluate(el => { el.scrollTop = el.scrollHeight; (el as HTMLTextAreaElement).setSelectionRange((el as HTMLTextAreaElement).value.length, (el as HTMLTextAreaElement).value.length); }); await page.locator("#chat-prompt").focus(); await page.keyboard.press("Tab"); await expect(page.locator(".chat-composer button[type=submit]")).toBeFocused(); await page.keyboard.press("Shift+Tab"); await expect(page.locator("#chat-prompt")).toBeFocused(); await capture(page, root, "long-input");
      await page.locator(".chat-composer button[type=submit]").click(); await answer(page, "收到：" + long, token2); expect(await page.locator(".chat-turn").last().locator(".chat-user-text").textContent()).toBe(long);
      await writeFile(resolve(root, "network.json"), JSON.stringify({ posts, console: consoleRecords, pageErrors, original_intent: original, accepted, get_attempts: attempts }, null, 2)); expect(pageErrors).toEqual([]); expect(JSON.stringify(consoleRecords)).not.toMatch(/Content Security Policy|Refused to (apply|execute|load)/i);
      const book = await context.newPage(); const resources: string[] = []; book.on("request", req => { if (!req.url().startsWith("file:")) resources.push(req.url()); }); await book.goto("file://" + resolve(SOURCE, "docs/agent-core/GUIDE.html")); await expect(book.locator("h2")).toHaveCount(10); expect(await book.locator("svg").count()).toBe(3); expect(resources).toEqual([]); await book.setViewportSize(viewport); await book.screenshot({path:resolve(root,"agent.png")});
      const bookGeometry = await book.evaluate(() => ({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,chapters:document.querySelectorAll("h2").length,svg:document.querySelectorAll("svg").length,text:document.body.innerText.slice(0,1200)}));
      await writeFile(resolve(root,"agent.json"),JSON.stringify({geometry:bookGeometry,external_resources:resources},null,2)); expect(bookGeometry.scrollWidth).toBeLessThanOrEqual(viewport.width); await book.close();
    } finally {
      for (const release of gates) release();
      try { if (!page.isClosed()) await page.unrouteAll({ behavior: "wait" }); }
      finally {
        const results = await Promise.allSettled(owners.map(owner => owner.close()));
        for (const result of results) if (result.status === "rejected") throw result.reason;
      }
    }
  });
}

// 独立有限补充：真实echo工具的完整长参数/结果、局部横滚与键盘，不重跑旧Run/Eval页面。
for (const viewport of [{ width:1280,height:900,name:"desktop" },{ width:390,height:844,name:"mobile" }]) {
  test(`同安装包长工具局部横滚 ${viewport.name}`,async ({page}) => {
    await page.setViewportSize(viewport); const root=resolve(EVIDENCE,"tools-"+viewport.name); await mkdir(root,{recursive:true});
    const owner=await startServer(resolve(root,"server"),resolve(root,"browser.sqlite"),1,"synthetic-long-tools");
    try {
      await page.goto(owner.ready!.url+"/ui/?view=chat"); await connect(page,"synthetic-long-tools"); await createSession(page,"完整工具参数");
      // 6000字节仍完整提交，但受保护Provider参数上限4096应真实拒绝，不改上限迎合测试。
      const over="LONG_PARAMETER_".repeat(400); await send(page,"/echo "+over); await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText("失败");
      await expect(page.locator(".chat-turn").last()).toContainText("provider_tool_arguments");
      const refused=await waitFile(resolve(root,"server/model-counts.json")); expect(refused.requests).toHaveLength(1); expect(refused.executions).toHaveLength(0);
      expect(await page.locator(".chat-turn").last().locator(".chat-user-text").textContent()).toBe("/echo "+over);
      // 独立的4050字节有效工具参数低于JSON容量；仍足以产生真正局部横滚。
      const long="LONG_PARAMETER_".repeat(270),input="/echo "+long; await send(page,input); await answer(page,long,"synthetic-long-tools");
      const last=page.locator(".chat-turn").last();
      const tool=last.locator(".chat-tool").first(); await expect(tool.locator("summary")).toBeVisible(); await tool.locator("summary").click();
      const argument=tool.locator(".chat-tool-section pre").nth(0),result=tool.locator(".chat-tool-section pre").nth(1);
      await expect(argument).toBeVisible(); expect(JSON.parse((await argument.textContent())!)).toEqual({text:long}); expect(JSON.parse((await result.textContent())!)).toBe(long);
      const read=()=>argument.evaluate(el=>({client:el.clientWidth,scroll:el.scrollWidth,left:el.scrollLeft,text:el.textContent}));
      const before=await read(); expect(before.scroll).toBeGreaterThan(before.client);
      await argument.focus(); await expect(argument).toBeFocused(); await page.keyboard.press("ArrowRight"); await expect.poll(async ()=>(await read()).left).toBeGreaterThan(0);
      await argument.evaluate(el=>{el.scrollLeft=el.scrollWidth;}); const after=await read(); expect(after.left).toBeGreaterThan(0);
      expect(await page.locator("#chat-prompt").inputValue()).toBe(""); expect(await last.locator(".chat-user-text").textContent()).toBe(input);
      await writeFile(resolve(root,"tool-scroll.json"),JSON.stringify({before,after,full_input:input,keyboard_focus:true},null,2)); await capture(page,root,"tool-detail");
      const counts=await waitFile(resolve(root,"server/model-counts.json")); expect(counts.requests).toHaveLength(3); expect(counts.executions).toHaveLength(1); expect(counts.executions[0]).toEqual({name:"echo",arguments:{text:long}});
    } finally {await owner.close();}
  });
}

for (const viewport of [{width:1280,height:900,name:"desktop"},{width:390,height:844,name:"mobile"}]) {
  test(`repair 相邻长短会话标题 ${viewport.name}`,async ({page})=>{
    await page.setViewportSize(viewport);const root=resolve(EVIDENCE,viewport.name);await mkdir(root,{recursive:true});
    const owner=await startServer(resolve(root,"server"),resolve(root,"titles.sqlite"),1,"synthetic-repair-title");
    try {
      await page.goto(owner.ready!.url+"/ui/?view=chat");await connect(page,"synthetic-repair-title");
      const entries=[{title:"短标题 A",id:""},{title:"长标题".repeat(35),id:""},{title:"短标题 B",id:""}];
      for(const value of entries)value.id=await createSession(page,value.title);
      await expect(page.getByRole("dialog")).toHaveCount(0);
      await page.evaluate(async()=>{await Promise.all(document.getAnimations().map(a=>a.finished.catch(()=>{})));window.scrollTo(0,0);});
      await page.screenshot({path:resolve(root,"adjacent-titles.png")});
      const geometry=await page.locator(".chat-session-list button.session-select").evaluateAll(elements=>elements.map(el=>{
        const b=el.getBoundingClientRect(),span=el.querySelector("span")!;const range=document.createRange();range.selectNodeContents(span);
        return {title:span.textContent,button:{x:b.x,y:b.y,right:b.right,bottom:b.bottom,width:b.width,height:b.height},text:[...range.getClientRects()].map(r=>({x:r.x,y:r.y,right:r.right,bottom:r.bottom})),focusable:(el as HTMLButtonElement).tabIndex};
      }));
      await writeFile(resolve(root,"geometry.json"),JSON.stringify({viewport,geometry},null,2));
      expect(geometry).toHaveLength(3);
      for(const item of geometry){expect(item.button.height).toBeGreaterThanOrEqual(44);expect(item.button.width).toBeGreaterThanOrEqual(44);for(const line of item.text){expect(line.y,item.title!).toBeGreaterThanOrEqual(item.button.y-1);expect(line.bottom,item.title!).toBeLessThanOrEqual(item.button.bottom+1);expect(line.x,item.title!).toBeGreaterThanOrEqual(item.button.x-1);expect(line.right,item.title!).toBeLessThanOrEqual(item.button.right+1);}}
      const selections=[];
      for(const value of entries){
        await openDrawerIfMobile(page);
        const button=page.getByRole("button",{name:value.title,exact:true});
        await button.focus();await expect(button).toBeFocused();
        // 键盘移动焦点后再读取描边证据，保持 :focus-visible 断言与桌面一致。
        await page.keyboard.press("Shift+Tab");await page.keyboard.press("Tab");await expect(button).toBeFocused();
        const focus=await button.evaluate(el=>({visible:el.matches(":focus-visible"),width:getComputedStyle(el).outlineWidth,color:getComputedStyle(el).outlineColor}));expect(focus.visible).toBe(true);expect(parseFloat(focus.width)).toBeGreaterThanOrEqual(2);selections.push({...value,method:"keyboard Enter",focus});
        await page.keyboard.press("Enter");await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText(value.title);expect(new URL(page.url()).searchParams.get("session")).toBe(value.id);
        await openDrawerIfMobile(page);
        await page.getByRole("button",{name:value.title,exact:true}).click();expect(new URL(page.url()).searchParams.get("session")).toBe(value.id);
      }
      const long=entries[1]!;await openDrawerIfMobile(page);await page.getByRole("button",{name:long.title,exact:true}).click();await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText(long.title);
      const saved=await page.request.get(owner.ready!.url+`/v2/workspaces/local/sessions/${long.id}`,{headers:{Authorization:"Bearer synthetic-repair-title"}});expect((await saved.json()).title).toBe(long.title);
      await page.evaluate(async()=>{await Promise.all(document.getAnimations().map(a=>a.finished.catch(()=>{})));window.scrollTo(0,0);});await capture(page,root,"long-selected");
      await writeFile(resolve(root,"selections.json"),JSON.stringify({selections,full_title:long.title},null,2));
      const book=await page.context().newPage();const external:string[]=[];book.on("request",r=>{if(!r.url().startsWith("file:"))external.push(r.url());});
      await book.setViewportSize(viewport);await book.goto("file://"+resolve(SOURCE,"docs/agent-core/GUIDE.html"));await expect(book.locator("h2")).toHaveCount(10);expect(await book.locator("svg").count()).toBe(3);expect(external).toEqual([]);
      await book.screenshot({path:resolve(root,"agent.png")});const bookGeometry=await book.evaluate(()=>({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,chapters:document.querySelectorAll("h2").length}));expect(bookGeometry.scrollWidth).toBeLessThanOrEqual(viewport.width);await writeFile(resolve(root,"agent.json"),JSON.stringify({geometry:bookGeometry,external},null,2));await book.close();
    } finally {await owner.close();}
  });
}
