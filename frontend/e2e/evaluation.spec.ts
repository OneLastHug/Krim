import { test, expect, type Page, type APIRequestContext } from "@playwright/test";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
const evidence = process.env.KRIM_GUIDE_EVIDENCE;
if (!evidence)
    throw new Error("需要任务自有证据目录");
mkdirSync(evidence, { recursive: true });
const file = (name: string) => resolve(evidence, name);
const KEY = "SYNTHETIC_M2_BROWSER_KEY", headers = { Authorization: "Bearer " + KEY };
const entries = () => existsSync(file("entries.jsonl")) ? readFileSync(file("entries.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
function sql(query: string, ...values: (string | number)[]) {
    const db = new DatabaseSync(file("browser.sqlite"), { readOnly: true });
    try {
        return db.prepare(query).all(...values);
    }
    finally {
        db.close();
    }
}
function save(name: string, value: unknown) { writeFileSync(file(name + ".json"), JSON.stringify(value, null, 2)); }
function deferred() { let release!: () => void; const promise = new Promise<void>(r => release = r); return { promise, release }; }
async function connect(page: Page, key = KEY) { await page.getByRole("button", { name: "连接", exact: true }).click(); await page.locator("#api-key").fill(key); await page.getByRole("button", { name: "应用", exact: true }).click(); await expect(page.getByRole("dialog")).toHaveCount(0); if (key === KEY)
    await expect(page.locator(".evaluation-results>.notice").filter({ hasText: /^密钥无效$/ })).toHaveCount(0); }
async function begin(page: Page) { await page.goto("/ui/?view=eval"); await connect(page); }
async function add(page: Page, id: string, input: string, expected: string) { await page.locator("#case-id").fill(id); await page.locator("#case-input").fill(input); await page.locator("#case-expected").fill(JSON.stringify(expected)); await page.locator("#case-reason").fill("人工核算的合成参考"); await page.getByRole("button", { name: "加入样本", exact: true }).click(); await expect(page.locator("#case-list")).toContainText(id); }
async function freeze(page: Page, name: string) { await page.locator("#dataset-name").fill(name); await page.locator("#freeze").click(); await expect(page.locator("#dataset-fingerprint")).toHaveText(/^[a-f0-9]{64}$/); return (await page.locator("#dataset-fingerprint").textContent())!; }
async function openEval(page: Page, id: string) { await page.locator("#open-eval-id").fill(id); await page.getByRole("button", { name: "打开评测", exact: true }).click(); await expect(page.locator("#eval-id")).toHaveText(id); await expect(page.locator("#eval-status")).toHaveText("已完成"); await expect(page.locator("#eval-items tbody tr")).not.toHaveCount(0); }
async function execute(page: Page, target: string) { const previous = await page.locator("#eval-id").count() ? await page.locator("#eval-id").textContent() : null; await page.locator("#eval-target").selectOption(target); await page.locator("#eval-start").click(); await expect.poll(async () => await page.locator("#eval-id").count() ? page.locator("#eval-id").textContent() : null).not.toBe(previous); await expect(page.locator("#eval-status")).toHaveText("已完成"); return (await page.locator("#eval-id").textContent())!; }
async function compare(page: Page, a: string, b: string, status: string) { await page.locator("#baseline-id").fill(a); await page.locator("#candidate-id").fill(b); await expect(page.locator("#gate-status")).toHaveCount(0); await page.locator("#compare").click(); await expect(page.locator("#gate-status")).toHaveText(status); }
async function report(request: APIRequestContext, id: string) { const response = await request.get(`/v1/evals/${id}/report`, { headers }); expect(response.status()).toBe(200); return response.json(); }
async function shot(page: Page, name: string) {
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    save(name + "-text", await page.evaluate(() => {
        const data = [...document.querySelectorAll<HTMLElement>("[data-ui-data]")].filter(e => e.getClientRects().length).map(e => e.innerText);
        const fixed: string[] = [], walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        let node: Node | null;
        while ((node = walker.nextNode())) {
            if (!node.textContent?.trim() || node.parentElement?.closest("[data-ui-data],script,style"))
                continue;
            const range = document.createRange();
            range.selectNodeContents(node);
            if ([...range.getClientRects()].some(r => r.width > 0 && r.height > 0))
                fixed.push(node.textContent.trim());
        }
        const inputs = [...document.querySelectorAll<HTMLInputElement>("input,textarea")].filter(e => e.getClientRects().length && e.type !== "password").map(e => ({ id: e.id, value: e.value }));
        return { visible: document.body.innerText, fixed, data, inputs };
    }));
    await page.screenshot({ path: file(name + ".png"), fullPage: true });
}
async function accessibility(page: Page, name: string) {
    const checks = await page.evaluate(() => {
        const visible = (e: Element) => e.getClientRects().length > 0;
        const unlabeled = [...document.querySelectorAll<HTMLInputElement>(".evaluation-workbench input:not([type=checkbox]),.evaluation-workbench textarea,.evaluation-workbench select")].filter(visible).filter(e => !document.querySelector(`label[for="${e.id}"]`)).map(e => e.id);
        const targets = [...document.querySelectorAll<HTMLElement>(".evaluation-workbench button,.evaluation-workbench summary,.rule-options label")].filter(visible).map(e => ({ label: e.innerText || e.getAttribute("aria-label"), width: e.getBoundingClientRect().width, height: e.getBoundingClientRect().height }));
        return { unlabeled, targets, overflow: document.documentElement.scrollWidth > innerWidth };
    });
    expect(checks.unlabeled).toEqual([]);
    expect(checks.overflow).toBe(false);
    expect(checks.targets.every(t => t.height >= 43 && t.width >= 43)).toBe(true);
    await page.locator("#case-expected").focus();
    await page.keyboard.press("Tab");
    expect(await page.locator("#case-status").evaluate(e => e === document.activeElement)).toBe(true);
    await page.keyboard.press("Shift+Tab");
    expect(await page.locator("#case-expected").evaluate(e => e === document.activeElement)).toBe(true);
    const focus = await page.locator("#case-expected").evaluate(e => { const c = getComputedStyle(e); return { outline: c.outlineStyle, width: c.outlineWidth, color: c.outlineColor, boxShadow: c.boxShadow }; });
    expect(focus.outline !== "none" || focus.boxShadow !== "none").toBe(true);
    await page.getByRole("button", { name: "连接", exact: true }).click();
    await expect(page.locator("#api-key")).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("button", { name: "连接", exact: true })).toBeFocused();
    await page.getByRole("tab", { name: "评测", exact: true }).focus();
    await page.keyboard.press("ArrowLeft");
    await expect(page.getByRole("tab", { name: "任务", exact: true })).toBeFocused();
    await page.getByRole("tab", { name: "评测", exact: true }).click();
    const contrasts = await page.evaluate(() => {
        const canvas = document.createElement("canvas");
        canvas.width = canvas.height = 1;
        const ctx = canvas.getContext("2d")!;
        function rgb(color: string) { ctx.clearRect(0, 0, 1, 1); ctx.fillStyle = color; ctx.fillRect(0, 0, 1, 1); return [...ctx.getImageData(0, 0, 1, 1).data].slice(0, 3); }
        function lum(rgb: number[]) { const v = rgb.map(n => n / 255).map(n => n <= .04045 ? n / 12.92 : ((n + .055) / 1.055) ** 2.4); return v[0]! * .2126 + v[1]! * .7152 + v[2]! * .0722; }
        function ratio(a: string, b: string) { const x = lum(rgb(a)), y = lum(rgb(b)); return (Math.max(x, y) + .05) / (Math.min(x, y) + .05); }
        return [...document.querySelectorAll<HTMLElement>(".evaluation-workbench h2,.evaluation-workbench label,.evaluation-workbench p,.evaluation-workbench button")].filter(e => e.getClientRects().length && !e.matches(":disabled")).map(e => { let parent: HTMLElement | null = e; while (parent && getComputedStyle(parent).backgroundColor === "rgba(0, 0, 0, 0)")
            parent = parent.parentElement; const background = parent ? getComputedStyle(parent).backgroundColor : "white"; return { text: e.innerText, color: getComputedStyle(e).color, background, ratio: ratio(getComputedStyle(e).color, background) }; });
    });
    expect(contrasts.every(c => c.ratio >= 4.5)).toBe(true);
    const long = '"' + "LONG_REFERENCE_".repeat(200) + '"';
    await page.locator("#case-expected").fill(long);
    await page.locator("#case-expected").focus();
    await page.keyboard.press("Control+End");
    const geometry = await page.locator("#case-expected").evaluate(e => { const t = e as HTMLTextAreaElement; return { height: t.clientHeight, scrollHeight: t.scrollHeight, scrollTop: t.scrollTop, length: t.value.length, end: t.selectionEnd }; });
    expect(geometry.height).toBeLessThanOrEqual(Math.min(240, viewportHeight(page) * .3));
    expect(geometry.scrollHeight).toBeGreaterThan(geometry.height);
    expect(geometry.scrollTop).toBeGreaterThan(0);
    expect(geometry.length).toBe(long.length);
    expect(geometry.end).toBe(long.length);
    await page.keyboard.press("Tab");
    await expect(page.locator("#case-status")).toBeFocused();
    await page.locator("#case-expected").fill('"3"');
    save(name + "-accessibility", { ...checks, focus, contrasts, geometry });
}
function viewportHeight(page: Page) { return page.viewportSize()!.height; }
for (const viewport of [{ name: "desktop", width: 1280, height: 900 }, { name: "narrow", width: 360, height: 800 }]) {
    test.describe(viewport.name, () => {
        test.use({ viewport: { width: viewport.width, height: viewport.height } });
        let messages: string[] = [];
        test.beforeEach(({ page }) => { messages = []; page.on("console", m => { if (m.type() === "error")
            messages.push(m.text()); }); page.on("pageerror", e => messages.push("PAGE_ERROR: " + e.message)); });
        test.afterEach(({}, info) => { save(viewport.name + "-console-" + createHash("sha256").update(info.title).digest("hex").slice(0, 10), messages); expect(messages.filter(m => /Content Security Policy|Refused to|PAGE_ERROR:/.test(m))).toEqual([]); });
        if (process.env.KRIM_GUIDE_PHASE === "eval-restart") {
            test("重启后只读报告与零重放", async ({ page }) => {
                const original = JSON.parse(readFileSync(file(viewport.name + "-identity.json"), "utf8"));
                const before = entries().filter(e => ["provider_entry", "tool_entry", "scorer_entry"].includes(e.kind)).length;
                const posts: string[] = [];
                page.on("request", r => {
                    if (r.method() === "POST")
                        posts.push(r.url());
                });
                await page.goto(`/ui/?view=eval&eval_id=${original.v2}`);
                await connect(page);
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                await expect(page.locator("#eval-id")).toHaveText(original.v2);
                await shot(page, viewport.name + "-restart-readonly");
                expect(posts).toEqual([]);
                expect(entries().filter(e => ["provider_entry", "tool_entry", "scorer_entry"].includes(e.kind)).length).toBe(before);
            });
        }
        else {
            test("来源冻结真实版本与比较", async ({ page, request }) => {
                test.setTimeout(60000);
                await page.goto("/ui/");
                await connect(page);
                await page.locator("#prompt").fill("/add 2 3");
                await page.locator("#send").click();
                await expect(page.locator("#result")).toHaveText("5");
                const source = (await page.locator("#run-id").textContent())!;
                await page.getByRole("button", { name: "选作样本", exact: true }).click();
                await expect(page.locator("#source-eligibility")).toHaveText("可选入");
                expect(await page.locator("#case-expected").inputValue()).toBe("");
                await page.getByRole("button", { name: "使用输入", exact: true }).click();
                await expect(page.locator("#case-input")).toHaveValue("/add 2 3");
                expect(await page.locator("#case-expected").inputValue()).toBe("");
                await shot(page, viewport.name + "-source-reference");
                await page.locator("#case-id").fill("integer");
                await page.locator("#case-expected").fill('"5"');
                await page.locator("#case-reason").fill("人工核算2加3");
                await page.getByRole("button", { name: "加入样本", exact: true }).click();
                await page.getByRole("button", { name: "改为手工样本", exact: true }).click();
                await add(page, "fraction_a", "/add 2.5 0.5", "3");
                await add(page, "fraction_b", "/add 1.5 1.5", "3");
                const name = viewport.name + "-versions", fingerprint = await freeze(page, name);
                await shot(page, viewport.name + "-frozen");
                await accessibility(page, viewport.name);
                const before = entries().length, v1 = await execute(page, "target-v1"), v2 = await execute(page, "target-v2"), bad = await execute(page, "target-bad");
                const reports = await Promise.all([v1, v2, bad].map(id => report(request, id)));
                for (const r of reports) {
                    expect(r.complete).toBe(true);
                    expect(r.summary.token_usage).toBeNull();
                    expect(r.items).toHaveLength(3);
                    expect(new Set(r.items.map((i: any) => i.run_id)).size).toBe(3);
                    expect(r.items.every((i: any) => i.run_id !== source && i.output.model_steps === 2 && i.output.tools[0].executor_started === true)).toBe(true);
                }
                const observed = entries().slice(before);
                for (const target of ["target-v1", "target-v2", "target-bad"]) {
                    expect(observed.filter(e => e.kind === "provider_entry" && e.target === target)).toHaveLength(6);
                }
                expect(observed.filter(e => e.kind === "tool_entry")).toHaveLength(9);
                await openEval(page, v2);
                await shot(page, viewport.name + "-report");
                await compare(page, v1, v2, "通过");
                await shot(page, viewport.name + "-gate-pass");
                await compare(page, v1, bad, "退化");
                await shot(page, viewport.name + "-gate-fail");
                // 真实GET延迟，A→B→A；最初A的回程不能覆盖再次打开A。
                await openEval(page, bad);
                const held = deferred(), fetched = deferred(), delivered = deferred();
                let first = true;
                await page.route(`**/v1/evals/${v2}`, async (route) => {
                    if (!first) {
                        await route.continue();
                        return;
                    }
                    first = false;
                    const response = await route.fetch();
                    fetched.release();
                    await held.promise;
                    try {
                        await route.fulfill({ response });
                    }
                    catch (error) {
                        if (route.request().failure()?.errorText !== "net::ERR_ABORTED")
                            throw error;
                    }
                    finally {
                        delivered.release();
                    }
                });
                await page.locator("#open-eval-id").fill(v2);
                await page.getByRole("button", { name: "打开评测", exact: true }).click();
                await fetched.promise;
                await openEval(page, bad);
                await openEval(page, v2);
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                held.release();
                await delivered.promise;
                await expect(page.locator("#eval-id")).toHaveText(v2);
                await expect(page.locator(".report-identity")).toContainText(reports[1].report_hash);
                await page.unroute(`**/v1/evals/${v2}`);
                await page.reload();
                await connect(page);
                await expect(page.locator("#eval-id")).toHaveText(v2);
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                expect(entries().filter(e => e.kind === "provider_entry" && e.target !== "demo").length).toBe(observed.filter(e => e.kind === "provider_entry").length + entries().slice(0, before).filter(e => e.kind === "provider_entry" && e.target !== "demo").length);
                // 新合成慢任务；实际Provider进入后停止，partial报告不可Gate pass。
                await add(page, "slow", "/slow", "5");
                await freeze(page, viewport.name + "-slow");
                await page.locator("#eval-start").click();
                await expect(page.locator("#eval-status")).toHaveText("执行中");
                const slow = (await page.locator("#eval-id").textContent())!;
                await expect.poll(() => sql("SELECT r.id FROM runs r JOIN eval_items i ON i.run_id=r.id WHERE i.eval_id=?", slow).length).toBe(1);
                const slowRun = String(sql("SELECT run_id FROM eval_items WHERE eval_id=?", slow)[0]!.run_id);
                await expect.poll(() => entries().some(e => e.kind === "provider_entry" && e.run_id === slowRun)).toBe(true);
                await shot(page, viewport.name + "-running");
                await page.locator("#eval-cancel").click();
                await expect(page.locator("#eval-status")).toHaveText("已停止");
                await expect.poll(async () => (await (await request.get("/healthz")).json()).active).toBe(0);
                await shot(page, viewport.name + "-partial");
                await compare(page, v1, v2, "通过");
                await page.locator("#candidate-id").fill(slow);
                await expect(page.locator("#gate-status")).toHaveCount(0);
                await page.locator("#compare").click();
                await expect(page.locator("#gate-status")).toHaveText("不可比较");
                await shot(page, viewport.name + "-gate-invalid");
                save(viewport.name + "-identity", { source, name, fingerprint, v1, v2, bad, slow, reports, observed });
            });
            test("冻结认证纠正", async ({ page }) => {
                await page.goto("/ui/?view=eval");
                await connect(page, "WRONG_SYNTHETIC_KEY");
                await add(page, "auth", "/add 2 3", "5");
                await page.locator("#dataset-name").fill(viewport.name + "-auth");
                const before = Number(sql("SELECT count(*) AS n FROM dataset_revisions")[0]!.n), entriesBefore = entries().filter(e => ["provider_entry", "tool_entry"].includes(e.kind)).length;
                const posts: string[] = [];
                page.on("request", r => {
                    if (r.method() === "POST")
                        posts.push(r.url());
                });
                const response = page.waitForResponse(r => r.url().endsWith("/v1/datasets") && r.request().method() === "POST");
                await page.locator("#freeze").click();
                const rejected = await response;
                expect(rejected.status()).toBe(401);
                expect((await rejected.json()).error.code).toBe("unauthorized");
                await expect(page.locator(".case-editor .notice")).toHaveText("密钥无效");
                expect(sql("SELECT count(*) AS n FROM dataset_revisions")[0]!.n).toBe(before);
                expect(await page.evaluate(() => localStorage.getItem("krim-agent-freeze-intent-v1"))).toBeNull();
                await expect(page.locator("#case-input")).toHaveValue("/add 2 3");
                await connect(page);
                expect(posts).toHaveLength(1);
                await page.locator("#freeze").dblclick();
                await expect(page.locator("#dataset-fingerprint")).toHaveText(/^[a-f0-9]{64}$/);
                expect(posts).toHaveLength(2);
                expect(sql("SELECT count(*) AS n FROM dataset_revisions WHERE name=?", viewport.name + "-auth")[0]!.n).toBe(1);
                expect(entries().filter(e => ["provider_entry", "tool_entry"].includes(e.kind)).length).toBe(entriesBefore);
                save(viewport.name + "-auth", { status: rejected.status(), before, afterDenied: before, after: before + 1, posts });
                // 合法小数参考走真实冻结与JSON评分，不按另一语言的浮点字节误锁定。
                await page.locator("#case-input").fill("/echo 1e-7");
                await page.locator("#case-expected").fill("1e-7");
                await page.getByRole("button", { name: "加入样本", exact: true }).click();
                await page.locator("#dataset-name").fill(viewport.name + "-numeric");
                const numericResponse = page.waitForResponse(r => r.url().endsWith("/v1/datasets") && r.request().method() === "POST");
                await page.locator("#freeze").click();
                const accepted = await numericResponse;
                expect(accepted.status()).toBe(200);
                const numeric = await accepted.json();
                await expect(page.locator("#dataset-fingerprint")).toHaveText(numeric.fingerprint);
                expect(await page.evaluate(() => localStorage.getItem("krim-agent-freeze-intent-v1"))).toBeNull();
                await page.locator("#rule-exact_match").uncheck();
                await page.locator("#rule-json_match").check();
                await execute(page, "target-v2");
                await expect(page.locator("#eval-summary")).toContainText("通过 1/1");
                await shot(page, viewport.name + "-numeric-reference");
            });
            test("未知回程与迟到读取", async ({ page, request }) => {
                await begin(page);
                await add(page, "unknown", "/add 2 3", "5");
                const name = viewport.name + "-unknown";
                await page.locator("#dataset-name").fill(name);
                let freezePosts = 0, lookup = 0;
                await page.route("**/v1/datasets", async (route) => { freezePosts++; await route.fetch(); await route.abort("failed"); });
                await page.route(`**/v1/datasets/${name}/1`, async (route) => {
                    lookup++;
                    if (lookup === 1)
                        await route.abort("failed");
                    else
                        await route.continue();
                });
                await page.locator("#freeze").click();
                await expect(page.locator(".case-editor .notice")).toHaveText("冻结结果未确认");
                await shot(page, viewport.name + "-unknown-freeze");
                const original = await page.evaluate(() => JSON.parse(localStorage.getItem("krim-agent-freeze-intent-v1")!));
                await page.locator("#dataset-name").fill(name + "-edited");
                await page.locator("#case-input").fill("new draft");
                await page.locator("#freeze").click();
                await expect(page.locator(".case-editor .notice")).toHaveText("原提交已确认");
                expect(freezePosts).toBe(1);
                await expect(page.locator("#dataset-fingerprint")).toHaveCount(0);
                await expect(page.locator("#case-input")).toHaveValue("new draft");
                await expect(page.locator("#dataset-name")).toHaveValue(name + "-edited");
                expect(sql("SELECT count(*) AS n FROM dataset_revisions WHERE name=?", name)[0]!.n).toBe(1);
                expect(sql("SELECT count(*) AS n FROM dataset_revisions WHERE name=?", name + "-edited")[0]!.n).toBe(0);
                await page.unroute("**/v1/datasets");
                await page.unroute(`**/v1/datasets/${name}/1`);
                await page.locator("#dataset-name").fill(name);
                await page.getByRole("button", { name: "打开修订", exact: true }).click();
                await expect(page.locator("#dataset-fingerprint")).toHaveText(/^[a-f0-9]{64}$/);
                let evalPosts = 0, evalLookups = 0, accepted = "", key = "";
                await page.route("**/v1/evals", async (route) => { const body = route.request().postDataJSON(); key = body.request_key; const response = await route.fetch(); accepted = (await response.json()).id; evalPosts++; await route.abort("failed"); });
                await page.route("**/v1/evals/by-key/*", async (route) => {
                    evalLookups++;
                    if (evalLookups === 1)
                        await route.abort("failed");
                    else
                        await route.continue();
                });
                await page.locator("#eval-start").click();
                await expect(page.locator(".evaluation-results>.notice")).toHaveText("评测创建结果未确认");
                await shot(page, viewport.name + "-unknown-eval");
                expect(await page.evaluate(() => localStorage.getItem("krim-agent-eval-intent-v1"))).not.toBeNull();
                await page.reload();
                await connect(page);
                await page.getByRole("button", { name: "查找评测", exact: true }).click();
                await expect(page.locator("#eval-id")).toHaveText(accepted);
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                expect(evalPosts).toBe(1);
                expect(sql("SELECT count(*) AS n FROM evaluations WHERE request_key=?", key)[0]!.n).toBe(1);
                const r = await report(request, accepted), run = r.items[0].run_id;
                expect(sql("SELECT count(*) AS n FROM runs WHERE id=?", run)[0]!.n).toBe(1);
                expect(entries().filter(e => e.kind === "provider_entry" && e.run_id === run)).toHaveLength(2);
                expect(entries().filter(e => e.kind === "tool_entry" && e.run_id === run)).toHaveLength(1);
                save(viewport.name + "-unknown", { original, freezePosts, lookup, accepted, key, evalPosts, evalLookups, run, report: r });
                // 来源读取期间编辑草稿，真实迟到GET不能写回输入或参考。
                const gate = deferred(), got = deferred(), done = deferred();
                await page.route(`**/v1/runs/${run}/candidate?*`, async (route) => {
                    const response = await route.fetch();
                    got.release();
                    await gate.promise;
                    try {
                        await route.fulfill({ response });
                    }
                    catch (error) {
                        if (route.request().failure()?.errorText !== "net::ERR_ABORTED")
                            throw error;
                    }
                    finally {
                        done.release();
                    }
                });
                await page.locator("#source-run").fill(run);
                await page.getByRole("button", { name: "读取来源", exact: true }).click();
                await got.promise;
                await page.locator("#case-input").fill("edited during source read");
                await page.locator("#case-reason").fill("new reason");
                gate.release();
                await done.promise;
                await expect(page.locator("#case-input")).toHaveValue("edited during source read");
                await expect(page.locator("#case-expected")).toHaveValue("");
                await expect(page.locator("#source-eligibility")).toHaveCount(0);
            });
        }
    });
}
