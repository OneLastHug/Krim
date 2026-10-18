import { test, expect, type Page, type APIRequestContext } from "@playwright/test";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

const root = process.env.KRIM_GUIDE_EVIDENCE;
if (!root) throw new Error("需要独立HTTP验收目录");
mkdirSync(root, { recursive: true });
const file = (name: string) => resolve(root, name);
const headers = { Authorization: "Bearer SYNTHETIC_M2_BROWSER_KEY" };
const entries = () => existsSync(file("entries.jsonl")) ? readFileSync(file("entries.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const counts = () => ({ provider: entries().filter(e => e.kind === "provider_entry").length, tools: entries().filter(e => e.kind === "tool_entry").length });
const save = (name: string, data: unknown) => writeFileSync(file(name + ".json"), JSON.stringify(data, null, 2));
async function connect(page: Page) { await page.getByRole("button", { name: "连接", exact: true }).click(); await page.locator("#api-key").fill("SYNTHETIC_M2_BROWSER_KEY"); await page.getByRole("button", { name: "应用", exact: true }).click(); await expect(page.getByRole("dialog")).toHaveCount(0); }
async function begin(page: Page) { await page.goto("/ui/?view=eval"); await connect(page); }
async function makeDataset(page: Page, name: string, input = "/add 2 3", expected = "5") {
    await page.locator("#dataset-name").fill(name);
    await page.locator("#case-id").fill("one");
    await page.locator("#case-input").fill(input);
    await page.locator("#case-expected").fill(JSON.stringify(expected));
    await page.locator("#case-reason").fill("人工核对的合成参考");
    await page.getByRole("button", { name: "加入样本", exact: true }).click();
    await page.locator("#freeze").click();
    await expect(page.locator("#dataset-fingerprint")).toHaveText(/^[a-f0-9]{64}$/);
    await page.locator("#eval-target").selectOption("target-http");
}
async function report(request: APIRequestContext, id: string) { const response = await request.get(`/v1/evals/${id}/report`, { headers }); expect(response.status()).toBe(200); return response.json(); }
async function capture(page: Page, name: string, selectors = ["#eval-status", ".eval-report>.run-identity>summary", "#eval-cancel"]) {
    await page.evaluate(() => scrollTo(0, 0));
    const geometry: Record<string, unknown> = {};
    for (const selector of selectors) {
        const box = await page.locator(selector).boundingBox();
        expect(box, selector).not.toBeNull();
        expect(box!.y).toBeGreaterThanOrEqual(0);
        expect(box!.y + box!.height).toBeLessThanOrEqual(page.viewportSize()!.height);
        geometry[selector] = box;
    }
    const data = await page.evaluate(() => {
        const fixed: string[] = [];
        const walker = document.createTreeWalker(document.querySelector("main")!, NodeFilter.SHOW_TEXT);
        while (walker.nextNode()) {
            const node = walker.currentNode, parent = node.parentElement;
            if (parent && parent.getClientRects().length && getComputedStyle(parent).visibility !== "hidden" && !parent.closest("[data-ui-data],pre,code,textarea,input,option")) {
                const value = node.textContent?.trim(); if (value) fixed.push(value);
            }
        }
        const data = [...document.querySelectorAll<HTMLElement>("[data-ui-data],pre,code,textarea")].filter(e => e.getClientRects().length).map(e => e instanceof HTMLTextAreaElement ? e.value : e.innerText);
        const active = document.activeElement as HTMLElement | null, style = active ? getComputedStyle(active) : null;
        return { visible: document.body.innerText, fixed, data: [...new Set(data)], overflow: document.documentElement.scrollWidth > innerWidth, focus: { id: active?.id, tag: active?.tagName, visible: active?.matches(":focus-visible"), outline: style?.outline, shadow: style?.boxShadow } };
    });
    expect(data.overflow).toBe(false);
    save(name, { geometry, viewport: page.viewportSize(), ...data, counts: counts() });
    await page.screenshot({ path: file(name + ".png") });
}
for (const viewport of [{ name: "desktop", width: 1280, height: 900 }, { name: "narrow", width: 360, height: 800 }]) {
    test.describe(viewport.name, () => {
        test.use({ viewport: { width: viewport.width, height: viewport.height } });
        const errors: string[] = [];
        test.beforeEach(async ({ page }) => { errors.length = 0; page.on("pageerror", e => errors.push(e.message)); page.on("console", e => { if (/Content Security Policy|Refused to/.test(e.text())) errors.push(e.text()); }); });
        test.afterEach(() => expect(errors).toEqual([]));
        if (process.env.KRIM_GUIDE_PHASE === "http-restart") {
            test("HTTP重启原报告只读", async ({ page, request }) => {
                const original = JSON.parse(readFileSync(file(viewport.name + "-identity.json"), "utf8"));
                const before = counts(), posts: string[] = [];
                page.on("request", r => { if (r.method() === "POST") posts.push(r.url()); });
                await page.goto(`/ui/?view=eval&eval_id=${original.eval}`); await connect(page);
                await expect(page.locator("#eval-id")).toHaveText(original.eval);
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                await capture(page, viewport.name + "-restart");
                expect((await report(request, original.eval)).target_descriptor.key).toBe("target-http");
                await page.reload(); await connect(page); await expect(page.locator("#eval-id")).toHaveText(original.eval);
                expect(counts()).toEqual(before); expect(posts).toEqual([]);
            });
        } else {
            test("HTTP目标来源冻结与真实子Run", async ({ page, request }) => {
                const before = counts();
                const accepted = await request.post("/v1/runs", { headers, data: { text: "/add 2 3", request_key: viewport.name + "-http-source", target: "target-http" } });
                expect(accepted.status()).toBe(202); const source = (await accepted.json()).id;
                await expect.poll(async () => (await (await request.get(`/v1/runs/${source}`, { headers })).json()).status).toBe("completed");
                await begin(page);
                await page.locator("#source-run").fill(source); await page.getByRole("button", { name: "读取来源", exact: true }).click();
                await expect(page.locator("#source-eligibility")).toHaveText("可选入");
                expect(await page.locator("#case-expected").inputValue()).toBe("");
                await page.getByRole("button", { name: "使用输入", exact: true }).click();
                await expect(page.locator("#case-input")).toHaveValue("/add 2 3");
                await page.locator("#dataset-name").fill(viewport.name + "-http");
                await page.locator("#case-id").fill("source"); await page.locator("#case-expected").fill('"5"'); await page.locator("#case-reason").fill("人工核算2加3等于5");
                await page.getByRole("button", { name: "加入样本", exact: true }).click(); await page.locator("#freeze").click();
                await expect(page.locator("#dataset-fingerprint")).toHaveText(/^[a-f0-9]{64}$/);
                await page.locator("#eval-target").selectOption("target-http"); await page.locator("#eval-start").click();
                await expect(page.locator("#eval-status")).toHaveText("已完成");
                const id = (await page.locator("#eval-id").textContent())!;
                const value = await report(request, id);
                expect(value.items[0].source_run_id).toBe(source); expect(value.items[0].run_id).not.toBe(source);
                expect(value.items[0].quality).toBe("passed"); expect(value.execution.limits.http.max_output_tokens).toBe(512);
                expect(counts()).toEqual({ provider: before.provider + 4, tools: before.tools + 2 });
                await capture(page, viewport.name + "-complete");
                save(viewport.name + "-identity", { eval: id, source, child: value.items[0].run_id });
                const stable = counts(); await page.reload(); await connect(page); await expect(page.locator("#eval-id")).toHaveText(id); expect(counts()).toEqual(stable);
                if (viewport.name === "narrow") {
                    await page.locator("#eval-editor-toggle").click();
                    const long = JSON.stringify("LONG_REFERENCE_".repeat(400));
                    await page.locator("#case-expected").fill(long); await expect(page.locator("#case-expected")).toHaveValue(long);
                    await page.locator("#case-expected").press("Control+End");
                    save("narrow-editor", await page.locator("#case-expected").evaluate(e => ({ length: (e as HTMLTextAreaElement).value.length, clientHeight: e.clientHeight, scrollHeight: e.scrollHeight, scrollTop: e.scrollTop })));
                    await page.locator("#case-expected").press("Tab"); expect(await page.locator("#case-expected").evaluate(e => document.activeElement !== e)).toBe(true);
                    await page.screenshot({ path: file("narrow-editor.png") });
                    await page.locator("#eval-editor-toggle").click();
                }
                await page.getByRole("button", { name: "查看任务", exact: true }).click();
                await expect(page.locator("#result")).toHaveText("5"); await page.getByRole("tab", { name: "Trace", exact: true }).click();
                await expect(page.locator("#trace")).not.toBeEmpty();
                await capture(page, viewport.name + "-trace", ["#status", "#cancel"]);
            });
            test("HTTP创建未知原键找回", async ({ page, request }) => {
                await begin(page); await makeDataset(page, viewport.name + "-unknown"); const before = counts();
                let release!: () => void, accepted!: (id: string) => void;
                const gate = new Promise<void>(r => { release = r; }), entered = new Promise<string>(r => { accepted = r; });
                let posts = 0, key = "";
                await page.route("**/v1/evals", async route => { key = route.request().postDataJSON().request_key; const response = await route.fetch(); posts++; const id = (await response.json()).id; accepted(id); await gate; await route.fulfill({ response }); });
                try {
                    await page.locator("#eval-start").click(); const id = await entered;
                    await page.getByRole("tab", { name: "任务", exact: true }).click(); release();
                    await page.getByRole("tab", { name: "评测", exact: true }).click();
                    await expect(page.locator("#eval-pending-state")).toHaveText("评测创建结果未确认");
                    await capture(page, viewport.name + "-unknown", ["#eval-pending-state", "#eval-request-key", "#eval-start"]);
                    await page.route("**/v1/evals/by-key/**", route => route.abort("failed"), { times: 1 });
                    await page.getByRole("button", { name: "查找评测", exact: true }).click();
                    // 第一GET失败后，原helper只读回退/确认仍用同一键；找回成功后不能再点击“开始评测”。
                    await expect(page.locator("#eval-id")).toHaveText(id); await expect(page.locator("#eval-status")).toHaveText("已完成");
                    expect((await report(request, id)).target_descriptor.key).toBe("target-http"); expect(posts).toBe(1);
                    expect(counts()).toEqual({ provider: before.provider + 2, tools: before.tools + 1 });
                    const database = new DatabaseSync(file("browser.sqlite"), { readOnly: true });
                    try { expect(database.prepare("SELECT count(*) AS n FROM evaluations WHERE request_key=?").get(key)!.n).toBe(1); } finally { database.close(); }
                    const stable = counts(); await page.reload(); await connect(page); await expect(page.locator("#eval-id")).toHaveText(id); expect(counts()).toEqual(stable);
                } finally { release(); }
            });
            test("HTTP取消partial与上游失败", async ({ page, request }) => {
                await begin(page); await makeDataset(page, viewport.name + "-hold", "/echo FIXTURE_HOLD", "FIXTURE_HOLD");
                await page.locator("#eval-start").click(); await expect(page.locator("#eval-status")).toHaveText("执行中");
                await capture(page, viewport.name + "-running"); await page.locator("#eval-cancel").click();
                await expect(page.locator("#eval-status")).toHaveText("已停止"); const id = (await page.locator("#eval-id").textContent())!;
                expect((await report(request, id)).complete).toBe(false); await capture(page, viewport.name + "-partial");
                const before = counts();
                const response = await request.post("/v1/runs", { headers, data: { text: "/echo FIXTURE_BAD_FRAME", target: "target-http", request_key: viewport.name + "-bad" } });
                expect(response.status()).toBe(202); const run = (await response.json()).id;
                await page.goto("/ui/#" + run); await connect(page); await expect(page.locator("#status")).toHaveText("失败");
                expect(counts()).toEqual({ provider: before.provider + 1, tools: before.tools }); await capture(page, viewport.name + "-failure", ["#status", "#cancel"]);
                const missing = await request.post("/v1/runs", { headers, data: { text: "/echo FIXTURE_NO_USAGE", target: "target-http", request_key: viewport.name + "-usage" } });
                const missingID = (await missing.json()).id;
                await expect.poll(async () => (await (await request.get(`/v1/runs/${missingID}`, { headers })).json()).status).toBe("completed");
                const facts = (await (await request.get(`/v1/runs/${missingID}/result`, { headers })).json()); expect(facts.token_usage).toBeNull(); expect(facts.usage_complete).toBe(false);
                save(viewport.name + "-unknown-usage", facts);
            });
        }
    });
}
