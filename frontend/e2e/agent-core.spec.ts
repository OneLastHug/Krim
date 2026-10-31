import { test, expect, type Page } from "@playwright/test";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

const evidence = process.env.KRIM_GUIDE_EVIDENCE;
if (!evidence) throw new Error("验收需要显式任务证据目录 KRIM_GUIDE_EVIDENCE");
mkdirSync(evidence, { recursive: true });
const file = (name: string) => resolve(evidence, name);
const entries = () => existsSync(file("provider-calls.jsonl")) ? readFileSync(file("provider-calls.jsonl"), "utf8").trim().split("\n").filter(Boolean).length : 0;

async function connect(page:Page,key:string){await page.getByRole("button",{name:"连接",exact:true}).click();await page.locator("#api-key").fill(key);await page.getByRole("button",{name:"应用",exact:true}).click();await expect(page.getByRole("dialog")).toHaveCount(0);}
async function openRun(page:Page,id:string){expect(id).not.toBe("");await page.getByRole("button",{name:"打开任务",exact:true}).click();await page.locator("#history-id").fill(id);await page.getByRole("button",{name:"打开",exact:true}).click();await expect(page.getByRole("dialog")).toHaveCount(0);}

if (["capacity", "auth"].includes(process.env.KRIM_GUIDE_PHASE ?? "")) {
  test("认证或容量明确拒绝后修正条件并显式创建", async ({ page, request }) => {
    const capacity = process.env.KRIM_GUIDE_PHASE === "capacity";
    const apiKey = "SYNTHETIC_RECOVERY_KEY";
    const countTools = () => existsSync(file("tool-calls.jsonl")) ? readFileSync(file("tool-calls.jsonl"), "utf8").trim().split("\n").filter(Boolean).length : 0;
    const counts = () => {
      const database = new DatabaseSync(file("browser.sqlite"), { readOnly: true });
      try { return Number(database.prepare("SELECT count(*) AS count FROM runs").get()!.count); }
      finally { database.close(); }
    };
    const occupied: string[] = [];
    const submitted: Record<string, unknown>[] = [];
    page.on("request", value => { if(value.method() === "POST" && value.url().endsWith("/v1/runs")) submitted.push(value.postDataJSON()); });
    try {
      if(capacity){
        for(let index=0;index<4;index++){
          const response=await request.post("/v1/runs",{data:{text:"/slow",request_key:"occupied-"+index}});
          expect(response.status()).toBe(202);
          occupied.push((await response.json()).id);
        }
        await expect.poll(entries).toBe(4);
        expect((await (await request.get("/healthz")).json()).active).toBe(4);
      }
      const before={runs:counts(),provider:entries(),tools:countTools()};
      expect(before).toEqual({runs:capacity?4:0,provider:capacity?4:0,tools:0});
      await page.goto("/ui/");
      if(!capacity)await connect(page,"WRONG_SYNTHETIC_KEY");
      await page.locator("#prompt").fill("/add 2 3");
      const rejected=page.waitForResponse(value=>value.url().endsWith("/v1/runs")&&value.request().method()==="POST");
      await page.getByRole("button",{name:"创建任务",exact:true}).click();
      const response=await rejected;
      expect(response.status()).toBe(capacity?429:401);
      const code=(await response.json()).error.code;
      expect(code).toBe(capacity?"capacity":"unauthorized");
      await expect(page.locator("#send")).toBeEnabled();
      const after={runs:counts(),provider:entries(),tools:countTools()};
      expect(after).toEqual(before);
      writeFileSync(file("recovery-rejected.json"),JSON.stringify({status:response.status(),code,before,after,submitted},null,2));
      await page.screenshot({path:file("recovery-rejected.png"),fullPage:true});
      if(capacity){
        const stopped=await request.post(`/v1/runs/${occupied[0]}/cancel`,{data:{}});
        expect(stopped.status()).toBe(200);expect((await stopped.json()).status).toBe("aborted");
      }
      // 条件修正后刷新，再由用户明确操作；不能自动创建或因旧 intent 永久只查 404。
      await page.reload();
      if(!capacity)await connect(page,apiKey);
      await page.locator("#prompt").fill("/add 2 3");
      await page.getByRole("button",{name:"创建任务",exact:true}).click();
      await expect(page.locator("#result")).toHaveText("5");
      const id=(await page.locator("#run-id").textContent())!.trim();
      expect(counts()).toBe(before.runs+1);expect(entries()).toBe(before.provider+2);expect(countTools()).toBe(1);
      expect(submitted).toHaveLength(2);expect(submitted.every(body=>body.text==="/add 2 3")).toBe(true);
      const database=new DatabaseSync(file("browser.sqlite"),{readOnly:true});
      try {
        expect(Number(database.prepare("SELECT count(*) AS count FROM runs WHERE request_key=?").get(String(submitted[0]!.request_key))!.count)).toBe(submitted[0]!.request_key===submitted[1]!.request_key?1:0);
        expect(Number(database.prepare("SELECT count(*) AS count FROM runs WHERE request_key=?").get(String(submitted[1]!.request_key))!.count)).toBe(1);
      }finally{database.close();}
      writeFileSync(file("recovery-completed.json"),JSON.stringify({id,before,after:{runs:counts(),provider:entries(),tools:countTools()},submitted},null,2));
      await page.screenshot({path:file("recovery-completed.png"),fullPage:true});
      const posts=submitted.length;
      await page.reload();if(!capacity)await connect(page,apiKey);
      // 密钥不持久化；刷新后的认证读取通过已有“只读打开”入口明确重试。
      if(!capacity){await openRun(page,id);}
      await expect(page.locator("#result")).toHaveText("5");expect(submitted).toHaveLength(posts);
      expect(entries()).toBe(before.provider+2);expect(countTools()).toBe(1);
    } finally {
      for(const id of occupied){const stopped=await request.post(`/v1/runs/${id}/cancel`,{data:{}});expect(stopped.status()).toBe(200);}
    }
  });
} else if (process.env.KRIM_GUIDE_PHASE === "restart") {
  test("重启后的原 ID 只读结果和零重放", async ({ page }) => {
    const original = JSON.parse(readFileSync(file("browser-runs.json"), "utf8"));
    const posts: string[] = [];
    page.on("request", request => { if (request.method() === "POST") posts.push(request.url()); });
    await page.goto("/ui/#" + original.add);
    await expect(page.locator("#result")).toHaveText("5");
    await expect(page.locator("#status")).toHaveText("已完成");
    await page.screenshot({ path: file("restart-readonly.png"), fullPage: true });
    let release: () => void = () => {};
    let received: () => void = () => {};
    let delivered: () => void = () => {};
    const handled=new Promise<void>(resolve=>{delivered=resolve;});
    let originalPayload:Record<string,unknown>={};
    const held = new Promise<void>(resolve => { release = resolve; });
    const fetched = new Promise<void>(resolve => { received = resolve; });
    await page.route(`**/v1/runs/${original.cancel}`, async route => {
      const response = await route.fetch();
      originalPayload=await response.json();
      received();
      await held;
      try{await route.fulfill({ response });}catch(error){if(route.request().failure()?.errorText!=="net::ERR_ABORTED")throw error;}finally{writeFileSync(file("late-response-observed.json"),JSON.stringify({payload:originalPayload,failure:route.request().failure()},null,2));delivered();}
    });
    await page.goto("/ui/#" + original.cancel);
    await fetched;
    await expect(page.locator("#status")).toHaveText("读取中…");
    await expect(page.locator("#cancel")).toBeDisabled();
    await openRun(page,original.add);
    await expect(page.locator("#result")).toHaveText("5");
    release();
    await handled;
    expect(originalPayload.id).toBe(original.cancel);
    expect(originalPayload.status).toBe("aborted");
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    await expect(page.locator("#run-id")).toHaveText(original.add);
    await expect(page.locator("#status")).toHaveText("已完成");
    await expect(page.locator("#cancel")).toBeDisabled();
    await page.screenshot({ path: file("late-response-keeps-original-id.png"), fullPage: true });
    await page.unroute(`**/v1/runs/${original.cancel}`);
    await openRun(page,original.cancel);
    await expect(page.locator("#status")).toHaveText("已停止");
    expect(posts).toEqual([]);
    expect(entries()).toBe(original.provider_entries);
    const starts = readFileSync(file("server-starts.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(starts.length).toBe(2);
    expect(starts.every(value => value.module.includes("site-packages"))).toBe(true);
  });
} else {
  test("明确拒绝的空白创建可在刷新后修正", async ({ page, request }) => {
    const counts = () => {
      const database = new DatabaseSync(file("browser.sqlite"), { readOnly: true });
      try { return database.prepare("SELECT count(*) AS count FROM runs").get()!.count; }
      finally { database.close(); }
    };
    expect(counts()).toBe(0);
    expect(entries()).toBe(0);
    expect(existsSync(file("tool-calls.jsonl"))).toBe(false);
    await page.goto("/ui/");
    await page.locator("#prompt").fill("   ");
    const rejected = page.waitForResponse(response => response.url().endsWith("/v1/runs") && response.request().method() === "POST");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    const response = await rejected;
    expect(response.status()).toBe(422);
    expect((await response.json()).error.code).toBe("invalid_input");
    await expect(page.locator(".notice")).toContainText("任务内容无效");
    expect(counts()).toBe(0);
    expect(entries()).toBe(0);
    expect(existsSync(file("tool-calls.jsonl"))).toBe(false);
    expect((await (await request.get("/healthz")).json()).active).toBe(0);
    expect(await page.evaluate(() => localStorage.getItem("krim-agent-intent"))).toBeNull();
    await page.reload();
    expect(await page.evaluate(() => localStorage.getItem("krim-agent-intent"))).toBeNull();
    await page.locator("#prompt").fill("/add 2 3");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await expect(page.locator("#result")).toHaveText("5");
    expect(counts()).toBe(1);
    expect(entries()).toBe(2);
    expect(readFileSync(file("tool-calls.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line).name)).toEqual(["add"]);
    writeFileSync(file("rejection-recovery.json"), JSON.stringify({ rejected_status: response.status(), rejected_runs: 0, rejected_provider: 0, rejected_tools: 0, corrected_runs: counts(), corrected_provider: entries(), corrected_tools: 1 }, null, 2));
    await page.screenshot({ path: file("rejection-corrected.png"), fullPage: true });
  });

  test("创建、原键找回、审批允许拒绝、取消、刷新和窄屏", async ({ page, request }) => {
    const initialEntries = entries();
    await page.goto("/ui/");
    let intercepted = 0;
    let originalId = "";
    let lostLookup = false;
    await page.route("**/v1/runs/by-key/*", async route => {
      if(!lostLookup){lostLookup=true;await route.abort("failed");}
      else await route.continue();
    });
    await page.route("**/v1/runs", async route => {
      const response = await route.fetch();
      originalId = (await response.json()).id;
      intercepted += 1;
      // 使用真正后端响应，只模拟创建回程丢失；不得重发新键。
      await route.abort("failed");
    });
    await page.locator("#prompt").fill("/add 2 3");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await expect(page.locator("#send")).toBeEnabled();
    expect(await page.evaluate(() => localStorage.getItem("krim-agent-intent"))).not.toBeNull();
    await page.reload();
    // 未知回程保留原键；刷新后的再次操作只查询原 Run，不发第二次 POST。
    await page.getByRole("button", { name: "查找任务", exact: true }).click();
    await expect(page.locator("#result")).toHaveText("5");
    const add = (await page.locator("#run-id").textContent())!.trim();
    expect(add).toBe(originalId);
    expect(intercepted).toBe(1);
    await page.unroute("**/v1/runs");
    await page.unroute("**/v1/runs/by-key/*");
    const facts = await (await request.get(`/v1/runs/${add}/result`)).json();
    const trace = await (await request.get(`/v1/runs/${add}/trace`)).json();
    expect(facts.model_steps).toBe(2);
    expect(facts.tools).toHaveLength(1);
    expect(facts.tools[0].value).toEqual({ sum: 5 });
    expect(trace.spans.find((value: { kind: string; span_id: string }) => value.kind === "tool").span_id).toBe(facts.tools[0].span_id);
    expect(trace.spans.every((value: { run_id: string }) => value.run_id === add)).toBe(true);
    expect(entries()).toBe(initialEntries + 2);
    await page.screenshot({ path: file("add-trace.png"), fullPage: true });

    const payload = '<img src=x onerror="window.__krimInjected=true">SYNTHETIC_SECRET';
    await page.locator("#prompt").fill("/echo " + payload);
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await expect(page.locator("#result")).toHaveText(payload);
    expect(await page.locator("#result img").count()).toBe(0);
    expect(await page.evaluate(() => (window as unknown as Record<string, unknown>).__krimInjected)).toBeUndefined();
    await page.getByRole("tab",{name:"Trace",exact:true}).click();
    expect(await page.locator("#trace").innerText()).not.toContain("SYNTHETIC_SECRET");
    await page.getByRole("tab",{name:"结果",exact:true}).click();
    await page.screenshot({ path: file("private-text.png"), fullPage: true });

    await page.locator("#prompt").fill("/note welcome");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await expect(page.locator("#approval")).toContainText("welcome");
    await page.getByRole("button", { name: /^允许 .* 本次调用$/ }).click();
    await expect(page.locator("#result")).toContainText("合成演示资料");
    const approve = (await page.locator("#run-id").textContent())!.trim();
    await page.screenshot({ path: file("approved-note.png"), fullPage: true });

    await page.locator("#prompt").fill("/note welcome");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await page.getByRole("button", { name: /^拒绝 .* 本次调用$/ }).click();
    await expect(page.locator("#status")).toHaveText("失败");
    await page.getByRole("tab",{name:"工具",exact:true}).click();
    await expect(page.locator("#tools")).toContainText("approval_denied");
    const deny = (await page.locator("#run-id").textContent())!.trim();
    const denied = await (await request.get(`/v1/runs/${deny}/result`)).json();
    expect(denied.tools[0].executor_started).toBe(false);

    const before = entries();
    await page.locator("#prompt").fill("/slow");
    await page.getByRole("button", { name: "创建任务", exact: true }).click();
    await expect(page.locator("#status")).toHaveText("执行中");
    await expect.poll(entries).toBe(before + 1);
    const cancel = (await page.locator("#run-id").textContent())!.trim();
    await page.getByRole("button", { name: "停止", exact: true }).click();
    await expect(page.locator("#status")).toHaveText("已停止");
    expect((await (await request.get("/healthz")).json()).active).toBe(0);
    await page.setViewportSize({ width: 360, height: 800 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await page.screenshot({ path: file("cancel-narrow.png"), fullPage: true });
    const posts: string[] = [];
    page.on("request", request => { if (request.method() === "POST") posts.push(request.url()); });
    await page.reload();
    await expect(page.locator("#status")).toHaveText("已停止");
    expect(posts).toEqual([]);
    expect(entries()).toBe(before + 1);
    writeFileSync(file("browser-runs.json"), JSON.stringify({ add, approve, deny, cancel, provider_entries: entries() }, null, 2));
  });
}
