import { test, expect, type Page, type APIRequestContext, type TestInfo } from "@playwright/test";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createConnection } from "node:net";
import { createHash } from "node:crypto";

const OUT = process.env.KRIM_AGENT_CORE_EVIDENCE!;
const ROOT = process.env.KRIM_M2_FINAL_ROOT!;
const AUTH = { Authorization: "Bearer final-api-token" };
const INTENTS = "krim.agent-core.workspace-intents/1";
const KNOWN = "krim.agent-core.workspace-known/1";
const CONSUMER = resolve(ROOT,"evidence/final/final-directory01-consumer/CONSUMER-IDENTITY.json");
const bodyFailures = new Map<string,unknown>();
test.describe.configure({mode:"serial"});
type State = { url: string; owner_id: number; session_id: string; other_session_id: string;
  provider_calls: number; streams_closed: number; database: string; active_runs: number;
  node_key: string[]; prepare_id: string;
  http: { method: string; path: string }[]; requests: Record<string, unknown>[] };
function barrier() {
  let release!: () => void;
  const promise = new Promise<void>(done => { release = done; });
  return { promise, release };
}
async function prepare(request: APIRequestContext, mode: string, seed = false): Promise<State> {
  const info=test.info(),node_key=info.titlePath.filter(Boolean),keys:string[][]=[];
  for(const viewport of ["1280x900","390x844"]) {
    const prefix=["agent-workspace.spec.ts",viewport];
    keys.push([...prefix,"workspace_unavailable_is_truthful_and_zero_effect"]);
    for(const variant of ["no-turn","old-turn"])
      keys.push([...prefix,`confirmed_inputs_${variant}_refresh_remount_same_identity`]);
    for(const boundary of ["post","get"]) for(const steering of ["session-return","credential","remount"])
      keys.push([...prefix,`real_accepted_input_${boundary}_late_${steering}`]);
    keys.push([...prefix,"real long history fixture",
      "long_history_summary_approval_real_unittest_artifact_verification_next_turn"]);
    for(const scenario of ["summary-fail","approval-denied","command-stop","verification-fail","artifact-unknown"])
      keys.push([...prefix,scenario,`real_${scenario}_preserves_partial_truth`]);
  }
  const ordinal=keys.findIndex(key=>JSON.stringify(key)===JSON.stringify(node_key));
  expect(ordinal).toBeGreaterThanOrEqual(0);
  const prepare_id=OUT.split("/").at(-1)!+"-"+ordinal,started=Date.now(),clock=performance.now();
  const receiptBytes=await readFile(CONSUMER),ready=JSON.parse(await readFile(resolve(OUT,"SERVER-READY.json"),"utf8"));
  expect(ready.consumer_receipt_path).toBe(CONSUMER);
  expect(ready.consumer_receipt_sha256).toBe(createHash("sha256").update(receiptBytes).digest("hex"));
  expect(ready.consumer_receipt_kind).toBe("M2_FINAL_CONSUMER_QUALIFICATION_V1");
  expect(ready.grant_sha256).toBe(createHash("sha256").update(await readFile(resolve(ROOT,"release/directory01/grant.json"))).digest("hex"));
  let preparationError:string|null=null;
  try {
    const result = await request.post("/__fixture/new", { headers: AUTH,
      data: { mode, seed, node_key, prepare_id } });
    expect(result.ok(), await result.text()).toBe(true);
    const owner=await result.json() as State;
    expect(owner.node_key).toEqual(node_key);expect(owner.prepare_id).toBe(prepare_id);
    return owner;
  } catch(error) {preparationError=String(error);throw error;}
  finally {
    await mkdir(resolve(OUT,"ui-preparations"),{recursive:true});
    await writeFile(resolve(OUT,"ui-preparations",prepare_id+".json"),JSON.stringify({
      node_key,prepare_id,mode,seed,started_at:started,ended_at:Date.now(),
      monotonic_elapsed_ms:performance.now()-clock,error:preparationError,
      phase:seed?"beforeAll-real-eight-turns":"test-prepare",default_timeout:info.timeout
    },null,2),{flag:"wx"});
  }
}
async function state(request: APIRequestContext, owner: State): Promise<State> {
  const result = await request.get(owner.url+"/__fixture/state", { headers: AUTH });
  expect(result.ok()).toBe(true); return await result.json() as State;
}
async function connect(page: Page, token = "final-api-token") {
  const drawer = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  if (!await drawer.isVisible()) {
    const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
    if (await mobileMenu.isVisible()) { await mobileMenu.click(); await expect(drawer).toBeVisible(); }
  }
  await page.getByRole("button", { name: "密钥身份", exact: true }).click();
  await page.getByRole("menuitem", { name: "连接 / 切换密钥" }).click();
  await page.locator("#api-key").fill(token); await page.locator("#connect-save").click();
  await expect(page.locator(".toolbar-actions").getByRole("button", { name: "新建对话", exact: true })).toBeEnabled();
  // 连接完成后恢复抽屉开合状态，避免遮挡主区操作。
  if (await drawer.isVisible()) { await page.keyboard.press("Escape"); await expect(drawer).not.toBeVisible(); }
}
async function navigateView(page: Page, view: "chat" | "run") {
  const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
  let sidebar = page.locator(".desktop-sidebar");
  if (await mobileMenu.isVisible()) {
    await mobileMenu.click();
    sidebar = page.getByRole("dialog", { name: "主导航与对话", exact: true });
  }
  const button = sidebar.locator(".sidebar-buttons").getByRole("button", { name: view === "run" ? "导航到运行记录" : "导航到对话", exact: true });
  await button.focus(); await expect(button).toBeFocused(); await page.keyboard.press("Enter");
  await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe(view);
}
async function selectSession(page: Page, title: string) {
  const mobileMenu = page.getByRole("button", { name: "打开导航", exact: true });
  if (await mobileMenu.isVisible()) {
    await mobileMenu.click();
    await page.getByRole("dialog", { name: "主导航与对话", exact: true }).getByRole("button", { name: title, exact: true }).click();
  } else {
    await page.getByRole("button", { name: title, exact: true }).click();
  }
}
async function openWorkspaceTaskArea(page: Page) {
  const area = page.locator(".workspace-task-area");
  await expect(area).toBeVisible();
  if (await area.getAttribute("open") === null) await area.locator("summary").click();
  await expect(page.locator("#workspace-text")).toBeVisible();
}
async function open(page: Page, owner: State) {
  await page.goto(owner.url+"/ui/?view=chat&workspace=local&session="+owner.session_id);
  await connect(page);
  await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("FINAL工作A");
  await openWorkspaceTaskArea(page);
  await expect(page.locator("#workspace-text")).toBeEnabled();
}
async function resetTurnChecks(page: Page) {
  while(await page.locator(".workspace-check-list li").count())
    await page.locator(".workspace-check-list li button").first().click();
  while(await page.locator(".workspace-condition-list > li").count())
    await page.locator(".workspace-condition-title button").first().click();
}
async function send(page: Page, text = "D4_TASK 浏览器真实项目") {
  await page.locator("#workspace-text").fill(text);
  await resetTurnChecks(page);
  if(!text.startsWith("D4_NEXT")) {
    await page.locator("#workspace-required-path").fill("calc.py");
    await page.getByRole("button",{name:"添加路径",exact:true}).click();
    await page.locator("#workspace-required-path").fill("answer.json");
    await page.getByRole("button",{name:"添加路径",exact:true}).click();
    await page.getByRole("button",{name:"添加完成条件",exact:true}).click();
    const condition=page.locator(".workspace-condition");
    await condition.getByLabel("条件名称").fill("answer");
    await condition.getByLabel("检查文件").fill("answer.json");
    await condition.getByLabel("JSON Pointer").fill("/answer");
    await condition.getByLabel("预期 JSON 值").fill("42");
  }
  await page.getByRole("button", { name: "发送工作请求", exact: true }).click();
}
async function approve(page: Page, command = true) {
  const steps = [{name:"workspace_write",argument:"calc.py"},
    {name:"workspace_write",argument:"test_calc.py"},
    ...(command?[{name:"command_run",argument:"unittest"}]:[])];
  for (const {name,argument} of steps) {
    const section = page.locator(".chat-approval").filter({ hasText: name });
    await expect(section).toBeVisible();
    // 相邻同名write必须等实际参数换成下一调用，不能重复点击仍显示的旧审批。
    await expect(section.locator("pre")).toContainText(argument);
    const actualArguments=JSON.parse((await section.locator("pre").textContent())!);
    const [reply]=await Promise.all([
      page.waitForResponse(response=>response.request().method()==="POST"&&response.url().includes("/approvals/")),
      section.getByRole("button", { name: "允许 "+name+" 本次调用", exact: true }).click()
    ]);
    const post=reply.request(),turnPath=post.url().split("/approvals/")[0]!;
    const [turn,trace]=await Promise.all([
      page.request.get(turnPath,{headers:AUTH}),page.request.get(turnPath+"/trace",{headers:AUTH})
    ]);
    await writeFile(test.info().outputPath("approval-"+argument+".json"),JSON.stringify({
      tool:name,expected_argument:argument,arguments:actualArguments,
      url:post.url(),method:post.method(),body:post.postDataJSON(),
      status:reply.status(),reply:await reply.json(),
      turn_status:turn.status(),turn:await turn.json(),trace_status:trace.status(),trace:await trace.json()
    },null,2),{flag:"wx"});
    expect(reply.ok()).toBe(true);expect(turn.ok()).toBe(true);expect(trace.ok()).toBe(true);
  }
}
async function work(request: APIRequestContext, owner: State) {
  const response = await request.get(owner.url+"/v3/workspaces/local/sessions/"+owner.session_id+"/work",
    { headers: AUTH }); expect(response.ok()).toBe(true); return await response.json();
}
async function expectTurnStatus(page: Page, request: APIRequestContext, owner: State, status: string) {
  const turnId = await page.locator(".chat-turn").last().getAttribute("data-turn-id");
  expect(turnId).toBeTruthy();
  await expect.poll(async () => {
    const response = await request.get(owner.url+"/v2/workspaces/local/sessions/"+owner.session_id+
      "/turns/"+encodeURIComponent(turnId!), { headers: AUTH });
    expect(response.ok()).toBe(true);
    const turn = await response.json();
    expect(turn.id).toBe(turnId);
    return turn.status as string;
  }).toBe(status);
}
async function capture(page: Page, info: TestInfo, name: string) {
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.keyboard.press("Tab");
  const output = info.outputPath(name+".png"); await mkdir(resolve(output, ".."), { recursive: true });
  await page.screenshot({ path: output, animations: "disabled" });
  const metrics = await page.evaluate(() => {
    const rect = (element: Element | null) => {
      if (!element) return null; const r = element.getBoundingClientRect();
      return { x:r.x, y:r.y, width:r.width, height:r.height, bottom:r.bottom };
    };
    const canvas = document.createElement("canvas"), ctx = canvas.getContext("2d")!;
    const rgb = (color: string) => { ctx.clearRect(0,0,1,1); ctx.fillStyle=color;
      ctx.fillRect(0,0,1,1); return [...ctx.getImageData(0,0,1,1).data]; };
    const background = (el: Element | null): number[] => {
      if (!el) return [255,255,255,255]; const a=rgb(getComputedStyle(el).backgroundColor),
        b=background(el.parentElement), alpha=a[3]!/255;
      return [0,1,2].map(i=>a[i]!*alpha+b[i]!*(1-alpha)).concat(255);
    };
    const lum=(c:number[])=>c.slice(0,3).map(v=>{ const n=v/255;return n<=.04045?n/12.92:((n+.055)/1.055)**2.4; })
      .reduce((a,v,i)=>a+v*[.2126,.7152,.0722][i]!,0);
    const contrast=[...document.querySelectorAll<HTMLElement>(".chat-workbench button,.workspace-panel p,.chat-toolbar .toolbar-title h1,.chat-turn [role=status]")]
      .filter(el=>el.getClientRects().length&&!el.closest("[hidden]")&&!el.matches(":disabled")&&getComputedStyle(el).visibility!=="hidden")
      .map(el=>{ const a=lum(rgb(getComputedStyle(el).color)),b=lum(background(el));return {
        text:el.innerText.slice(0,100),ratio:(Math.max(a,b)+.05)/(Math.min(a,b)+.05) }; });
    const buttons=[...document.querySelectorAll<HTMLButtonElement>(".chat-workbench button")]
      .filter(el=>el.getClientRects().length&&!el.disabled&&!el.closest("[hidden]"))
      .map(el=>({text:el.innerText,rect:rect(el)}));
    const active=document.activeElement as HTMLElement;
    return { viewport:{width:innerWidth,height:innerHeight},
      overflow:document.documentElement.scrollWidth>innerWidth,heading:rect(document.querySelector(".chat-toolbar .toolbar-title h1")),
      session:new URL(location.href).searchParams.get("session"),turn:new URL(location.href).searchParams.get("turn"),
      stop:rect(document.querySelector("#chat-stop")),find:[...document.querySelectorAll(".workspace-pending button,.workspace-known button")].map(rect),
      buttons,contrast,focus:{tag:active?.tagName,id:active?.id,outline:getComputedStyle(active).outlineWidth},
      scroll:[...document.querySelectorAll<HTMLElement>(".chat-transcript,.workspace-artifact pre,textarea")]
        .map(el=>({tag:el.tagName,clientHeight:el.clientHeight,scrollHeight:el.scrollHeight,scrollTop:el.scrollTop})),
      visible_text:document.body.innerText };
  });
  await writeFile(info.outputPath(name+".json"),JSON.stringify(metrics,null,2));
  expect(metrics.overflow).toBe(false);
  expect(metrics.heading!.bottom).toBeLessThanOrEqual(metrics.viewport.height);
  for(const target of metrics.buttons) { expect(target.rect!.width,target.text).toBeGreaterThanOrEqual(44);
    expect(target.rect!.height,target.text).toBeGreaterThanOrEqual(44); }
  for(const item of metrics.contrast) expect(item.ratio,item.text).toBeGreaterThanOrEqual(4.5);
  await info.attach(name,{path:output,contentType:"image/png"});
}
async function finish(page: Page, request: APIRequestContext, owner: State, info: TestInfo,
  errors: string[]) {
  const failures:unknown[]=[];
  const save=async(name:string,value:unknown)=>{
    try {await writeFile(info.outputPath(name),JSON.stringify(value,null,2),{flag:"wx"});}
    catch(error){failures.push(error);}
  };
  // 仅在批准的finish观察本node同浏览器的系统字体与已保存截图文字；不改capture。
  try {
    type Face={file:string;sha256:string;postscript_name:string;coverage_ranges:[number,number][]};
    type Font={familyName:string;postScriptName:string;isCustomFont:boolean;glyphCount:number};
    const fontBytes=await readFile(resolve(ROOT,"materials/fonts/FONT-QUALIFIED.json"));
    const material=JSON.parse(fontBytes.toString()) as {coverage:{faces:Face[]}};
    const fonts:Font[]=[];
    const cdp=await page.context().newCDPSession(page);
    try {
      await cdp.send("DOM.enable");await cdp.send("CSS.enable");
      const document=await cdp.send("DOM.getDocument");
      const nodes=await cdp.send("DOM.querySelectorAll",{nodeId:document.root.nodeId,
        selector:"h2,button,p,pre,summary,label"});
      for(const nodeId of nodes.nodeIds.slice(0,128)) {
        const value=await cdp.send("CSS.getPlatformFontsForNode",{nodeId});
        fonts.push(...value.fonts.filter((font:Font)=>font.glyphCount>0));
      }
    } finally {await cdp.detach();}
    const faces=material.coverage.faces.filter(face=>fonts.some(font=>font.postScriptName===face.postscript_name));
    const files=[];
    for(const file of [...new Set(faces.map(face=>face.file))]) {
      const actual=createHash("sha256").update(await readFile(file)).digest("hex");
      files.push({file,sha256:actual});expect(faces.find(face=>face.file===file)!.sha256).toBe(actual);
    }
    const screenshots=[];
    for(const attachment of info.attachments.filter(a=>a.contentType==="image/png"&&a.path?.startsWith(info.outputDir+"/"))) {
      const path=attachment.path!,metricsPath=path.replace(/\.png$/,".json");
      const metrics=JSON.parse(await readFile(metricsPath,"utf8")) as {visible_text:string};
      const codepoints=[...new Set([...metrics.visible_text].map(ch=>ch.codePointAt(0)!)
        .filter(cp=>(cp>=0x3400&&cp<=0x9fff)||(cp>=0x20000&&cp<=0x323af)))];
      const missing=codepoints.filter(cp=>!faces.some(face=>face.coverage_ranges.some(([start,end])=>start<=cp&&cp<=end)));
      screenshots.push({path,metrics_path:metricsPath,codepoints,missing_codepoints:missing,
        screenshot_sha256:createHash("sha256").update(await readFile(path)).digest("hex")});
    }
    await save("actual-page-fonts.json",{observation_phase:"node-finish-before-navigation",
      material_sha256:createHash("sha256").update(fontBytes).digest("hex"),fonts,files,
      screenshots,cdp_detached:true,scope:"same browser selection and cmap coverage; screenshots remain original"});
    for(const screenshot of screenshots) expect(screenshot.missing_codepoints).toEqual([]);
  } catch(error) {failures.push(error);}
  try {
    const snapshot = await state(request, owner);
    const receipt=await readFile(CONSUMER);
    await writeFile(info.outputPath("node-database.json"),JSON.stringify({ node:info.titlePath,
      database:snapshot.database,server:snapshot,installed_receipt:
      CONSUMER,consumer_sha256:createHash("sha256").update(receipt).digest("hex"),
      consumer_kind:JSON.parse(receipt.toString()).kind,prepare_id:owner.prepare_id },null,2));
  } catch(error) {failures.push(error);
  } finally {
    const routes=await Promise.allSettled([page.unrouteAll({behavior:"wait"})]);
    const navigation=await Promise.allSettled([page.goto("about:blank")]);
    const closing=await Promise.allSettled([
      request.post("/__fixture/close",{headers:AUTH,data:{owner_id:owner.owner_id}})
    ]);
    await save("console-and-CSP.json",errors);
    await save("reader-navigation-cleanup.json",navigation);
    await save("route-cleanup.json",routes);
    try {
      const outcome=closing[0]!;
      if(outcome.status!=="fulfilled") throw outcome.reason;
      const response=outcome.value;
      expect(response.ok(),await response.text()).toBe(true);const closed=await response.json();
      await save("owner-closed.json",closed);
      expect(closed.errors).toEqual([]);expect(closed.pending_connections).toEqual([]);
      expect(closed.store_closed).toBe(true);expect(closed.provider_closed).toBe(true);
      expect(closed.second_writer_closed).toBe(true);expect(closed.all_producers_joined).toBe(true);
      if(closed.runner) { expect(closed.runner.closed).toBe(true);expect(closed.runner.jobs).toBe(0);
        expect(closed.runner.resources).toBe(0);expect(closed.runner.owner_closed).toBe(true); }
      expect(closed.FD_equal).toBe(true);expect(closed.unsafe_gc).toBe(0);
      expect(closed.provider_active).toBe(0);expect(closed.model_tasks_pending).toBe(0);
      expect(closed.provider_calls).toBe(closed.streams_closed);
    } catch(error) {failures.push(error);}
    try {
      // 原逐条console/CSP与导航断言保持；错误最后合并，不能短路真实关闭。
      for(const message of errors) expect(message).toMatch(
        /^Failed to load resource: (the server responded with a status of (401|404).*|net::ERR_FAILED)$/);
      expect(navigation[0]!.status).toBe("fulfilled");expect(routes[0]!.status).toBe("fulfilled");
    } catch(error) {failures.push(error);}
    const business=bodyFailures.get(info.testId);bodyFailures.delete(info.testId);
    await save("cleanup-attempts.json",{routes,navigation,closing:closing.map(v=>v.status),
      business_error:business instanceof Error?{message:business.message,stack:business.stack}:String(business),
      cleanup_errors:failures.map(String),finished_at:Date.now()});
    if(failures.length) throw new AggregateError(business===undefined?failures:[business,...failures],"node cleanup failed");
  }
}
function observe(page: Page) {
  const errors: string[]=[];
  page.on("pageerror",error=>errors.push("uncaught:"+error.message));
  page.on("console",message=>{if(message.type()==="error") errors.push(message.text());});
  return errors;
}

test.afterAll(async({request})=>{
  // 文件级无条件关闭；native finally也持有同一服务，不依赖最后标题或worker存活。
  const reply=await request.post("/__fixture/shutdown",{headers:AUTH});expect(reply.ok()).toBe(true);
  const path=resolve(OUT,"SERVER-CLOSED.json");let closed:Record<string,unknown>|null=null;
  for(let i=0;i<500;i++){try{closed=JSON.parse(await readFile(path,"utf8"));break;}
    catch{await new Promise(done=>setTimeout(done,10));}}
  expect(closed).not.toBeNull();expect(closed!.errors).toEqual([]);expect(closed!.FD_equal).toBe(true);
  expect(closed!.all_tasks_joined).toBe(true);
  const portClosed=await new Promise<boolean>(done=>{const socket=createConnection({host:"127.0.0.1",port:8876});
    socket.once("error",()=>{socket.destroy();done(true);});socket.once("connect",()=>{socket.destroy();done(false);});});
  expect(portClosed).toBe(true);
});

for(const viewport of [{width:1280,height:900},{width:390,height:844}]) test.describe(`${viewport.width}x${viewport.height}`,()=>{
  test.use({viewport,extraHTTPHeaders:AUTH});
  test.describe("real long history fixture",()=>{
  let historyOwner:State;
  // 真实8轮前史在独立默认期限的fixture hook内产生，保留原生成量且不增加产品/测试deadline。
  test.beforeAll(async({request})=>{historyOwner=await prepare(request,"normal",true);});
  test("long_history_summary_approval_real_unittest_artifact_verification_next_turn",async({page,request},info)=>{
    const owner=historyOwner,errors=observe(page);
    try {
      await open(page,owner);await send(page);
      await expect(page.locator(".chat-approval")).toBeVisible();await capture(page,info,"approval");
      await approve(page);await expectTurnStatus(page,request,owner,"completed");
      await page.getByRole("button",{name:"刷新工作结果",exact:true}).click();
      await expect(page.locator(".workspace-facts")).toContainText("passed");
      const value=await work(request,owner);expect(value.verification.state).toBe("passed");
      expect(value.compactions.some((v:{status:string})=>v.status==="completed")).toBe(true);
      expect(value.effects.filter((v:{kind:string})=>v.kind==="command")).toHaveLength(1);
      await expect(page.locator(".workspace-artifact")).toContainText("answer.json");
      await capture(page,info,"verified-artifact");
      await send(page,"D4_NEXT "+value.verification.revision_id);
      await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText("失败");
      const next=await work(request,owner);expect(next.effects).toHaveLength(1);
      expect(next.effects[0].kind).toBe("read");
      const seen=await state(request,owner);expect(seen.requests.some(v=>!("tools" in v))).toBe(true);
      await capture(page,info,"next-turn-original-revision");
    } catch(error) {bodyFailures.set(info.testId,error);throw error;}
    finally { await finish(page,request,owner,info,errors); }
  });
  });
  test("workspace_unavailable_is_truthful_and_zero_effect",async({page,request},info)=>{
    const owner=await prepare(request,"unavailable"),errors=observe(page);
    try {
      await page.goto(owner.url+"/ui/?view=chat");await connect(page);
      await page.locator(".toolbar-actions").getByRole("button",{name:"新建对话",exact:true}).click();
      await expect(page.locator('option[value="target-workspace"]')).toBeDisabled();
      await expect(page.getByText("工作区未配置",{exact:true})).toBeVisible();
      const denied=await request.post(owner.url+"/v2/workspaces/local/sessions",{headers:AUTH,
        data:{request_key:"unavailable",target_key:"target-workspace"}});
      expect(denied.ok()).toBe(false);const snapshot=await state(request,owner);
      expect(snapshot.provider_calls).toBe(0);expect(snapshot.requests).toEqual([]);
      await capture(page,info,"workspace-unavailable");
    } catch(error) {bodyFailures.set(info.testId,error);throw error;}
    finally { await finish(page,request,owner,info,errors); }
  });
  for(const mode of ["summary-fail","approval-denied","command-stop","verification-fail","artifact-unknown"]) test.describe(mode,()=>{
    let seededOwner:State|undefined;
    if(mode==="summary-fail") test.beforeAll(async({request})=>{seededOwner=await prepare(request,mode,true);});
    test(`real_${mode}_preserves_partial_truth`,async({page,request},info)=>{
      const owner=seededOwner??await prepare(request,mode==="approval-denied"?"normal":mode),errors=observe(page);
      try {
        await open(page,owner);await send(page);
        if(mode==="approval-denied") {
          await expect(page.locator(".chat-approval")).toBeVisible();
          await page.locator(".chat-approval").getByRole("button",{name:"拒绝",exact:true}).click();
        } else if(mode!=="summary-fail") {
          await approve(page);
          if(mode==="command-stop") {
            await expect.poll(async()=>{const value=await work(request,owner);
              return value.effects.some((v:{kind:string;phase:string})=>v.kind==="command"&&v.phase==="dispatch_intent");}).toBe(true);
            await capture(page,info,"command-running-stop");
            await page.locator("#chat-stop").click();
          }
        }
        await expect(page.locator(".chat-turn").last().locator("[role=status]")).toHaveText(mode==="command-stop"?"已停止":"失败");
        await page.getByRole("button",{name:"刷新工作结果",exact:true}).click();
        const value=await work(request,owner);expect(value.verification?.state).not.toBe("passed");
        if(mode==="summary-fail"||mode==="approval-denied")
          expect(value.effects.filter((v:{executor_started:boolean|null})=>v.executor_started===true)).toEqual([]);
        if(mode==="verification-fail") expect(value.verification.state).toBe("failed");
        if(mode==="artifact-unknown") expect(value.effects.find((v:{kind:string})=>v.kind==="command").exit_code).toBe(0);
        await capture(page,info,mode);
      } catch(error) {bodyFailures.set(info.testId,error);throw error;}
    finally { await finish(page,request,owner,info,errors); }
    });
  });
  for(const variant of ["no-turn","old-turn"])
    test(`confirmed_inputs_${variant}_refresh_remount_same_identity`,async({page,request},info)=>{
      const owner=await prepare(request,"inputs"),errors=observe(page);
      try {
        if(variant==="old-turn") {
          const session=(await request.get(owner.url+"/v2/workspaces/local/sessions/"+owner.session_id,{headers:AUTH}));
          const original=await session.json();const response=await request.post(owner.url+
            "/v3/workspaces/local/sessions/"+owner.session_id+"/turns",{headers:AUTH,data:{
              schema_version:1,request_key:"old",expected_revision:original.revision,text:"历史无执行请求",
              input:{mode:"head_at_dispatch",revision_id:null},completion:{profile:"python-unittest/1",required_paths:[],conditions:[]}}});
          expect(response.status()).toBe(202);
          const oldTurn=await response.json();
          await expect.poll(async()=> (await request.get(owner.url+
            "/v2/workspaces/local/sessions/"+owner.session_id+"/turns/"+oldTurn.id,
            {headers:AUTH})).json().then(value=>value.status)).toBe("failed");
        }
        await open(page,owner);
        await page.locator("#workspace-input-revision").fill("0");
        await page.getByRole("button",{name:"添加空文件",exact:true}).click();
        const inputEditor=page.locator(".workspace-file-editor");
        await inputEditor.locator("input").fill("hello.txt");
        await inputEditor.locator("textarea").fill("hello");
        await page.getByRole("button",{name:"提交输入快照",exact:true}).click();
        await expect(page.locator(".workspace-artifact")).toContainText("hello.txt");
        const ledger=await page.evaluate(key=>localStorage.getItem(key),KNOWN);expect(ledger).not.toBeNull();
        const identity=Object.values(JSON.parse(ledger!))[0] as {identity:string;manifest_hash:string};
        await page.reload();await connect(page);await openWorkspaceTaskArea(page);
        await expect(page.locator(".workspace-artifact")).toContainText(identity.manifest_hash);
        await navigateView(page,"run");await navigateView(page,"chat");await openWorkspaceTaskArea(page);
        await expect(page.locator(".workspace-artifact")).toContainText("hello.txt");
        const snapshot=await state(request,owner);
        expect(snapshot.http.filter(v=>v.method==="POST"&&v.path.endsWith("/inputs"))).toHaveLength(1);
        expect(snapshot.http.some(v=>v.method==="GET"&&v.path.endsWith("/artifacts/"+identity.identity))).toBe(true);
        await capture(page,info,"confirmed-original-input");
      } catch(error) {bodyFailures.set(info.testId,error);throw error;}
    finally { await finish(page,request,owner,info,errors); }
    });
  for(const boundary of ["post","get"] as const) for(const steering of ["session-return","credential","remount"] as const)
    test(`real_accepted_input_${boundary}_late_${steering}`,async({page,request},info)=>{
      const owner=await prepare(request,"inputs"),errors=observe(page),hold=barrier(),entered=barrier();
      let accepted:Record<string,unknown>|null=null,original:Record<string,unknown>|null=null,armed=true;
      const base=owner.url+"/v3/workspaces/local/sessions/"+owner.session_id;
      try {
        await open(page,owner);
        await page.route(base+"/inputs",async route=>{
          const response=await route.fetch();accepted=await response.json();original=route.request().postDataJSON();
          if(boundary==="post"){entered.release();await hold.promise;await route.fulfill({response});}
          else await route.abort("failed");
        });
        if(boundary==="get") await page.route(base+"/inputs/by-key/*",async route=>{
          const response=await route.fetch();if(armed){armed=false;entered.release();await hold.promise;}
          await route.fulfill({response});
        });
        await page.locator("#workspace-input-revision").fill("0");
        await page.getByRole("button",{name:"提交输入快照",exact:true}).click();
        if(boundary==="get") {await expect(page.getByRole("button",{name:"查找原工作请求",exact:true})).toBeVisible();
          await page.getByRole("button",{name:"查找原工作请求",exact:true}).click();}
        await entered.promise;
        if(steering==="session-return") {
          await selectSession(page,"FINAL工作B");
          await expect(page.locator(".chat-toolbar .toolbar-title h1")).toHaveText("FINAL工作B");
          await selectSession(page,"FINAL工作A");
        } else if(steering==="credential") {await connect(page,"final-peer-token");await connect(page);}
        else {await navigateView(page,"run");await navigateView(page,"chat");}
        await openWorkspaceTaskArea(page);
        hold.release();
        await expect.poll(async()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)??"[]"),INTENTS))
          .toEqual(expect.arrayContaining([expect.objectContaining({body:original})]));
        await page.unroute(base+"/inputs");
        await expect(page.getByRole("button",{name:"查找原工作请求",exact:true})).toBeVisible();
        await page.getByRole("button",{name:"查找原工作请求",exact:true}).click();
        await expect.poll(async()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)??"[]"),INTENTS)).toEqual([]);
        const saved=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)??"{}"),KNOWN);
        expect(Object.values(saved)).toEqual(expect.arrayContaining([expect.objectContaining({identity:accepted!.revision_id})]));
        const snapshot=await state(request,owner);
        expect(snapshot.http.filter(v=>v.method==="POST"&&v.path.endsWith("/inputs"))).toHaveLength(1);
        expect(snapshot.provider_calls).toBe(0);
        await writeFile(info.outputPath("accepted-original.json"),JSON.stringify({original,accepted,snapshot},null,2));
        await capture(page,info,"original-key-recovered");
      } catch(error) {bodyFailures.set(info.testId,error);throw error;}
      finally {hold.release();await finish(page,request,owner,info,errors);}
    });
});
