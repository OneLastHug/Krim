import {test,expect,type Page,type BrowserContext} from "@playwright/test";
import {writeFileSync,mkdirSync,readFileSync,existsSync} from "node:fs";
import {resolve} from "node:path";
const evidence=process.env.KRIM_GUIDE_EVIDENCE;
if(!evidence)throw new Error("需要本轮证据根");
const visual=process.env.KRIM_GUIDE_PHASE==="visual";
const folder=(size:string)=>{const path=resolve(evidence,"visual-"+size);mkdirSync(path,{recursive:true});return path;};
async function capture(page:Page,path:string,state:string){
 await page.screenshot({path:resolve(path,state+".png"),fullPage:true,animations:"disabled"});
 const words=await page.evaluate(()=>{
  const records:{text:string;kind:string}[]=[];const walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT);
  let node:Node|null;
  while((node=walker.nextNode())){
   const element=node.parentElement,text=node.textContent?.trim();if(!element||!text||element.closest("script,style,.sr-only,[hidden]"))continue;
   const style=getComputedStyle(element);if(style.visibility==="hidden"||style.display==="none")continue;
   const range=document.createRange();range.selectNodeContents(node);if(!range.getBoundingClientRect().width||!range.getBoundingClientRect().height)continue;
   records.push({text,kind:element.closest("[data-ui-data]")?"user_or_service_data":"fixed_ui"});
  }
  for(const input of document.querySelectorAll<HTMLInputElement|HTMLTextAreaElement>("input,textarea")){if(input.getClientRects().length&&input.value)records.push({text:input instanceof HTMLInputElement&&input.type==="password"?"[密钥已填写]":input.value,kind:"user_or_service_data"});}
  return {viewport:{width:innerWidth,height:innerHeight},bodyText:document.body.innerText,records};
 });
 writeFileSync(resolve(path,state+"-visible-text.json"),JSON.stringify(words,null,2));
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 for(const phrase of ["LC-M1","不代表商业模型","尚未提供","为确保","验收","设计上"]){expect(words.records.filter(r=>r.kind==="fixed_ui").map(r=>r.text).join(" ")).not.toContain(phrase);}
 expect(await page.getByText("离线演示",{exact:true}).count()).toBe(1);
 if(!["connection","trace"].includes(state))await accessibility(page,path,state,false);
}
async function create(page:Page,text:string){await page.locator("#prompt").fill(text);await page.getByRole("button",{name:"创建任务",exact:true}).click();}
async function longInput(page:Page,path:string,text:string){
 const prompt=page.locator("#prompt");await prompt.fill(text);await expect(prompt).toHaveValue(text);
 await page.evaluate(()=>window.scrollTo(0,0));
 const geometry=await page.evaluate(()=>{
  const input=document.querySelector<HTMLTextAreaElement>("#prompt")!;
  const bounds=(selector:string)=>{const r=document.querySelector(selector)!.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height,top:r.top,bottom:r.bottom};};
  return {viewport:{width:innerWidth,height:innerHeight},pageScrollY:scrollY,pageHeight:document.documentElement.scrollHeight,
   prompt:{clientHeight:input.clientHeight,scrollHeight:input.scrollHeight,maxHeight:getComputedStyle(input).maxHeight,overflowY:getComputedStyle(input).overflowY,rect:bounds("#prompt"),length:input.value.length},
   create:bounds("#send"),heading:bounds("#workspace-heading"),controls:bounds(".run-actions")};
 });
 // 在断言前保存原始几何和图片，使旧候选的真实布局失败同样可审查。
 writeFileSync(resolve(path,"long-input-geometry.json"),JSON.stringify(geometry,null,2));
 await capture(page,path,"long-input-top");
 expect(geometry.pageScrollY).toBe(0);
 expect(geometry.prompt.rect.height).toBeLessThanOrEqual(Math.min(320,geometry.viewport.height*.4)+1);
 expect(geometry.prompt.scrollHeight).toBeGreaterThan(geometry.prompt.clientHeight);
 expect(geometry.prompt.overflowY).toBe("auto");
 for(const box of [geometry.create,geometry.heading,geometry.controls]){expect(box.top).toBeGreaterThanOrEqual(0);expect(box.bottom).toBeLessThanOrEqual(geometry.viewport.height);}
 await prompt.focus();await page.keyboard.press("Control+End");
 expect(await prompt.evaluate(input=>(input as HTMLTextAreaElement).selectionStart)).toBe(text.length);
 const keyboardEnd=await prompt.evaluate(input=>({scrollTop:input.scrollTop,scrollHeight:input.scrollHeight,clientHeight:input.clientHeight,selectionStart:(input as HTMLTextAreaElement).selectionStart}));
 // Ctrl+End定位光标；滚轮另外验证包含底部padding在内的真实滚动范围。
 await prompt.hover();await page.mouse.wheel(0,geometry.prompt.scrollHeight);
 await expect.poll(()=>prompt.evaluate(input=>input.scrollHeight-input.clientHeight-input.scrollTop)).toBeLessThanOrEqual(2);
 const scrolledToEnd=await prompt.evaluate(input=>({scrollTop:input.scrollTop,scrollHeight:input.scrollHeight,clientHeight:input.clientHeight}));
 await page.keyboard.insertText("X");await expect(prompt).toHaveValue(text+"X");
 await page.keyboard.press("Backspace");await expect(prompt).toHaveValue(text);
 const editing=await prompt.evaluate(element=>{const input=element as HTMLTextAreaElement;return {length:input.value.length,selectionStart:input.selectionStart,selectionEnd:input.selectionEnd,scrollTop:input.scrollTop,scrollHeight:input.scrollHeight,clientHeight:input.clientHeight,focusVisible:input.matches(":focus-visible"),outlineWidth:getComputedStyle(input).outlineWidth};});
 expect(editing.focusVisible).toBe(true);expect(parseFloat(editing.outlineWidth)).toBeGreaterThanOrEqual(1);
 await page.keyboard.press("Tab");await expect(page.locator("#send")).toBeFocused();
 expect(await page.locator("#send").evaluate(button=>button.matches(":focus-visible"))).toBe(true);
 writeFileSync(resolve(path,"long-input-keyboard.json"),JSON.stringify({keyboardEnd,scrolledToEnd,editing,buttonFocused:true,fullInputPreserved:await prompt.inputValue()===text},null,2));
 await capture(page,path,"long-input-editing");await accessibility(page,path,"long-input-editing");
}
async function accessibility(page:Page,path:string,state:string,keyboardFocus=true){
 const result=await page.evaluate(()=>{
  function rgb(raw:string){const n=raw.match(/[\d.]+/g)?.map(Number)??[];return n.length>=3?n.slice(0,3):[0,0,0];}
  function luminance(c:number[]){return c.map(v=>{v/=255;return v<=.04045?v/12.92:((v+.055)/1.055)**2.4;}).reduce((sum,v,i)=>sum+v*[.2126,.7152,.0722][i]!,0);}
  function ratio(a:number[],b:number[]){const x=luminance(a),y=luminance(b);return (Math.max(x,y)+.05)/(Math.min(x,y)+.05);}
  function background(element:Element){let p:Element|null=element;while(p){const color=getComputedStyle(p).backgroundColor;if(color!=="rgba(0, 0, 0, 0)"&&color!=="transparent")return rgb(color);p=p.parentElement;}return [255,255,255];}
  const text=[];
  for(const element of document.querySelectorAll("h1,h2,p,label,button,[role=tab],summary,pre,code,[role=status],.chat-toolbar .toolbar-title h1,[data-slot=dialog-title]")){
   if(!element.getClientRects().length||element.closest(".sr-only,[hidden]")||element instanceof HTMLButtonElement&&element.disabled)continue;
   if(!element.textContent?.trim())continue;
   const s=getComputedStyle(element);text.push({name:element.textContent.trim().slice(0,100),color:s.color,background:background(element),contrast:ratio(rgb(s.color),background(element)),size:s.fontSize});
  }
  const fields=[...document.querySelectorAll("input,textarea")].map(e=>({id:e.id,label:!!document.querySelector(`label[for="${e.id}"]`),borderContrast:ratio(rgb(getComputedStyle(e).borderColor),background(e))}));
  const targets=[...document.querySelectorAll("button,[role=tab],summary")].filter(e=>e.getClientRects().length).map(e=>({name:e.getAttribute("aria-label")??e.textContent?.trim(),height:e.getBoundingClientRect().height,width:e.getBoundingClientRect().width}));
  const active=document.activeElement,focusStyle=active?getComputedStyle(active):null;
  // 正outline-offset把焦点线画在控件之外，应与外侧背景比较，而非按钮自身填充。
  const focusBackground=active&&focusStyle?background(parseFloat(focusStyle.outlineOffset)>=0?(active.parentElement??active):active):null;
  const focus=active&&focusStyle&&focusBackground?{name:active.getAttribute("aria-label")??active.textContent,outline:focusStyle.outlineColor,outlineWidth:focusStyle.outlineWidth,outlineOffset:focusStyle.outlineOffset,adjacentBackground:focusBackground,visible:active.matches(":focus-visible"),contrast:ratio(rgb(focusStyle.outlineColor),focusBackground)}:null;
  return {text,fields,targets,focus};
 });
 writeFileSync(resolve(path,state+"-accessibility.json"),JSON.stringify(result,null,2));
 expect(result.text.every(row=>row.contrast>=4.5)).toBe(true);
 expect(result.fields.every(row=>row.label&&row.borderContrast>=3)).toBe(true);
 expect(result.targets.every(row=>Boolean(row.name)&&row.height>=43&&row.width>=43)).toBe(true);
 if(keyboardFocus){expect(result.focus?.visible).toBe(true);expect(result.focus?.contrast).toBeGreaterThanOrEqual(3);
 expect(parseFloat(result.focus?.outlineWidth??"0")).toBeGreaterThanOrEqual(1);}
}
for(const size of [{name:"desktop",width:1280,height:900},{name:"narrow",width:360,height:800}]){
 test(`工作台渲染、文案与键盘 ${size.name}`,async({browser})=>{
  test.skip(!visual,"只在独立视觉库阶段运行");
  const context=await browser.newContext({viewport:{width:size.width,height:size.height}});const page=await context.newPage();const path=folder(size.name);const csp:string[]=[];const errors:string[]=[];const requests:string[]=[];
  let extraContext:BrowserContext|null=null;let extraPage:Page|null=null;
  page.on("console",message=>{if(message.type()==="error")errors.push(message.text());});page.on("pageerror",error=>errors.push(error.message));
  page.on("request",request=>requests.push(request.url()));
  await page.addInitScript(()=>document.addEventListener("securitypolicyviolation",event=>{(window as unknown as {__csp:string[]}).__csp??=[];(window as unknown as {__csp:string[]}).__csp.push(event.violatedDirective+":"+event.blockedURI);}));
  try{
   await page.goto(process.env.KRIM_GUIDE_BASE_URL+"/ui/");await expect(page.locator("#result")).toHaveText("暂无任务");await capture(page,path,"default");
   const account=page.getByRole("button",{name:"密钥身份",exact:true});
   const drawer=page.getByRole("dialog",{name:"主导航与对话",exact:true});
   if(!await drawer.isVisible()){const mobileMenu=page.getByRole("button",{name:"打开导航",exact:true});if(await mobileMenu.isVisible()){await mobileMenu.click();await expect(drawer).toBeVisible();}}
   await account.click();await page.getByRole("menuitem",{name:"连接 / 切换密钥"}).click();await expect(page.locator("#api-key")).toBeFocused();await capture(page,path,"connection");await accessibility(page,path,"connection");
   await page.keyboard.press("Tab");await expect(page.getByRole("button",{name:"应用",exact:true})).toBeFocused();await page.keyboard.press("Shift+Tab");await expect(page.locator("#api-key")).toBeFocused();await page.keyboard.press("Escape");await expect(page.getByRole("dialog")).toHaveCount(0);await account.click();await page.keyboard.press("Escape");await expect(account).toBeFocused();
   if(await drawer.isVisible()){await page.keyboard.press("Escape");await expect(drawer).not.toBeVisible();}
   await create(page,"/add 2 3");await expect(page.locator("#result")).toHaveText("5");await capture(page,path,"completed");
   await page.locator(".run-identity > summary").click();await expect(page.locator("#run-id")).toBeVisible();await expect(page.locator("#run-id")).toHaveText(new URL(page.url()).hash.slice(1));await capture(page,path,"identity-expanded");await page.locator(".run-identity > summary").click();
   const resultTab=page.getByRole("tab",{name:"结果",exact:true});await resultTab.focus();await page.keyboard.press("ArrowRight");await expect(page.getByRole("tab",{name:"工具",exact:true})).toBeFocused();await page.keyboard.press("Enter");await expect(page.getByRole("tab",{name:"工具",exact:true})).toHaveAttribute("aria-selected","true");await page.keyboard.press("ArrowRight");await expect(page.getByRole("tab",{name:"Trace",exact:true})).toBeFocused();await page.keyboard.press("Enter");await expect(page.getByRole("tab",{name:"Trace",exact:true})).toHaveAttribute("aria-selected","true");await capture(page,path,"trace");await accessibility(page,path,"trace");
   const longText="LONG_PARAMETER_".repeat(400);await longInput(page,path,"/echo "+longText);await create(page,"/echo "+longText);await expect(page.locator("#result")).toHaveText(longText);expect(await page.locator("#result").evaluate(element=>element.scrollHeight>element.clientHeight)).toBe(true);await page.getByRole("tab",{name:"工具",exact:true}).click();await page.locator(".tool details > summary").click();expect(JSON.parse((await page.locator(".tool details pre").textContent())!).text).toBe(longText);expect(await page.locator(".tool details pre").evaluate(element=>element.scrollHeight>element.clientHeight)).toBe(true);await capture(page,path,"long-details");
   await create(page,"/note welcome");await expect(page.locator("#approval")).toContainText("welcome");await capture(page,path,"approval");
   await page.getByRole("button",{name:/^拒绝 .* 本次调用$/}).click();await expect(page.locator("#status")).toHaveText("失败");await capture(page,path,"failed");
   const beforeSlow=existsSync(resolve(evidence!,"provider-calls.jsonl"))?readFileSync(resolve(evidence!,"provider-calls.jsonl"),"utf8").trim().split("\n").length:0;
   await create(page,"/slow");await expect(page.locator("#status")).toHaveText("执行中");await expect.poll(()=>readFileSync(resolve(evidence!,"provider-calls.jsonl"),"utf8").trim().split("\n").length).toBe(beforeSlow+1);
   await capture(page,path,"running");const runningId=(await page.locator("#run-id").textContent())!.trim();
   await page.reload();await expect(page.locator("#status")).toHaveText("执行中");expect((await (await page.request.get(process.env.KRIM_GUIDE_BASE_URL+"/healthz")).json()).active).toBe(1);
   await page.getByRole("button",{name:"停止",exact:true}).click();await expect(page.locator("#status")).toHaveText("已停止");expect((await (await page.request.get(process.env.KRIM_GUIDE_BASE_URL+"/healthz")).json()).active).toBe(0);
   // 新浏览器存储上下文展示首次创建未知，不把上一个 Run 的状态冒充它。
   csp.push(...await page.evaluate(()=>(window as unknown as {__csp?:string[]}).__csp??[]));
   await context.close();
   const next=await browser.newContext({viewport:{width:size.width,height:size.height}});const unknown=await next.newPage();extraContext=next;extraPage=unknown;
   unknown.on("request",request=>requests.push(request.url()));unknown.on("pageerror",error=>errors.push(error.message));
   await unknown.addInitScript(()=>document.addEventListener("securitypolicyviolation",event=>{(window as unknown as {__csp:string[]}).__csp??=[];(window as unknown as {__csp:string[]}).__csp.push(event.violatedDirective+":"+event.blockedURI);}));
   let lost=false,posts=0,accepted="";
   await unknown.route("**/v1/runs",async route=>{const response=await route.fetch();accepted=(await response.json()).id;posts++;await route.abort("failed");});
   await unknown.route("**/v1/runs/by-key/*",async route=>{if(!lost){lost=true;await route.abort("failed");}else await route.continue();});
   await unknown.goto(process.env.KRIM_GUIDE_BASE_URL+"/ui/");await create(unknown,"/add 2 3");await expect(unknown.locator(".notice")).toContainText("创建结果未确认");await capture(unknown,path,"unknown");
   expect(await unknown.locator("#prompt").inputValue()).toBe("/add 2 3");await unknown.getByRole("button",{name:"查找任务",exact:true}).click();await expect(unknown.locator("#result")).toHaveText("5");expect((await unknown.locator("#run-id").textContent())!.trim()).toBe(accepted);expect(posts).toBe(1);
   csp.push(...await unknown.evaluate(()=>(window as unknown as {__csp?:string[]}).__csp??[]));await next.close();extraContext=null;extraPage=null;
   writeFileSync(resolve(path,"runtime-observed.json"),JSON.stringify({runningId,unknownAcceptedId:accepted,unknownPosts:posts,csp,errors,requests},null,2));
   expect(csp).toEqual([]);expect(errors).toEqual([]);expect(requests.every(url=>url.startsWith(process.env.KRIM_GUIDE_BASE_URL!))).toBe(true);
  }finally{if(extraPage&&!extraPage.isClosed()){try{csp.push(...await extraPage.evaluate(()=>(window as unknown as {__csp?:string[]}).__csp??[]));}catch(error){errors.push(String(error));}}if(extraContext)await extraContext.close();if(!page.isClosed()){try{csp.push(...await page.evaluate(()=>(window as unknown as {__csp?:string[]}).__csp??[]));}catch(error){errors.push(String(error));}writeFileSync(resolve(path,"runtime-finally.json"),JSON.stringify({csp,errors,requests},null,2));}await context.close();}
 });
}
