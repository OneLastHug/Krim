import { expect,test,type Page } from "@playwright/test";
const headers={ Authorization:"Bearer browser-test-key" };
async function open(page:Page):Promise<void> {
  await page.goto("/ui/");await page.getByLabel("服务访问密钥",{exact:true}).fill("browser-test-key");
  await page.getByRole("button",{name:"连接",exact:true}).click();await expect(page.locator("#connection")).toContainText("本地服务已连接");
}
async function enabled(page:Page):Promise<void> {
  const view=await (await page.request.get("/v1/mcp/servers/browser_oauth",{headers})).json();
  if (!view.profile?.enabled) expect((await page.request.put("/v1/mcp/servers/browser_oauth",{headers,data:{expected_revision:view.profile?.revision||0,resource_hash:view.resource_hash,enabled:true}})).ok()).toBeTruthy();
}
test("发起、Cookie绑定真实回调、只读恢复与Locale",async({page,context})=> {
  await enabled(page);await open(page);const row=page.locator('.oauth-resource[data-name="browser_oauth"]');
  const before=await (await page.request.get("/_test/oauth-counts",{headers})).json();
  page.once("dialog",dialog=>dialog.accept());await row.getByRole("button",{name:"发起MCP浏览器授权",exact:true}).click();
  await expect(row.locator(".oauth-status")).toContainText("等待浏览器授权");
  const cookies=await context.cookies();const binding=cookies.find(cookie=>cookie.name.startsWith("krim-oauth-"));
  expect(binding?.httpOnly).toBe(true);expect(binding?.sameSite).toBe("Lax");
  const stored=await page.evaluate(()=>JSON.stringify({...sessionStorage}));expect(stored).not.toContain(binding?.value||"not-a-cookie");
  await context.route("**/_test/oauth/authorize?*",async route=> {
    const query=new URL(route.request().url()).searchParams;
    await route.fulfill({status:302,headers:{location:query.get("redirect_uri")+"?state="+encodeURIComponent(query.get("state")||"")+"&code=browser-code"}});
  });
  await row.getByRole("link",{name:"打开MCP授权页面",exact:true}).click();
  await expect(row.locator(".oauth-status")).toContainText("浏览器授权完成");
  const after=await (await page.request.get("/_test/oauth-counts",{headers})).json();expect(after.tokens).toBe(before.tokens+1);
  await open(page);await expect(page.locator('.oauth-resource[data-name="browser_oauth"] .oauth-status')).toContainText("浏览器授权完成");
  expect((await (await page.request.get("/_test/oauth-counts",{headers})).json()).tokens).toBe(after.tokens);
  await page.locator("#locale-select").selectOption("en-US");await expect(page.locator('.oauth-resource[data-name="browser_oauth"] .oauth-status')).toContainText("Browser authorization completed");
});
test("丢失发起响应只读查询原intent，取消不交换或重放",async({page})=> {
  await enabled(page);await open(page);const row=page.locator('.oauth-resource[data-name="browser_oauth"]');let starts=0;
  await page.route("**/v1/mcp/servers/browser_oauth/oauth/begin",async route=> {starts+=1;await route.fetch();await route.abort("failed");});
  page.once("dialog",dialog=>dialog.accept());await row.getByRole("button",{name:"发起MCP浏览器授权",exact:true}).click();
  await expect(row.locator(".oauth-status")).toContainText("浏览器授权响应未确认");
  await row.getByRole("button",{name:"查询原授权记录",exact:true}).click();await expect(row.locator(".oauth-status")).toContainText("等待浏览器授权");
  expect(starts).toBe(1);await row.getByRole("button",{name:"取消浏览器授权",exact:true}).click();await expect(row.locator(".oauth-status")).toContainText("浏览器授权已取消");
  expect(starts).toBe(1);
});
