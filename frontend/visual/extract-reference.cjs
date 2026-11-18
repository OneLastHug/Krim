const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { createRequire } = require('node:module');
const root = path.resolve(__dirname, '../..');
const req = createRequire(path.join(root, 'frontend/package.json'));
const postcss = req('postcss');
const { JSDOM, VirtualConsole } = req('jsdom');
const input = path.join(root, 'docs/prototypes/qa-ui-improvements/index.html');
const html = fs.readFileSync(input, 'utf8');
const css = html.match(/<style>([\s\S]*?)<\/style>/)[1];
const allowed = /^\.(?:icon-button|app-sidebar|sidebar-|session-tree|session-group|session-select|session-more|session-status|chat-session|account-button|account-avatar|account-identity-line|identity-dot|account-gateway|chat-toolbar|actions-divider|toolbar-|chat-workbench|chat-main|chat-history-shell|chat-transcript|chat-thread|chat-turn|chat-user-|chat-answer-actions|chat-message-action|chat-composer|composer-add|chat-compose-|chat-send|chat-controls|control-chip|context-meter|dev-panel-root|dev-rail|g-|dev-context-bar|empty-|chat-empty-guide|popover-menu|menu-|settings-popover|theme-options|reading-size-options|theme-swatch|appearance-|mobile-drawer-sheet|obs-swatch|task-plan|chat-tool|chat-assistant|chat-step-text|message-|chat-answer(?:\s|$)|chat-approval|approval-|interaction-|question-)/;
const scope = ':is(.workbench-shell, .mobile-drawer-content, .session-tree-menu-layer, .dev-dialog, .new-session-project-dialog, .session-sidebar-dialog, .session-search-dialog, .session-project-permission-dialog)';
const prefix = selector => {
  selector = selector.replaceAll('.mobile-drawer-sheet', '.mobile-drawer-content');
  if (selector.startsWith('.mobile-drawer-content')) return selector;
  if (selector.startsWith(':root')) { const i = selector.indexOf(' '); return selector.slice(0, i) + ' ' + scope + selector.slice(i); }
  return scope + ' ' + selector;
};
const output = postcss.root();
const declarations = [];
function copy(container, target, media = '') {
  for (const node of container.nodes ?? []) {
    if (node.type === 'rule') {
      const selectors = node.selectors.filter(s => allowed.test(s.replace(/^:root\S*\s+/, '')));
      if (!selectors.length) continue;
      const clone = node.clone({ selector: selectors.map(prefix).join(', ') });
      if (selectors.includes('.chat-composer #chat-prompt')) clone.walkDecls(d => { if (d.prop === 'height' || d.prop === 'overflow-y') d.remove(); });
      target.append(clone);
      for (const selector of selectors) declarations.push({ media, selector, props: Object.fromEntries(node.nodes.filter(n => n.type === 'decl').map(n => [n.prop,n.value])) });
    } else if (node.type === 'atrule' && node.name === 'media') {
      const clone = node.clone({ nodes: [] }); copy(node, clone, node.params); if (clone.nodes.length) target.append(clone);
    }
  }
}
copy(postcss.parse(css), output);
const provenance = crypto.createHash('sha256').update(html).digest('hex');
const target = path.join(root, 'frontend/src/styles/approved-workbench.css');
fs.writeFileSync(target, `/* Approved HTML visual values, scoped to the workbench.\n * Reference SHA-256: ${provenance}\n * Behavior and real API data remain in native React components.\n */\n` + output.toString() + '\n');
const visualDir = path.join(root, 'frontend/visual'); fs.mkdirSync(visualDir, { recursive: true });
fs.writeFileSync(path.join(visualDir, 'approved-style-contract.json'), JSON.stringify({ reference_sha256: provenance, declarations }, null, 2) + '\n');
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
let script = scriptMatch[1]; const last = script.lastIndexOf('})();'); if (last < 0) throw new Error('Reference closure not found');
const exportCode = `window.__approvedData = {scenarios:state.sessions, groups:state.groups, sessions:state.sessions.map(s=>({id:s.id,title:s.title,projectId:s.projectId,mode:s.mode,status:s.status,lifecycle:s.lifecycle,pinned:s.pinned,parentId:s.parentId})), currentSessionId:state.currentSessionId, projects:PROJECTS, context:currentSession()?.context, outputs:currentSession()?.outputs, tools:MCP_TOOLS.map(t=>({name:t.name,risk:t.risk})), plan:currentSession()?.plan, turns:currentSession()?.turns.map(t=>({id:t.id,runId:t.runId,prompt:t.prompt,answerHtml:t.answerHtml,tools:t.tools.map(tool=>({name:tool.name,args:tool.args,summary:tool.summary,state:tool.state,icon:tool.icon,formatted:tool.formatted}))})),turnKeys:Object.keys(currentSession()?.turns?.[0]||{})};\n`;
script = script.slice(0,last) + exportCode + script.slice(last);
const errors = [];
const vc = new VirtualConsole(); vc.on('jsdomError', e => errors.push(e.message));
const dom = new JSDOM(html.replace(scriptMatch[0], '<script>' + script + '</script>'), { url:'http://localhost:5195/', runScripts:'dangerously', virtualConsole:vc, beforeParse(w) {
  Object.defineProperty(w,'innerWidth',{value:1440,configurable:true}); Object.defineProperty(w,'innerHeight',{value:900,configurable:true});
  w.matchMedia = query => ({matches:query.includes('max-width')?1440<=Number(query.match(/\d+/)?.[0]??0):false, addEventListener(){},removeEventListener(){}});
  w.ResizeObserver = class { observe(){} disconnect(){} };
  w.requestAnimationFrame = cb => { cb(0); return 1; }; w.cancelAnimationFrame=()=>{};
  w.HTMLElement.prototype.scrollIntoView=()=>{};
  w.fetch=()=>Promise.reject(new Error('Reference fixture extraction has no network'));
}});
if (!dom.window.__approvedData) { dom.window.close(); throw new Error('Reference data extraction failed: '+errors.join('; ')); }
const data = dom.window.__approvedData;
data.visibleTurns = [...dom.window.document.querySelectorAll('.chat-turn')].map(e=>({id:e.dataset.turnId,user:e.querySelector('.chat-user-text')?.textContent??'',answer:e.querySelector('.chat-answer')?.textContent??''}));
fs.writeFileSync(path.join(visualDir,'approved-cases.json'), JSON.stringify(data,null,2)+'\n');
dom.window.close();
console.log(JSON.stringify({css:target,rules:declarations.length,reference_sha256:provenance,fixture:{sessions:data.sessions.length,current:data.currentSessionId,turnKeys:data.turnKeys},referenceRuntimeNotes:errors}));
