import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {parseRun,request,RequestError,isUnadmittedCreate} from "../src/api.ts";
const vectors=JSON.parse(readFileSync(new URL("../../tests/core/contracts.json",import.meta.url),"utf8"));
for(const vector of vectors){let valid=true;try{parseRun(vector.value);}catch{valid=false;}assert.equal(valid,vector.valid,vector.name);}
console.log(`${vectors.length} shared agent-core vectors passed`);
const originalFetch=globalThis.fetch;
try{
  globalThis.fetch=async()=>new Response(JSON.stringify({error:{code:"invalid_input"}}),{status:422});
  await assert.rejects(request("/v1/runs","",{}),error=>error instanceof RequestError&&error.status===422&&error.code==="invalid_input");
  globalThis.fetch=async()=>new Response(JSON.stringify({error:{code:"storage_unavailable"}}),{status:503});
  await assert.rejects(request("/v1/runs","",{}),error=>error instanceof RequestError&&error.status===503&&error.code==="storage_unavailable");
  globalThis.fetch=async()=>{throw new TypeError("synthetic transport loss");};
  await assert.rejects(request("/v1/runs","",{}),error=>error instanceof TypeError&&!(error instanceof RequestError));
}finally{globalThis.fetch=originalFetch;}
console.log("HTTP rejection status and unknown transport remain distinct");
for(const [status,code,unadmitted] of [
  [401,"unauthorized",true],[429,"capacity",true],[422,"invalid_input",true],[422,"input_limit",true],
  [404,"not_found",false],[500,"execution_error",false],[503,"storage_unavailable",false],
  [401,"capacity",false],[429,"unauthorized",false],[500,"invalid_input",false],
] as const){assert.equal(isUnadmittedCreate(new RequestError(status,code)),unadmitted,`${status}/${code}`);}
assert.equal(isUnadmittedCreate(new TypeError("unknown transport")),false);
assert.equal(isUnadmittedCreate({status:401,code:"unauthorized"}),false);
console.log("Only exact create rejection status/code pairs prove non-admission; 404 and unknown remain unknown");

const controller=new AbortController();let observedSignal:AbortSignal|null|undefined;
try{
  globalThis.fetch=async(_input,options)=>{observedSignal=options?.signal;return new Response("{}",{status:200});};
  await request("/v1/runs/original","",undefined,controller.signal);assert.equal(observedSignal,controller.signal);
  await request("/v1/runs","",{},controller.signal);assert.equal(observedSignal,undefined);
}finally{globalThis.fetch=originalFetch;}
console.log("GET cancellation signal never transfers to explicit POST");
