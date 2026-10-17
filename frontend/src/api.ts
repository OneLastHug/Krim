export interface RunData { id:string; request_key:string; status:string; trace_id:string; root_span_id:string; created_at:number; finished_at:number|null; code:string|null; stop_requested:boolean }
const keys=["id","request_key","status","trace_id","root_span_id","created_at","finished_at","code","stop_requested"];
export function object(value:unknown):Record<string,unknown>{
  if(typeof value!=="object"||value===null||Array.isArray(value))throw new Error("响应结构无效");
  return value as Record<string,unknown>;
}
export function parseRun(value:unknown):RunData{
  const r=object(value);
  if(Object.keys(r).some(k=>!keys.includes(k))||keys.some(k=>!(k in r))||typeof r.id!=="string"||typeof r.request_key!=="string"||typeof r.trace_id!=="string"||typeof r.root_span_id!=="string"||! ["queued","running","completed","failed","aborted","interrupted"].includes(String(r.status))||typeof r.created_at!=="number"||!Number.isFinite(r.created_at)||typeof r.stop_requested!=="boolean"||(r.finished_at!==null&&(typeof r.finished_at!=="number"||!Number.isFinite(r.finished_at)))||(r.code!==null&&typeof r.code!=="string"))throw new Error("运行响应无效");
  return r as unknown as RunData;
}
export class RequestError extends Error{
  readonly status:number;readonly code:string;
  constructor(status:number,code:string){super("请求未完成："+code);this.status=status;this.code=code;}
}
export function isUnadmittedCreate(error:unknown):error is RequestError{
  // 仅用于首次创建 POST 的明确拒绝；查询 404 或其它错误不证明未准入。
  return error instanceof RequestError&&(
    (error.status===422&&["invalid_input","input_limit"].includes(error.code))||
    (error.status===401&&error.code==="unauthorized")||
    (error.status===429&&error.code==="capacity")
  );
}
export async function request(path:string,key:string,body?:unknown,signal?:AbortSignal):Promise<unknown>{
  const headers:Record<string,string>={};if(key)headers.Authorization="Bearer "+key;
  if(body!==undefined)headers["Content-Type"]="application/json";
  const response=await fetch(path,{method:body===undefined?"GET":"POST",headers,body:body===undefined?undefined:JSON.stringify(body),redirect:"error",signal:body===undefined?signal:undefined});
  const value:unknown=await response.json();
  if(!response.ok){const err=object(object(value).error);throw new RequestError(response.status,/^[a-z_]{1,64}$/.test(String(err.code))?String(err.code):"execution_error");}
  return value;
}
