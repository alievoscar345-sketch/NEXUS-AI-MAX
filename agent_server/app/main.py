from __future__ import annotations
import asyncio, ipaddress, json, os, socket, sqlite3, time, uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from .model_gateway import ModelGateway

DB_PATH = Path(os.getenv("NEXUS_AGENT_DB", "./data/nexus-agent.db"))
WORKSPACE = Path(os.getenv("NEXUS_AGENT_WORKSPACE", "./data/workspace")).resolve()
API_KEY = os.getenv("NEXUS_AGENT_API_KEY", "")
LLM_BASE = os.getenv("NEXUS_LLM_BASE_URL", "https://anymodel.org/v1").rstrip("/")
LLM_MODEL = os.getenv("NEXUS_LLM_MODEL", "claude-fable-5-2")
LLM_KEY = os.getenv("NEXUS_LLM_API_KEY", "")
TAVILY_KEY = os.getenv("TAVILY_API_KEY", "")
MAX_STEPS = max(1, min(int(os.getenv("NEXUS_AGENT_MAX_STEPS", "12")), 30))
MAX_SECONDS = max(60, min(int(os.getenv("NEXUS_AGENT_MAX_TASK_SECONDS", "1800")), 7200))
WORKSPACE.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def db():
    c = sqlite3.connect(DB_PATH, timeout=20)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,prompt TEXT NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,plan_json TEXT NOT NULL DEFAULT '[]',results_json TEXT NOT NULL DEFAULT '[]',summary TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,created_at REAL NOT NULL,kind TEXT NOT NULL,message TEXT NOT NULL,FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS approvals(id TEXT PRIMARY KEY,task_id TEXT NOT NULL,description TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at REAL NOT NULL,FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE);
        """)

def event(tid, kind, message):
    with db() as c:
        c.execute("INSERT INTO events(task_id,created_at,kind,message) VALUES(?,?,?,?)",(tid,time.time(),kind,message))
        c.execute("UPDATE tasks SET updated_at=? WHERE id=?",(time.time(),tid))

def serialize(row):
    return {"id":row["id"],"prompt":row["prompt"],"status":row["status"],"created_at":row["created_at"],"updated_at":row["updated_at"],"plan":json.loads(row["plan_json"]),"results":json.loads(row["results_json"]),"summary":row["summary"],"error":row["error"]}

def auth(authorization: str | None = Header(default=None)):
    if not API_KEY: raise HTTPException(503,"Agent API is not configured")
    if authorization != "Bearer "+API_KEY: raise HTTPException(401,"Invalid agent API key")

class NewTask(BaseModel):
    prompt: str = Field(min_length=3,max_length=12000)
    autonomy: str = Field(default="level3",pattern="^level[123]$")
    max_steps: int = Field(default=8,ge=1,le=30)
class Approval(BaseModel):
    approved: bool
    note: str = Field(default="",max_length=1000)

_model_gateway = ModelGateway()

async def llm_json(system: str, user: str, purpose: str = "general", task_id: str | None = None) -> dict[str,Any]:
    parsed, result = await _model_gateway.complete_json(system, user, purpose)
    if task_id:
        usage = result.usage
        event(task_id, "model_call", json.dumps({
            "purpose": purpose,
            "provider": result.provider_id,
            "model": result.model,
            "usage": usage,
        }, ensure_ascii=False)[:1500])
    return parsed

def safe_url(value: str) -> str:
    p=urlparse(value)
    if p.scheme!="https" or not p.hostname or p.username or p.password: raise ValueError("Only public HTTPS URLs are allowed")
    host=p.hostname.lower()
    if host in {"localhost","metadata.google.internal"} or host.endswith((".local",".internal")): raise ValueError("Private network destinations are blocked")
    try:
        for item in socket.getaddrinfo(host,443,type=socket.SOCK_STREAM):
            if not ipaddress.ip_address(item[4][0]).is_global: raise ValueError("Private IP destinations are blocked")
    except socket.gaierror as exc: raise ValueError("Unable to resolve destination host") from exc
    return value

async def run_step(step, tid):
    action=str(step.get("action",""))
    args=step.get("input") if isinstance(step.get("input"),dict) else {}
    title=str(step.get("title",action))[:160]
    if action=="web_search":
        if not TAVILY_KEY: return {"title":title,"status":"blocked","detail":"TAVILY_API_KEY is not configured"}
        async with httpx.AsyncClient(timeout=30) as client:
            r=await client.post("https://api.tavily.com/search",json={"api_key":TAVILY_KEY,"query":str(args.get("query",""))[:500],"max_results":5})
            r.raise_for_status()
        return {"title":title,"status":"completed","data":r.json().get("results",[])}
    if action=="fetch_url":
        url=safe_url(str(args.get("url","")))
        async with httpx.AsyncClient(timeout=25,follow_redirects=False,headers={"User-Agent":"NEXUS-Agent/0.1"}) as client:
            r=await client.get(url); r.raise_for_status()
            if "text" not in r.headers.get("content-type","") and "json" not in r.headers.get("content-type",""): raise ValueError("Only text/JSON pages can be fetched")
        return {"title":title,"status":"completed","url":url,"content":r.text[:20000]}
    if action=="save_artifact":
        name=Path(str(args.get("filename","result.md"))).name[:100]
        if not name or name in {".",".."}: raise ValueError("Invalid filename")
        target=(WORKSPACE/(tid+"-"+name)).resolve()
        if target.parent!=WORKSPACE: raise ValueError("Invalid artifact path")
        target.write_text(str(args.get("content",""))[:100000],encoding="utf-8")
        return {"title":title,"status":"completed","artifact":target.name}
    if action=="draft_external_action":
        approval_id=str(uuid.uuid4())
        description=str(args.get("description","External action requested"))[:1000]
        with db() as c:
            c.execute("INSERT INTO approvals(id,task_id,description,created_at) VALUES(?,?,?,?)",(approval_id,tid,description,time.time()))
            c.execute("UPDATE tasks SET status='awaiting_approval',updated_at=? WHERE id=?",(time.time(),tid))
        event(tid,"approval_required",description)
        return {"title":title,"status":"awaiting_approval","approval_id":approval_id,"detail":"No external action was executed. A service-specific connector and approval workflow are required."}
    raise ValueError("Unsupported tool: "+action)

async def process(tid):
    with db() as c: row=c.execute("SELECT * FROM tasks WHERE id=?",(tid,)).fetchone()
    if not row or row["status"] not in {"queued","running"}: return
    started=time.monotonic()
    try:
        with db() as c: c.execute("UPDATE tasks SET status='planning',updated_at=? WHERE id=?",(time.time(),tid))
        event(tid,"status","Planning task")
        plan=await llm_json(
            'You are NEXUS Agent Planner. Return strict JSON only: {"summary":"...","steps":[{"title":"...","action":"web_search|fetch_url|save_artifact|draft_external_action","input":{}}]}. Use only listed actions. For publishing, messaging, purchases or account changes use draft_external_action. Never claim external actions succeeded.',
            row["prompt"], purpose="planning", task_id=tid)
        steps=plan.get("steps",[])
        if not isinstance(steps,list): raise ValueError("Planner did not return steps")
        steps=steps[:min(MAX_STEPS,8)]
        with db() as c: c.execute("UPDATE tasks SET status='running',plan_json=? WHERE id=?",(json.dumps(steps,ensure_ascii=False),tid))
        results=[]
        for i,step in enumerate(steps):
            if time.monotonic()-started>MAX_SECONDS: raise TimeoutError("Task time limit reached")
            if not isinstance(step,dict): continue
            event(tid,"step",str(i+1)+"/"+str(len(steps))+": "+str(step.get("title","Step"))[:150])
            result=await run_step(step,tid); results.append(result)
            with db() as c: c.execute("UPDATE tasks SET results_json=?,updated_at=? WHERE id=?",(json.dumps(results,ensure_ascii=False),time.time(),tid))
            if result.get("status")=="awaiting_approval": return
        final=await llm_json(
            'Return strict JSON only: {"summary":"..."}. Summarize only supplied results. Explicitly identify blocked or unavailable actions; do not claim they succeeded.',
            json.dumps({"goal":row["prompt"],"results":results},ensure_ascii=False)[:30000], purpose="evaluation", task_id=tid)
        with db() as c: c.execute("UPDATE tasks SET status='completed',summary=?,results_json=?,updated_at=? WHERE id=?",(str(final.get("summary","Task finished"))[:12000],json.dumps(results,ensure_ascii=False),time.time(),tid))
        event(tid,"completed","Task completed")
    except Exception as exc:
        with db() as c: c.execute("UPDATE tasks SET status='failed',error=?,updated_at=? WHERE id=?",(str(exc)[:2000],time.time(),tid))
        event(tid,"error",str(exc)[:1000])

async def worker():
    while True:
        tid=None
        with db() as c:
            row=c.execute("SELECT id FROM tasks WHERE status='queued' ORDER BY created_at LIMIT 1").fetchone()
            if row:
                tid=row["id"]; c.execute("UPDATE tasks SET status='running',updated_at=? WHERE id=?",(time.time(),tid))
        if tid: await process(tid)
        else: await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app):
    init_db(); task=asyncio.create_task(worker())
    yield
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass

app=FastAPI(title="NEXUS Agent Core",version="0.1.0",lifespan=lifespan)
@app.get("/health")
async def health(): return {"status":"ok","service":"nexus-agent-core","version":"0.1.0"}
@app.post("/v1/tasks",dependencies=[Depends(auth)])
async def create_task(body: NewTask):
    tid=str(uuid.uuid4()); now=time.time()
    with db() as c: c.execute("INSERT INTO tasks(id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?)",(tid,body.prompt,"queued",now,now))
    event(tid,"created","Task queued in cloud")
    return {"id":tid,"status":"queued"}
@app.get("/v1/tasks",dependencies=[Depends(auth)])
async def list_tasks(limit:int=30):
    with db() as c: rows=c.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?",(max(1,min(limit,100)),)).fetchall()
    return {"tasks":[serialize(r) for r in rows]}
@app.get("/v1/tasks/{tid}",dependencies=[Depends(auth)])
async def get_task(tid:str):
    with db() as c:
        row=c.execute("SELECT * FROM tasks WHERE id=?",(tid,)).fetchone()
        if not row: raise HTTPException(404,"Task not found")
        events=c.execute("SELECT created_at,kind,message FROM events WHERE task_id=? ORDER BY id",(tid,)).fetchall()
    return {**serialize(row),"events":[dict(x) for x in events]}
@app.post("/v1/tasks/{tid}/cancel",dependencies=[Depends(auth)])
async def cancel_task(tid:str):
    with db() as c:
        row=c.execute("SELECT status FROM tasks WHERE id=?",(tid,)).fetchone()
        if not row: raise HTTPException(404,"Task not found")
        if row["status"] in {"completed","failed","cancelled"}: raise HTTPException(409,"Task already finished")
        c.execute("UPDATE tasks SET status='cancelled',updated_at=? WHERE id=?",(time.time(),tid))
    event(tid,"cancelled","Cancelled by user")
    return {"id":tid,"status":"cancelled"}
@app.get("/v1/approvals",dependencies=[Depends(auth)])
async def list_approvals():
    with db() as c: rows=c.execute("SELECT * FROM approvals WHERE status='pending' ORDER BY created_at").fetchall()
    return {"approvals":[dict(r) for r in rows]}
@app.post("/v1/approvals/{approval_id}",dependencies=[Depends(auth)])
async def decide_approval(approval_id:str,body:Approval):
    with db() as c:
        row=c.execute("SELECT * FROM approvals WHERE id=?",(approval_id,)).fetchone()
        if not row: raise HTTPException(404,"Approval not found")
        status="approved" if body.approved else "rejected"
        c.execute("UPDATE approvals SET status=? WHERE id=?",(status,approval_id))
        c.execute("UPDATE tasks SET status='paused',updated_at=? WHERE id=?",(time.time(),row["task_id"]))
    event(row["task_id"],"approval_decision",status+": "+body.note)
    return {"approval_id":approval_id,"status":status,"detail":"Decision recorded; no third-party action is executed by this generic backend."}
