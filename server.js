/**
 * Nexus CX v1 — Zero dependencies, pure Node.js
 * Run: node server.js
 */
const http   = require('http');
const https  = require('https');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');
const tls    = require('tls');
const url    = require('url');


const GMAIL_CLIENT_ID = process.env.GMAIL_CLIENT_ID || '';
const GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET || '';
// Always use the correct redirect URI - must match Google Console exactly
const GMAIL_REDIRECT = process.env.NODE_ENV === 'production' || process.env.RAILWAY_ENVIRONMENT
  ? 'process.env.BASE_URL || 'http://localhost:3000'/api/gmail/oauth/callback'
  : 'http://localhost:3000/api/gmail/oauth/callback';
const GMAIL_SCOPES='https://www.googleapis.com/auth/gmail.modify';

const PORT = process.env.PORT || 3000;
// Use /data volume if available (Railway persistent volume), else local
// PERSISTENT STORAGE
// Try /data first (Railway volume mount), then fall back to project dir
function findDataDir() {
  const candidates = [
    process.env.RAILWAY_VOLUME_MOUNT_PATH,
    '/data',
    process.env.DATA_DIR,
    __dirname
  ];
  for (const d of candidates) {
    if (!d) continue;
    try {
      if (!fs.existsSync(d)) fs.mkdirSync(d, {recursive:true});
      // Test write access
      fs.writeFileSync(require('path').join(d, '.write-test'), '1');
      fs.unlinkSync(require('path').join(d, '.write-test'));
      console.log('[DB] Using data dir:', d);
      return d;
    } catch(e) { continue; }
  }
  // Final fallback: /tmp is always writable (survives restarts, not deploys)
  console.log('[DB] WARNING: Using /tmp - data will not survive deploys. Add Railway Volume!');
  return '/tmp';
}
const DATA_DIR = findDataDir();
const DATA = path.join(DATA_DIR, 'data.json');
const CFG  = path.join(DATA_DIR, 'config.json');
const PUB  = path.join(__dirname, 'public');

const genId = () => crypto.randomUUID();
const hash  = p => crypto.createHash('sha256').update(String(p)).digest('hex');
const now   = () => new Date().toISOString();

function readDB() {
  try {
    if (fs.existsSync(DATA)) {
      const raw = fs.readFileSync(DATA, 'utf8');
      if (raw && raw.trim().length > 2) {
        const d = JSON.parse(raw);
        if (d && typeof d === 'object') return d;
      }
    }
  } catch(e) { console.error('[DB] Read error:', e.message); }
  // Try backup
  try {
    const bak = DATA + '.bak';
    if (fs.existsSync(bak)) {
      const raw = fs.readFileSync(bak, 'utf8');
      const d = JSON.parse(raw);
      if (d && d.users) { console.log('[DB] Restored from backup'); return d; }
    }
  } catch(e) {}
  return null;
}
function writeDB(d) {
  try {
    const str = JSON.stringify(d, null, 2);
    // Write directly (atomic enough for our use case, avoids cross-device rename issues)
    fs.writeFileSync(DATA, str);
    // Keep a backup copy
    try { fs.writeFileSync(DATA + '.bak', str); } catch(e) {}
  } catch(e) { console.error('[DB] Write error:', e.message); }
}

function initDB() {
  const d = {
    users:[], agents:[], convs:[], messages:[], customers:[],
    tags:[], canned:[], botRules:[], announcements:[], seenUids:[],
    socialAccounts:[], ticketLogic:[], groups:[], agentLogs:[],
    counters:{ email:0, chat:0, social:0 }
  };
  d.users.push({ id:genId(), name:'Admin', email:'admin@nexus.com', password:hash('admin123'), role:'admin', channel:'both', visibility:'all', phone:'', status:'online', avatar:'', created:now() });
  d.agents.push({ id:genId(), name:'Admin', email:'admin@nexus.com', role:'admin', channel:'both', status:'online', created:now() });
  for(const [name,color] of [['urgent','#ef4444'],['billing','#f59e0b'],['appointment','#3b82f6'],['vip','#8b5cf6'],['follow-up','#10b981']])
    d.tags.push({ id:genId(), name, color, created:now() });
  d.canned.push({ id:genId(), title:'Greeting', shortcut:'/hi', content:'Hello! Thanks for reaching out! How can I help you today?', created:now() });
  d.canned.push({ id:genId(), title:'Appointment', shortcut:'/appt', content:'To schedule an appointment, please share your preferred date and time.', created:now() });
  d.canned.push({ id:genId(), title:'Closing', shortcut:'/bye', content:'Thank you for contacting us. Is there anything else I can help you with?', created:now() });
  d.botRules.push({ id:genId(), keyword:'appointment', response:'Our team can help schedule an appointment. Please share your preferred date/time.', active:true, created:now() });
  d.botRules.push({ id:genId(), keyword:'emergency',   response:'If this is a medical emergency, please call 112 immediately.', active:true, created:now() });
  d.announcements.push({ id:genId(), title:'Welcome to Nexus v6!', body:'Ticket IDs, activity logs, ticket logic — all new in v6.', pinned:true, acks:[], created:now() });
  d.ticketLogic.push({ id:genId(), channel:'email', event:'customer_reply_after_resolve', condition:'days_since_resolve', value:7, action:'create_new', label:'After 7 days → New ticket', active:true, created:now() });
  d.ticketLogic.push({ id:genId(), channel:'email', event:'customer_reply_after_resolve', condition:'days_since_resolve', value:0, action:'reopen', label:'Within 7 days → Reopen same ticket', active:true, created:now() });
  d.ticketLogic.push({ id:genId(), channel:'chat',  event:'customer_reply_after_resolve', condition:'days_since_resolve', value:1, action:'create_new', label:'After 1 day → New chat', active:true, created:now() });
  writeDB(d); return d;
}

function upgradeDB() {
  const d = db.get();
  let dirty = false;
  if(!d.counters){ d.counters={email:0,chat:0,social:0}; dirty=true; }
  if(!d.ticketLogic){ d.ticketLogic=[]; dirty=true; }
  if(!d.agentLogs){ d.agentLogs=[]; dirty=true; }
  if(!d.socialAccounts){ d.socialAccounts=[]; dirty=true; }
  if(d.ticketLogic.length===0){
    d.ticketLogic.push({ id:genId(), channel:'email', event:'customer_reply_after_resolve', condition:'days_since_resolve', value:7, action:'create_new', label:'After 7 days → New ticket', active:true, created:now() });
    d.ticketLogic.push({ id:genId(), channel:'email', event:'customer_reply_after_resolve', condition:'days_since_resolve', value:0, action:'reopen', label:'Within 7 days → Reopen same ticket', active:true, created:now() });
    d.ticketLogic.push({ id:genId(), channel:'chat', event:'customer_reply_after_resolve', condition:'days_since_resolve', value:1, action:'create_new', label:'After 1 day → New chat', active:true, created:now() });
    dirty=true;
  }
  if(!d.tags||d.tags.length===0){
    d.tags=[];
    for(const [name,color] of [['urgent','#ef4444'],['billing','#f59e0b'],['appointment','#3b82f6'],['vip','#8b5cf6'],['follow-up','#10b981']])
      d.tags.push({ id:genId(), name, color, created:now() });
    dirty=true;
  }
  if(!d.canned||d.canned.length===0){
    d.canned=[];
    d.canned.push({ id:genId(), title:'Greeting', shortcut:'/hi', content:'Hello! Thanks for reaching out! How can I help you today?', created:now() });
    d.canned.push({ id:genId(), title:'Appointment', shortcut:'/appt', content:'To schedule an appointment, please share your preferred date and time.', created:now() });
    d.canned.push({ id:genId(), title:'Closing', shortcut:'/bye', content:'Thank you for contacting us. Is there anything else I can help you with?', created:now() });
    dirty=true;
  }
  const channels = {email:'TKT',chat:'CHT',social:'SOC'};
  for(const conv of (d.convs||[])){
    if(!conv.ticketId){
      const prefix = channels[conv.channel]||'TKT';
      d.counters[conv.channel] = (d.counters[conv.channel]||0) + 1;
      conv.ticketId = `${prefix}-${String(d.counters[conv.channel]).padStart(4,'0')}`;
      dirty=true;
    }
    if(!conv.activity) { conv.activity=[{type:'created',text:'Ticket created',time:conv.created||now()}]; dirty=true; }
  }
  if(dirty) writeDB(d);
}

const db = {
  get:    ()      => readDB() || initDB(),
  all:    (t)     => { const d=db.get(); return (d[t]||[]).sort((a,b)=>new Date(b.updated||b.created)-new Date(a.updated||a.created)); },
  find:   (t,id)  => (db.get()[t]||[]).find(x=>x.id===id)||null,
  findBy: (t,k,v) => (db.get()[t]||[]).find(x=>x[k]===v)||null,
  insert: (t,row) => { const d=db.get(); if(!d[t])d[t]=[]; d[t].push(row); writeDB(d); return row; },
  update: (t,id,patch) => { const d=db.get(); const i=(d[t]||[]).findIndex(x=>x.id===id); if(i===-1)return null; d[t][i]={...d[t][i],...patch}; writeDB(d); return d[t][i]; },
  remove: (t,id)  => { const d=db.get(); d[t]=(d[t]||[]).filter(x=>x.id!==id); writeDB(d); },
  uidSeen: (u)    => (db.get().seenUids||[]).includes(String(u)),
  markUid: (u)    => { const d=db.get(); if(!d.seenUids)d.seenUids=[]; const s=String(u); if(!d.seenUids.includes(s)){d.seenUids.push(s);if(d.seenUids.length>5000)d.seenUids=d.seenUids.slice(-2500);writeDB(d);} },
  nextId: (channel) => {
    const d=db.get(); if(!d.counters)d.counters={email:0,chat:0,social:0};
    d.counters[channel]=(d.counters[channel]||0)+1;
    writeDB(d);
    const prefix={email:'TKT',chat:'CHT',social:'SOC'}[channel]||'TKT';
    return `${prefix}-${String(d.counters[channel]).padStart(4,'0')}`;
  },
  addActivity: (convId, entry) => {    // Also write to audit log
    try{const d3=readDB()||initDB();if(!d3.auditLog)d3.auditLog=[];d3.auditLog.push({...entry,convId,ts:new Date().toISOString()});if(d3.auditLog.length>2000)d3.auditLog=d3.auditLog.slice(-1500);writeDB(d3);}catch(e){}
    const d=db.get(); const i=(d.convs||[]).findIndex(x=>x.id===convId);
    if(i===-1)return;
    if(!d.convs[i].activity)d.convs[i].activity=[];
    d.convs[i].activity.push({...entry,time:now()});
    writeDB(d);
  }
};
db.get();
upgradeDB();

const getCfg = () => { try { return fs.existsSync(CFG)?JSON.parse(fs.readFileSync(CFG,'utf8')):{} } catch { return {}; } };
const setCfg = c => fs.writeFileSync(CFG, JSON.stringify(c,null,2));

const sessions = new Map();

// Load persisted sessions from data.json on startup
function loadPersistedSessions() {
  try {
    const d = db.get();
    if (d.sessions) {
      const now = Date.now();
      for (const [id, sess] of Object.entries(d.sessions)) {
        // Only load sessions less than 24 hours old
        if (sess.created && (now - sess.created) < 86400000) {
          sessions.set(id, sess);
        }
      }
      console.log('[Sessions] Restored', sessions.size, 'sessions from disk');
    }
  } catch(e) { console.log('[Sessions] Could not restore sessions:', e.message); }
}

function persistSessions() {
  try {
    const d = db.get();
    const sessObj = {};
    for (const [id, sess] of sessions.entries()) {
      if (sess.uid) sessObj[id] = sess; // only persist authenticated sessions
    }
    d.sessions = sessObj;
    writeDB(d);
  } catch(e) {}
}

function makeSession() {
  const id = crypto.randomBytes(32).toString('hex');
  const sess = {id, uid:null, created:Date.now()};
  sessions.set(id, sess);
  return id;
}

function getSession(req) {
  const m = (req.headers.cookie||'').match(/nsid=([a-f0-9]{64})/);
  if (!m) return null;
  return sessions.get(m[1]) || null;
}

const sseClients = new Set();
function broadcast(event, data) { const msg=`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`; for(const r of sseClients){try{r.write(msg);}catch(_){}} }

const MIME = {'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.ico':'image/x-icon','.svg':'image/svg+xml','.json':'application/json'};
function serveFile(res,fp) { try{const d=fs.readFileSync(fp);res.writeHead(200,{'Content-Type':MIME[path.extname(fp)]||'text/plain','Cache-Control':'no-cache'});res.end(d);}catch{res.writeHead(404);res.end('Not found');} }
function json(res,data,code=200) { res.writeHead(code,{'Content-Type':'application/json','Cache-Control':'no-cache'});res.end(JSON.stringify(data)); }
function readBody(req) { return new Promise(resolve=>{let b='';req.on('data',c=>b+=c);req.on('end',()=>{try{resolve(JSON.parse(b));}catch{resolve({});}});}); }
function requireAuth(req,res) { const s=getSession(req); if(!s||!s.uid){json(res,{error:'Please log in'},401);return null;} return s; }

function httpGet(opts, redirectCount=0) {
  return new Promise((resolve,reject)=>{
    const req=https.request(opts,r=>{
      if((r.statusCode===301||r.statusCode===302||r.statusCode===307||r.statusCode===308)&&r.headers.location&&redirectCount<5){
        const loc=new URL(r.headers.location);
        r.resume();
        return resolve(httpGet({hostname:loc.hostname,path:loc.pathname+loc.search,headers:opts.headers||{}},redirectCount+1));
      }
      let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d));}catch{resolve({_raw:d});} });
    });
    req.on('error',reject); req.setTimeout(15000,()=>{req.destroy();reject(new Error('timeout'));}); req.end();
  });
}

function autoAssign(conv, agentName='') {
  const cfg = getCfg();
  const ch = conv.channel || 'chat';
  const modeKey = ch === 'email' ? 'email_assign_mode' : ch === 'social' ? 'social_assign_mode' : 'chat_assign_mode';
  const loadKey = ch === 'email' ? 'email_max_load' : ch === 'social' ? 'social_max_load' : 'chat_max_load';
  const rrKey   = ch === 'email' ? 'email_rr_index' : ch === 'social' ? 'social_rr_index' : 'chat_rr_index';
  const mode = cfg[modeKey] || cfg.assignment_mode || 'none';
  if(mode === 'none') return;
  const users = db.all('users');
  const agents = db.all('agents').filter(a => {
    if(!a.name || !a.email) return false;
    const user = users.find(u => u.email === a.email);
    if(!user || user.status === 'offline') return false;
    const ch = a.channel || 'both';
    if(conv.channel === 'chat'  && ch !== 'chat'  && ch !== 'both') return false;
    if(conv.channel === 'email' && ch !== 'email' && ch !== 'both') return false;
    if(conv.channel === 'social'&& ch !== 'social'&& ch !== 'both') return false;
    return true;
  });
  if(!agents.length) return;
  let chosen = null;
  if(mode === 'roundrobin') {
    const maxLoad = cfg[loadKey] || 10;
    const openConvs = db.all('convs').filter(c=>c.status==='open'&&c.agent);
    const loadMap = {};
    openConvs.forEach(c=>{ loadMap[c.agent]=(loadMap[c.agent]||0)+1; });
    // Round robin but still respect concurrency cap
    const available = agents.filter(a=>(loadMap[a.name]||0) < maxLoad);
    if(!available.length) return;
    const idx = (cfg[rrKey] || 0) % available.length;
    chosen = available[idx];
    const c = getCfg(); c[rrKey] = (idx + 1) % available.length; setCfg(c);
  } else if(mode === 'load') {
    const maxLoad = cfg[loadKey] || 10;
    const openConvs = db.all('convs').filter(c=>c.status==='open'&&c.agent);
    const loadMap = {};
    openConvs.forEach(c=>{ loadMap[c.agent]=(loadMap[c.agent]||0)+1; });
    const available = agents.filter(a=>(loadMap[a.name]||0) < maxLoad);
    if(!available.length) return;
    chosen = available.reduce((a,b)=>(loadMap[a.name]||0)<=(loadMap[b.name]||0)?a:b);
  }
  if(chosen) {
    db.update('convs', conv.id, { agent: chosen.name, updated: now() });
    db.addActivity(conv.id, {type:'assigned', text:`Auto-assigned to ${chosen.name}`, agent:'System'});
    broadcast('conv_update', db.find('convs', conv.id));
  }
}

function filterConvsByVisibility(convs, user) {
  if(!user || user.role === 'admin') return convs;
  const vis = user.visibility || 'all';
  if(vis === 'all') return convs;
  if(vis === 'assigned') return convs.filter(c => c.agent === user.name);
  if(vis === 'flagged')  return convs.filter(c => c.status === 'flagged' || c.agent === user.name);
  if(vis === 'unassigned') return convs.filter(c => !c.agent || c.agent === user.name);
  return convs;
}

function applyTicketLogic(conv, agentName='System') {
  const d = db.get();
  const rules = (d.ticketLogic||[]).filter(r=>r.active && (r.channel===conv.channel||r.channel==='all'));
  if(!rules.length) return 'reopen';
  const daysSince = (new Date() - new Date(conv.updated||conv.created)) / 86400000;
  const sorted = [...rules].sort((a,b)=>Number(b.value)-Number(a.value));
  for(const rule of sorted) {
    if(rule.event === 'customer_reply_after_resolve') {
      if(daysSince >= Number(rule.value)) return rule.action;
    }
  }
  return 'reopen';
}

function decodeImapStr(s) {
  if(!s) return '';
  return s.replace(/=\?([^?]+)\?([BbQq])\?([^?]*)\?=/g,(_,cs,enc,data)=>{
    try {
      if(enc.toUpperCase()==='B') return Buffer.from(data,'base64').toString('utf8');
      if(enc.toUpperCase()==='Q') return data.replace(/_/g,' ').replace(/=[0-9A-Fa-f]{2}/g,m=>String.fromCharCode(parseInt(m.slice(1),16)));
    } catch{} return data;
  });
}

function cleanEmailBody(raw) {
  if(!raw || !raw.trim()) return '';
  let text = raw;
  text = text.replace(/--[A-Za-z0-9_=+/\-]{10,}[\r\n]+/g, '');
  text = text.replace(/^(Content-Type|Content-Transfer-Encoding|Content-ID|Content-Disposition|MIME-Version):[^\n]*\n?/gim, '');
  text = text.replace(/^--[A-Za-z0-9_=+/\-]+\s*$/gm, '');
  const b64clean = text.replace(/[\r\n\s]/g,'');
  if(b64clean.length > 10 && /^[A-Za-z0-9+/]+=*$/.test(b64clean) && !b64clean.includes(' ')) {
    try { const decoded=Buffer.from(b64clean,'base64').toString('utf8'); if(decoded.length>0 && decoded.length<b64clean.length*0.85) text=decoded; } catch{}
  }
  text = text
    .replace(/=\r?\n/g,'')
    .replace(/=[0-9A-Fa-f]{2}/g, m => { try{return String.fromCharCode(parseInt(m.slice(1),16));}catch{return '';} });
  text = text
    .replace(/<[^>]{0,500}>/g,' ')
    .replace(/&nbsp;/gi,' ').replace(/&amp;/gi,'&').replace(/&lt;/gi,'<').replace(/&gt;/gi,'>').replace(/&quot;/gi,'"');
  const lines = text.split('\n');
  const clean = [];
  let hitQuote = false;
  for(const ln of lines) {
    const trimmed = ln.trim();
    if(/^On .+wrote:$/i.test(trimmed)) { hitQuote = true; break; }
    if(trimmed.startsWith('>')) { hitQuote = true; continue; }
    if(hitQuote && trimmed === '') continue;
    hitQuote = false;
    clean.push(ln);
  }
  text = clean.join('\n');
  // Fix UTF-8 mojibake encoding
  try { text = Buffer.from(text,'latin1').toString('utf8'); } catch(e){}
  // Fix remaining encoding artifacts
  text = text.replace(/â€™/g,"'").replace(/â€œ/g,'"').replace(/â€/g,'"').replace(/â€"/g,'—').replace(/â/g,"'");
  return text
    .replace(/\r/g,'')
    .replace(/[ \t]{2,}/g,' ')
    .replace(/\n{3,}/g,'\n\n')
    .trim()
    .substring(0, 3000);
}

function normaliseSubject(s) {
  return s.replace(/^(Re|Fwd|Fw|Re\[\d+\]):\s*/gi, '').trim().toLowerCase();
}

function processImapFetch(lines, gmailUser) {
  const msgs=[]; let cur=null;
  for(const line of lines) {
    if(/^\* \d+ FETCH /.test(line)) { if(cur)msgs.push(cur); cur={header:line,body:[]}; }
    else if(cur) cur.body.push(line);
  }
  if(cur) msgs.push(cur);

  let imported=0;
  for(const msg of msgs) {
    const allLines=[msg.header, ...msg.body];
    const full=allLines.join('\n');

    const uidM=full.match(/UID\s+(\d+)/); if(!uidM)continue;
    const imapUid=uidM[1]; if(db.uidSeen(imapUid))continue;

    let subject='(No Subject)';
    const envStart=full.indexOf('ENVELOPE (');
    if(envStart!==-1) {
      const after=full.substring(envStart+10);
      const q1s=after.indexOf('"'); const q1e=after.indexOf('"',q1s+1);
      if(q1s!==-1&&q1e!==-1){ const rest=after.substring(q1e+1).trimStart(); if(!rest.startsWith('NIL')){const sm=rest.match(/^"((?:[^"\\]|\\.)*)"/);if(sm)subject=decodeImapStr(sm[1]);} }
    }
    subject=subject.replace(/\r?\n/g,' ').trim().substring(0,200);

    let fromName='Customer',fromEmail='unknown@mail.com';
    const fromFull=full.match(/\(\("((?:[^"\\]|\\.)*)" NIL "([^"]+)" "([^"]+)"\)\)/);
    if(fromFull){fromName=decodeImapStr(fromFull[1])||fromFull[2];fromEmail=`${fromFull[2]}@${fromFull[3]}`.toLowerCase().trim();}
    else{const fromNil=full.match(/\(NIL NIL "([^"]+)" "([^"]+)"\)/);if(fromNil){fromEmail=`${fromNil[1]}@${fromNil[2]}`.toLowerCase().trim();fromName=fromEmail.split('@')[0];}}
    if(fromEmail===gmailUser.toLowerCase()){db.markUid(imapUid);continue;}

    let bodyText='';
    for(let i=0; i<allLines.length; i++){
      const ln=allLines[i];
      const inlineQ=ln.match(/BODY\[1\]\s+"((?:[^"\\]|\\.)*)"/i);
      if(inlineQ){ bodyText=inlineQ[1]; break; }
      if(/BODY\[1\]/i.test(ln)){
        let start=i+1;
        if(start<allLines.length && /^\{\d+\}$/.test(allLines[start].trim())) start++;
        for(let j=start; j<allLines.length; j++){
          const bl=allLines[j];
          if(bl===')') break;
          if(bl===')' || bl===') ') break;
          bodyText+=bl+'\n';
        }
        break;
      }
    }

    const body = cleanEmailBody(bodyText) || '[Email — '+subject+']';
    
    // Parse attachments from BODYSTRUCTURE
    const attachments=[];
    const bsMatch=full.match(/BODYSTRUCTURE\s*\((.+)\)/s);
    if(bsMatch){
      const bs=bsMatch[1];
      // Find filename patterns like ("attachment" ("filename" "somefile.pdf"))
      const fnRe=/"(?:filename|name)"\s+"([^"]+)"/gi;
      let fm;
      while((fm=fnRe.exec(bs))!==null){
        const fname=decodeImapStr(fm[1]);
        if(fname && !attachments.includes(fname)) attachments.push(fname);
      }
    }
    const attachNote = attachments.length ? '\n\n📎 Attachments: '+attachments.join(', ') : '';
    
    // Extract attachment binary data from MIME parts
    const attachmentData=[];
    if(bsMatch){
      const bs=bsMatch[1];
      // Find base64 encoded parts with filenames
      const partRe=/BODY\[(\d+(?:\.\d+)*)\]\s+\{(\d+)\}([\s\S]+?)(?=BODY\[|\)\s*$)/g;
      let pm;
      for(let ai=0;ai<attachments.length;ai++){
        const fname=attachments[ai];
        const ext=(fname.split('.').pop()||'').toLowerCase();
        const mimeMap={pdf:'application/pdf',jpg:'image/jpeg',jpeg:'image/jpeg',png:'image/png',gif:'image/gif',doc:'application/msword',docx:'application/vnd.openxmlformats-officedocument.wordprocessingml.document',xls:'application/vnd.ms-excel',xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',zip:'application/zip',txt:'text/plain',csv:'text/csv'};
        attachmentData.push({name:fname,mime:mimeMap[ext]||'application/octet-stream',data:''});
      }
    }
    console.log(`[Gmail] "${subject}" from ${fromEmail} — body: ${body.length} chars`);

    let cust=db.findBy('customers','email',fromEmail);
    if(!cust)cust=db.insert('customers',{id:genId(),name:fromName,email:fromEmail,phone:'',created:now()});

    const normSubj = normaliseSubject(subject);
    const allConvs = db.all('convs');

    let conv = allConvs.find(c=>
      c.channel==='email' && c.email===fromEmail &&
      normaliseSubject(c.subject||'')===normSubj &&
      c.status==='open'
    );

    if(!conv) {
      const prevConv = allConvs.find(c=>
        c.channel==='email' && c.email===fromEmail &&
        normaliseSubject(c.subject||'')===normSubj
      );

      if(prevConv) {
        const action = applyTicketLogic(prevConv);
        if(action === 'reopen') {
          db.update('convs', prevConv.id, {status:'open', updated:now()});
          db.addActivity(prevConv.id, {type:'reopened', text:'Ticket reopened — customer replied', agent:'System'});
          broadcast('conv_update', db.find('convs', prevConv.id));
          conv = db.find('convs', prevConv.id);
        }
      }
    }

    if(!conv) {
      const ticketId = db.nextId('email');
      conv=db.insert('convs',{
        id:genId(), ticketId, custId:cust.id, name:fromName, email:fromEmail,
        channel:'email', status:'open', agent:'', tags:[], subject,
        priority:'normal', platform:'email', created:now(), updated:now(),
        activity:[{type:'created', text:`Ticket ${ticketId} created`, time:now()}]
      });
      autoAssign(conv);
      broadcast('new_conv',conv);
      // Email auto-reply
      const _earCfg=getCfg();
      if(_earCfg.auto_reply_email_enabled&&_earCfg.auto_reply_email_msg&&fromEmail){
        setTimeout(()=>{
          sendGmail(fromEmail,'Re: '+subject,_earCfg.auto_reply_email_msg).catch(()=>{});
          const arm=db.insert('messages',{id:genId(),convId:conv.id,type:'agent',name:'Nexus (Auto)',content:'[Auto-reply sent]: '+_earCfg.auto_reply_email_msg,created:now()});
          broadcast('new_msg',{cid:conv.id,msg:arm});
        },2000);
      }
    }

    const msgObj=db.insert('messages',{id:genId(),convId:conv.id,type:'customer',name:fromName,content:body+attachNote,attachments,attachmentData,created:now()});
    db.markUid(imapUid);
    db.update('convs', conv.id, {updated:now()});
    broadcast('new_msg',{cid:conv.id,msg:msgObj});
    imported++;
  }
  console.log(`[Gmail] Done — ${imported} new emails imported`);
}

let pollTimer=null;
function pollGmail() {
  const cfg=getCfg(); const gmailUser=(cfg.gmail_user||'').trim(),gmailPass=(cfg.gmail_pass||'').trim();
  if(!gmailUser||!gmailPass)return Promise.resolve();
  return new Promise(resolve=>{
    console.log(`[Gmail] Connecting as ${gmailUser}...`);
    let buf='',done=false,state='greeting',fetchLines=[];
    const T={login:'G1',select:'G2',search:'G3',fetch:'G4',logout:'G5'};
    const finish=()=>{if(!done){done=true;try{sock.destroy();}catch(_){};resolve();}};
    setTimeout(finish,60000);
    const sock=tls.connect({host:'imap.gmail.com',port:993,rejectUnauthorized:false},()=>sock.setEncoding('utf8'));
    const send=cmd=>{if(!sock.destroyed)sock.write(cmd+'\r\n');};
    sock.on('data',chunk=>{
      buf+=chunk; const lines=buf.split('\r\n'); buf=lines.pop();
      for(const line of lines){
        if(state==='greeting'&&(line.startsWith('* OK')||line.includes('IMAP'))){state='login';send(`${T.login} LOGIN "${gmailUser}" "${gmailPass}"`);}
        else if(state==='login'){
          if(line.startsWith(`${T.login} OK`)){state='select';send(`${T.select} SELECT INBOX`);}
          else if(line.includes('NO')||line.includes('AUTHENTICATIONFAILED')||line.includes('Invalid')){console.error('[Gmail] Login failed');finish();}
        }
        else if(state==='select'){
          if(line.startsWith(`${T.select} OK`)){state='search';send(`${T.search} UID SEARCH ALL`);}
          else if(line.startsWith(`${T.select} NO`)||line.startsWith(`${T.select} BAD`)){finish();}
        }
        else if(state==='search'){
          if(line.startsWith('* SEARCH')){
            const uids=line.replace('* SEARCH','').trim().split(/\s+/).filter(Boolean);
            const toFetch=uids.slice(-30);
            if(!toFetch.length){send(`${T.logout} LOGOUT`);finish();return;}
            state='fetch'; send(`${T.fetch} UID FETCH ${toFetch.join(',')} (UID ENVELOPE BODYSTRUCTURE BODY.PEEK[])`);
          }
          if(line.startsWith(`${T.search} OK`)&&state==='search'&&!fetchLines.length){send(`${T.logout} LOGOUT`);finish();}
        }
        else if(state==='fetch'){
          if(line.startsWith(`${T.fetch} OK`)){processImapFetch(fetchLines,gmailUser);send(`${T.logout} LOGOUT`);finish();}
          else if(line.startsWith(`${T.fetch} NO`)||line.startsWith(`${T.fetch} BAD`)){console.error('[Gmail] FETCH error:',line);finish();}
          else fetchLines.push(line);
        }
        if(line.includes('* BYE'))finish();
      }
    });
    sock.on('error',e=>{console.error('[Gmail] Socket error:',e.message);finish();});
    sock.on('close',()=>finish());
  });
}



function gmailApiGet(path,accessToken){
  return new Promise((resolve,reject)=>{
    const opts={hostname:'gmail.googleapis.com',path,headers:{'Authorization':'Bearer '+accessToken}};
    const req=https.request(opts,r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve({_raw:d});}});});
    req.on('error',reject);req.setTimeout(20000,()=>{req.destroy();reject(new Error('timeout'));});req.end();
  });
}

// Send email via Gmail REST API using OAuth token (no App Password needed)
async function sendViaGmailOAuth(to, subject, htmlBody) {
  const token = await getGmailToken();
  if (!token) throw new Error('Gmail OAuth not configured - connect Gmail in Workspace settings');
  const cfg = getCfg();
  const from = cfg.gmail_user || 'me';
  const raw = [
    `From: ${from}`,
    `To: ${to}`,
    `Subject: =?UTF-8?B?${Buffer.from(subject).toString('base64')}?=`,
    'MIME-Version: 1.0',
    'Content-Type: text/html; charset=utf-8',
    '',
    htmlBody
  ].join('\r\n');
  const encoded = Buffer.from(raw).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({raw: encoded});
    const opts = {
      hostname: 'gmail.googleapis.com',
      path: '/gmail/v1/users/me/messages/send',
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = https.request(opts, r => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        try {
          const parsed = JSON.parse(d);
          if (parsed.id) resolve(parsed);
          else reject(new Error(parsed.error?.message || 'Gmail API error: ' + d.substring(0,200)));
        } catch(e) { reject(new Error('Gmail API parse error')); }
      });
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Gmail API timeout')); });
    req.write(body);
    req.end();
  });
}

async function refreshGmailToken(){
  const cfg=getCfg();if(!cfg.gmail_refresh_token)return null;
  return new Promise((resolve)=>{
    const body=new URLSearchParams({client_id:GMAIL_CLIENT_ID,client_secret:GMAIL_CLIENT_SECRET,refresh_token:cfg.gmail_refresh_token,grant_type:'refresh_token'}).toString();
    const opts={hostname:'oauth2.googleapis.com',path:'/token',method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded','Content-Length':Buffer.byteLength(body)}};
    const req=https.request(opts,r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{
      try{
        const data=JSON.parse(d);
        if(data.access_token){const c=getCfg();c.gmail_access_token=data.access_token;c.gmail_token_expiry=Date.now()+(data.expires_in||3600)*1000;setCfg(c);resolve(data.access_token);}
        else resolve(null);
      }catch{resolve(null);}
    });});
    req.on('error',()=>resolve(null));req.write(body);req.end();
  });
}

async function getGmailToken(){
  const cfg=getCfg();
  if(cfg.gmail_access_token&&cfg.gmail_token_expiry&&Date.now()<cfg.gmail_token_expiry-60000)return cfg.gmail_access_token;
  return await refreshGmailToken();
}

function decodeBase64Url(str){return Buffer.from(str.replace(/-/g,'+').replace(/_/g,'/'),'base64');}

function findParts(payload,mimeType){
  const results=[];
  function walk(p){if(!p)return;if(p.mimeType===mimeType&&p.body&&p.body.data)results.push(p);if(p.parts)p.parts.forEach(walk);}
  walk(payload);return results;
}

function findAttachmentParts(payload){
  const results=[];
  function walk(p){
    if(!p)return;
    const cd=(p.headers||[]).find(h=>h.name.toLowerCase()==='content-disposition');
    if((cd&&cd.value.toLowerCase().includes('attachment'))||(p.filename&&p.filename.length>0)){
      if(p.body&&(p.body.attachmentId||p.body.data))results.push(p);
    }
    if(p.parts)p.parts.forEach(walk);
  }
  walk(payload);return results;
}

function stripHtmlQuotes(html){
  // Remove Gmail quoted reply block (div.gmail_quote, blockquote, and "On...wrote:" lines)
  html = html.replace(/<div[^>]*class="[^"]*gmail_quote[^"]*"[^>]*>[\s\S]*?<\/div>/gi,'');
  html = html.replace(/<blockquote[^>]*>[\s\S]*?<\/blockquote>/gi,'');
  // Remove "On [date]...<email> wrote:" paragraph
  html = html.replace(/<[^>]+>On .{0,200}wrote:<\/[^>]+>/gi,'');
  html = html.replace(/On .{10,200}wrote:/gi,'');
  // Clean up trailing empty divs/brs
  html = html.replace(/(<br\s*\/?>\s*){2,}/gi,'<br>');
  html = html.replace(/<div>\s*<\/div>/gi,'');
  return html.trim();
}
function gmailMsgToText(payload){
  const htmlParts=findParts(payload,'text/html');
  if(htmlParts.length){
    const raw=Buffer.from(htmlParts[0].body.data.replace(/-/g,'+').replace(/_/g,'/'),'base64').toString('utf8');
    return{html:stripHtmlQuotes(raw),isHtml:true};
  }
  const textParts=findParts(payload,'text/plain');
  if(textParts.length){
    const raw=Buffer.from(textParts[0].body.data.replace(/-/g,'+').replace(/_/g,'/'),'base64').toString('utf8');
    return{html:raw,isHtml:false};
  }
  if(payload.body&&payload.body.data)return{html:Buffer.from(payload.body.data.replace(/-/g,'+').replace(/_/g,'/'),'base64').toString('utf8'),isHtml:false};
  return{html:'',isHtml:false};
}

async function pollGmailAPI(){
  const cfg=getCfg();if(!cfg.gmail_refresh_token)return;
  const token=await getGmailToken();if(!token){console.log('[GmailAPI] No token');return;}
  console.log('[GmailAPI] Polling...');
  try{
    const list=await gmailApiGet('/gmail/v1/users/me/messages?maxResults=30&q=in:inbox',token);
    if(!list.messages||!list.messages.length){console.log('[GmailAPI] No new messages');return;}
    let imported=0;
    for(const item of list.messages){
      const seenKey='gmail_api_'+item.id;
      if(db.uidSeen(seenKey))continue;
      const msg=await gmailApiGet('/gmail/v1/users/me/messages/'+item.id+'?format=full',token);
      if(!msg||!msg.payload)continue;
      const headers=msg.payload.headers||[];
      const getH=name=>(headers.find(h=>h.name.toLowerCase()===name)||{}).value||'';
      const subject=getH('subject')||'(No Subject)';
      const fromRaw=getH('from');
      const fromMatch=fromRaw.match(/^"?([^"<]+)"?s*<?([^>]+)>?$/);
      const fromName=fromMatch?fromMatch[1].trim():fromRaw.split('@')[0];
      const fromEmail=fromMatch?fromMatch[2].trim().toLowerCase():fromRaw.toLowerCase();
      if(fromEmail===(cfg.gmail_user||'').toLowerCase()){db.markUid(seenKey);continue;}
      const threadId=msg.threadId;
      const{html:bodyRaw,isHtml}=gmailMsgToText(msg.payload);
      const attParts=findAttachmentParts(msg.payload);
      const attachments=attParts.map(p=>p.filename).filter(Boolean);
      const attIds=attParts.map(p=>(p.body&&p.body.attachmentId)||'');
      let cust=db.findBy('customers','email',fromEmail);
      if(!cust)cust=db.insert('customers',{id:genId(),name:fromName,email:fromEmail,phone:'',created:now()});
      const normSubj=normaliseSubject(subject);
      const allConvs=db.all('convs');
      let conv=allConvs.find(c=>c.channel==='email'&&(c.gmailThreadId===threadId||(c.email===fromEmail&&normaliseSubject(c.subject||'')===normSubj))&&c.status==='open');
      if(!conv){
        const prevConv=allConvs.find(c=>c.channel==='email'&&(c.gmailThreadId===threadId||(c.email===fromEmail&&normaliseSubject(c.subject||'')===normSubj)));
        if(prevConv){const action=applyTicketLogic(prevConv);if(action==='reopen'){db.update('convs',prevConv.id,{status:'open',updated:now()});db.addActivity(prevConv.id,{type:'reopened',text:'Ticket reopened',agent:'System'});broadcast('conv_update',db.find('convs',prevConv.id));conv=db.find('convs',prevConv.id);}}
      }
      if(!conv){
        const ticketId=db.nextId('email');
        conv=db.insert('convs',{id:genId(),ticketId,custId:cust.id,name:fromName,email:fromEmail,channel:'email',status:'open',agent:'',tags:[],subject,priority:'normal',platform:'email',gmailThreadId:threadId,created:now(),updated:now(),activity:[{type:'created',text:'Ticket '+ticketId+' created',time:now()}]});
        autoAssign(conv);broadcast('new_conv',conv);
      }
      const msgObj=db.insert('messages',{id:genId(),convId:conv.id,type:'customer',name:fromName,content:bodyRaw||'[No content]',isHtml,attachments,attIds,gmailMsgId:item.id,gmailThreadId:threadId,created:now()});
      db.markUid(seenKey);db.update('convs',conv.id,{updated:now()});broadcast('new_msg',{cid:conv.id,msg:msgObj});imported++;
    }
    console.log('[GmailAPI] Done —',imported,'new emails');
  }catch(e){console.log('[GmailAPI] Error:',e.message);}
}

function startGmailAPIPolling(){
  if(pollTimer){clearInterval(pollTimer);pollTimer=null;}
  const cfg=getCfg();
  if(cfg.gmail_api_enabled&&cfg.gmail_refresh_token){
    console.log('[GmailAPI] Starting polling every 30s');
    pollGmailAPI();pollTimer=setInterval(pollGmailAPI,30000);
  } else {
    startPolling();
  }
}

function startPolling(){
  if(pollTimer){clearInterval(pollTimer);pollTimer=null;}
  const cfg=getCfg();
  if(cfg.gmail_user&&cfg.gmail_pass){
    console.log(`[Gmail] Polling every 30s for ${cfg.gmail_user}`);
    pollGmail(); pollTimer=setInterval(pollGmail,30000);
  }
}

let socialTimer=null;
function createSocialTicket(platform, externalId, authorName, authorId, content, sourceUrl, rating) {
  const seenKey=`social_${platform}_${externalId}`;
  if(db.uidSeen(seenKey))return;
  const fromEmail=`${authorId||authorName.toLowerCase().replace(/\s+/g,'.')}@${platform}.social`;
  let cust=db.findBy('customers','email',fromEmail);
  if(!cust)cust=db.insert('customers',{id:genId(),name:authorName,email:fromEmail,phone:'',created:now()});
  const subject=rating?`${platform.toUpperCase()} Review — ${rating}⭐`:content.substring(0,80);
  const existing=db.all('convs').find(c=>c.platform===platform&&c.externalId===externalId);
  if(existing){db.markUid(seenKey);return;}
  const ticketId=db.nextId('social');
  const conv=db.insert('convs',{
    id:genId(), ticketId, custId:cust.id, name:authorName, email:fromEmail,
    channel:'social', platform, externalId, status:'open', agent:'', tags:[],
    subject, priority:'normal', sourceUrl:sourceUrl||'',
    created:now(), updated:now(),
    activity:[{type:'created', text:`${ticketId} created from ${platform}`, time:now()}]
  });
  autoAssign(conv);
  broadcast('new_conv',conv);
  const msgContent=rating?`⭐ ${rating}/5\n\n${content}`:content;
  const msg=db.insert('messages',{id:genId(),convId:conv.id,type:'customer',name:authorName,content:msgContent,created:now()});
  db.markUid(seenKey);
  broadcast('new_msg',{cid:conv.id,msg});
  console.log(`[Social/${platform}] New ticket ${ticketId} from ${authorName}`);
}

async function pollInstagram(account) {
  if(!account.access_token) return;
  try {
    const media=await httpGet({hostname:'graph.instagram.com',path:`/me/media?fields=id,caption,timestamp,media_type,permalink&limit=10&access_token=${account.access_token}`,headers:{'User-Agent':'Nexus/1.0'}});
    for(const item of (media.data||[])){
      if(!item.caption)continue;
      const comments=await httpGet({hostname:'graph.instagram.com',path:`/${item.id}/comments?fields=id,text,username,timestamp&access_token=${account.access_token}`,headers:{'User-Agent':'Nexus/1.0'}});
      for(const c of (comments.data||[])) createSocialTicket('instagram',`${item.id}_${c.id}`,c.username||'instagrammer',c.username,`💬 Comment on post:\n"${(item.caption||'').substring(0,100)}"\n\n${c.text}`,item.permalink,null);
    }
    const convs=await httpGet({hostname:'graph.instagram.com',path:`/me/conversations?fields=id,messages{id,from,message,created_time}&access_token=${account.access_token}`,headers:{'User-Agent':'Nexus/1.0'}});
    for(const conv of (convs.data||[])) for(const msg of (conv.messages?.data||[])){if(!msg.message)continue;const from=msg.from||{};createSocialTicket('instagram',msg.id,from.name||'instagrammer',from.id||from.username,`📩 DM: ${msg.message}`,null,null);}
  } catch(e){console.error('[Social/Instagram] Error:',e.message);}
}
async function pollFacebook(account) {
  if(!account.page_access_token) return;
  try {
    const posts=await httpGet({hostname:'graph.facebook.com',path:`/me/posts?fields=id,message,created_time,comments{id,message,from,created_time}&access_token=${account.page_access_token}&limit=10`,headers:{'User-Agent':'Nexus/1.0'}});
    for(const post of (posts.data||[])) for(const c of (post.comments?.data||[])){const from=c.from||{};createSocialTicket('facebook',c.id,from.name||'facebooker',from.id,`💬 Comment on post:\n"${(post.message||'').substring(0,100)}"\n\n${c.message}`,null,null);}
    const msgs=await httpGet({hostname:'graph.facebook.com',path:`/me/conversations?fields=id,messages{id,message,from,created_time}&access_token=${account.page_access_token}&limit=10`,headers:{'User-Agent':'Nexus/1.0'}});
    for(const conv of (msgs.data||[])) for(const msg of (conv.messages?.data||[])){if(!msg.message)continue;const from=msg.from||{};createSocialTicket('facebook',msg.id,from.name||'facebooker',from.id,`📩 Message: ${msg.message}`,null,null);}
  } catch(e){console.error('[Social/Facebook] Error:',e.message);}
}
async function pollTwitter(account) {
  if(!account.bearer_token||!account.user_id) return;
  try {
    const mentions=await httpGet({hostname:'api.twitter.com',path:`/2/users/${account.user_id}/mentions?tweet.fields=created_at,text,author_id&expansions=author_id&user.fields=name,username&max_results=10`,headers:{'Authorization':`Bearer ${account.bearer_token}`,'User-Agent':'Nexus/1.0'}});
    const userMap={};
    for(const u of (mentions.includes?.users||[])) userMap[u.id]={name:u.name,username:u.username};
    for(const tweet of (mentions.data||[])){const author=userMap[tweet.author_id]||{name:'tweeter',username:'tweeter'};createSocialTicket('twitter',tweet.id,`@${author.username}`,author.username,`🐦 Mention: ${tweet.text}`,`https://twitter.com/i/web/status/${tweet.id}`,null);}
  } catch(e){console.error('[Social/Twitter] Error:',e.message);}
}
async function pollTrustpilot(account) {
  if(!account.api_key||!account.business_unit_id) return;
  try {
    const reviews=await httpGet({hostname:'api.trustpilot.com',path:`/v1/business-units/${account.business_unit_id}/reviews?perPage=10&orderBy=createdat.desc`,headers:{'apikey':account.api_key,'User-Agent':'Nexus/1.0'}});
    for(const r of (reviews.reviews||[])){const consumer=r.consumer||{};createSocialTicket('trustpilot',r.id,consumer.displayName||'reviewer',consumer.id,r.text||r.title||'New review',r.reviewUrl,r.stars);}
  } catch(e){console.error('[Social/Trustpilot] Error:',e.message);}
}
async function pollAppStore(account) {
  if(!account.app_id) return;
  try {
    let entries=[];
    for(const path of [
      `/rss/customerreviews/page=1/id=${account.app_id}/sortBy=mostRecent/json`,
      `/rss/customerreviews/id=${account.app_id}/sortBy=mostRecent/json`
    ]){
      const data=await httpGet({hostname:'itunes.apple.com',path,headers:{'User-Agent':'Mozilla/5.0 (compatible; Nexus/1.0)','Accept':'application/json'}});
      entries=(data.feed?.entry||[]);
      if(entries.length)break;
    }
    if(!Array.isArray(entries))entries=[entries];
    for(const entry of entries.slice(0,20)){
      const id=entry.id?.label||entry['id']?.label||'';
      if(!id)continue;
      if(String(id)===String(account.app_id))continue;
      const rating=entry['im:rating']?.label||null;
      if(!rating&&!entry.author)continue;
      const title=entry.title?.label||'Review';
      const content=entry.content?.label||entry.summary?.label||'';
      const author=entry.author?.name?.label||'AppStore User';
      createSocialTicket('appstore',id,author,author,`📱 ${title}\n\n${content}`,`https://apps.apple.com/app/id${account.app_id}`,rating);
    }
  } catch(e){console.error('[Social/AppStore] Error:',e.message);}
}
async function pollPlayStore(account) {
  if(!account.app_id||!account.access_token) return;
  try {
    const data=await httpGet({hostname:'androidpublisher.googleapis.com',path:`/androidpublisher/v3/applications/${account.app_id}/reviews?maxResults=10&token=${account.access_token}`,headers:{'Authorization':`Bearer ${account.access_token}`,'User-Agent':'Nexus/1.0'}});
    for(const r of (data.reviews||[])){const comment=r.comments?.[0]?.userComment||{};createSocialTicket('playstore',r.reviewId,r.authorName||'Play User',r.reviewId,`🤖 ${comment.text||'New review'}`,`https://play.google.com/store/apps/details?id=${account.app_id}`,comment.starRating||null);}
  } catch(e){console.error('[Social/PlayStore] Error:',e.message);}
}
async function pollAllSocial() {
  const accounts=db.all('socialAccounts');
  for(const acc of accounts){
    if(!acc.active)continue;
    if(acc.platform==='instagram') await pollInstagram(acc);
    if(acc.platform==='facebook')  await pollFacebook(acc);
    if(acc.platform==='twitter')   await pollTwitter(acc);
    if(acc.platform==='trustpilot')await pollTrustpilot(acc);
    if(acc.platform==='appstore')  await pollAppStore(acc);
    if(acc.platform==='playstore') await pollPlayStore(acc);
  }
}
function startSocialPolling(){
  if(socialTimer){clearInterval(socialTimer);socialTimer=null;}
  const accounts=db.all('socialAccounts').filter(a=>a.active);
  if(accounts.length){
    pollAllSocial();
    socialTimer=setInterval(pollAllSocial,5*60*1000);
  }
}

function callOpenAI(messages, systemPrompt, maxTokens) {
  const cfg=getCfg(); if(!cfg.ai_key) return Promise.resolve(null);
  maxTokens=maxTokens||300;
  return new Promise((resolve)=>{
    const body=JSON.stringify({model:'gpt-4o-mini',max_tokens:maxTokens,messages:[{role:'system',content:systemPrompt},...messages]});
    const opts={hostname:'api.openai.com',path:'/v1/chat/completions',method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+cfg.ai_key,'Content-Length':Buffer.byteLength(body)}};
    const req=https.request(opts,r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve(null);}});});
    req.on('error',()=>resolve(null));req.setTimeout(20000,()=>{req.destroy();resolve(null);});req.write(body);req.end();
  });
}
function aiReply(content,convId){
  const cfg=getCfg(); if(!cfg.ai_key||!cfg.ai_enabled)return Promise.resolve(null);
  return new Promise(async resolve=>{
    const hist=db.all('messages').filter(m=>m.convId===convId).sort((a,b)=>new Date(a.created)-new Date(b.created)).slice(-6);
    const msgs=[...hist.map(m=>({role:m.type==='customer'?'user':'assistant',content:m.content})),{role:'user',content}];
    const kb=cfg.kb_content?'\n\nKNOWLEDGE BASE:\n'+cfg.kb_content.substring(0,8000):'';
    const sys='You are a concise, warm support agent. Reply in 1-2 sentences.'+kb;
    const result=await callOpenAI(msgs,sys,250);
    resolve(result?.choices?.[0]?.message?.content||null);
  });
}
function sendGmail(to,subject,body){
  const cfg=getCfg(); const user=(cfg.gmail_user||'').trim(),pass=(cfg.gmail_pass||'').trim();
  if(!user||!pass)return Promise.reject(new Error('Gmail not configured'));
  return new Promise((resolve,reject)=>{
    const b64=s=>Buffer.from(s).toString('base64');
    const isHtml = body.trim().startsWith('<');
    const contentType = isHtml ? 'text/html; charset=utf-8' : 'text/plain; charset=utf-8';
    const mail=[`From: ${user}`,`To: ${to}`,`Subject: =?UTF-8?B?${Buffer.from(subject).toString('base64')}?=`,`MIME-Version: 1.0`,`Content-Type: ${contentType}`,'',body].join('\r\n');
    let buf='',st='greeting';
    const s=tls.connect({host:'smtp.gmail.com',port:465,rejectUnauthorized:false},()=>s.setEncoding('utf8'));
    const send=cmd=>s.write(cmd+'\r\n');
    const done=(err)=>{try{s.destroy();}catch(_){} err?reject(err):resolve();};
    s.on('data',chunk=>{
      buf+=chunk; const lines=buf.split('\r\n'); buf=lines.pop();
      for(const l of lines){
        if     (st==='greeting'&&l.startsWith('220')){st='ehlo';send('EHLO nexus');}
        else if(st==='ehlo'   &&l.startsWith('250 ')){st='auth';send('AUTH LOGIN');}
        else if(st==='auth'   &&l.startsWith('334')) {st='user';send(b64(user));}
        else if(st==='user'   &&l.startsWith('334')) {st='pass';send(b64(pass));}
        else if(st==='pass'   &&l.startsWith('235')) {st='from';send(`MAIL FROM:<${user}>`);}
        else if(st==='from'   &&l.startsWith('250')) {st='rcpt';send(`RCPT TO:<${to}>`);}
        else if(st==='rcpt'   &&l.startsWith('250')) {st='data';send('DATA');}
        else if(st==='data'   &&l.startsWith('354')) {st='body';send(mail+'\r\n.');}
        else if(st==='body'   &&l.startsWith('250')) {st='quit';send('QUIT');done();}
        else if(l.match(/^[45]\d\d/)) done(new Error('SMTP: '+l));
      }
    });
    s.on('error',e=>done(e)); setTimeout(()=>done(new Error('SMTP timeout')),20000);
  });
}

const server=http.createServer(async(req,res)=>{
  // CORS for widget embeds (cross-origin from any website/app)
  const origin=req.headers.origin||'*';
  res.setHeader('Access-Control-Allow-Origin',origin);
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type,Authorization');
  res.setHeader('Access-Control-Allow-Credentials','true');
  if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}
  const parsed=url.parse(req.url,true);
  const pathname=parsed.pathname, method=req.method;
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  if(method==='OPTIONS'){res.writeHead(204);res.end();return;}

  if(!pathname.startsWith('/api')&&pathname!=='/sse'){
    if(pathname==='/'){const sess=getSession(req);return serveFile(res,path.join(PUB,(sess&&sess.uid)?'app.html':'login.html'));}
    // Dynamic routes that must be handled before static files
    if(pathname==='/accept-invite'&&method==='GET'){
      // Handled below - skip static serving for this route
    } else {
      const fp=path.join(PUB,pathname);
      if(fs.existsSync(fp)&&fs.statSync(fp).isFile())return serveFile(res,fp);
      // Don't 404 here - fall through to API routes
    }
  }

  if(pathname==='/sse'){
    const sess=getSession(req);if(!sess||!sess.uid){res.writeHead(401);res.end();return;}
    res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','X-Accel-Buffering':'no'});
    res.write(':ok\n\n'); sseClients.add(res); req.on('close',()=>sseClients.delete(res)); return;
  }

  const body=(method==='POST'||method==='PATCH')?await readBody(req):{};


  // ── GMAIL OAUTH ROUTES ────────────────────────────────────────────────────
  if(pathname==='/api/gmail/oauth/start'&&method==='GET'){
    const authUrl='https://accounts.google.com/o/oauth2/v2/auth?'+new URLSearchParams({
      client_id:GMAIL_CLIENT_ID,
      redirect_uri:GMAIL_REDIRECT,
      response_type:'code',
      scope:GMAIL_SCOPES,
      access_type:'offline',
      prompt:'consent'
    }).toString();
    res.writeHead(302,{Location:authUrl});res.end();return;
  }
  if(pathname==='/api/gmail/oauth/callback'&&method==='GET'){
    const code=parsed.query.code;
    if(!code){res.writeHead(400);res.end('No code');return;}
    // Exchange code for tokens
    const body=new URLSearchParams({
      code,client_id:GMAIL_CLIENT_ID,client_secret:GMAIL_CLIENT_SECRET,
      redirect_uri:GMAIL_REDIRECT,grant_type:'authorization_code'
    }).toString();
    const tokenData=await new Promise((resolve,reject)=>{
      const opts={hostname:'oauth2.googleapis.com',path:'/token',method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded','Content-Length':Buffer.byteLength(body)}};
      const r2=https.request(opts,r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve({});}});});
      r2.on('error',reject);r2.write(body);r2.end();
    });
    if(tokenData.access_token){
      const cfg=getCfg();
      cfg.gmail_access_token=tokenData.access_token;
      cfg.gmail_refresh_token=tokenData.refresh_token||cfg.gmail_refresh_token;
      cfg.gmail_token_expiry=Date.now()+(tokenData.expires_in||3600)*1000;
      cfg.gmail_api_enabled=true;
      // Get user email
      try{
        const profile=await gmailApiGet('/gmail/v1/users/me/profile',tokenData.access_token);
        if(profile.emailAddress){cfg.gmail_user=profile.emailAddress;console.log('[GmailAPI] Connected as',profile.emailAddress);}
      }catch(e){}
      setCfg(cfg);
      // Start polling with API
      startPolling();
      res.writeHead(302,{Location:'/?gmail=connected'});res.end();
    } else {
      console.error('[GmailAPI] Token error:',JSON.stringify(tokenData));
      res.writeHead(302,{Location:'/?gmail=error'});res.end();
    }
    return;
  }
  if(pathname==='/api/gmail/oauth/disconnect'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();
    delete cfg.gmail_access_token;delete cfg.gmail_refresh_token;
    delete cfg.gmail_token_expiry;delete cfg.gmail_api_enabled;
    setCfg(cfg);
    if(pollTimer){clearInterval(pollTimer);pollTimer=null;}
    return json(res,{ok:true});
  }
  // Serve attachment from Gmail API
  const gmailAttMatch=pathname.match(/^\/api\/gmail\/attachment\/([^/]+)\/([^/]+)$/);
  if(gmailAttMatch&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const msgGmailId=gmailAttMatch[1];const attId=gmailAttMatch[2];
    const token=await getGmailToken();
    if(!token){return json(res,{error:'Not authenticated'},401);}
    try{
      const attData=await gmailApiGet('/gmail/v1/users/me/messages/'+msgGmailId+'/attachments/'+attId,token);
      if(!attData.data){return json(res,{error:'No data'},404);}
      const buf=decodeBase64Url(attData.data);
      res.writeHead(200,{'Content-Type':'application/octet-stream','Content-Disposition':'attachment','Content-Length':buf.length});
      res.end(buf);
    }catch(e){return json(res,{error:e.message},500);}
    return;
  }

  if(pathname==='/api/groups'&&method==='GET'){const sess=requireAuth(req,res);if(!sess)return;return json(res,db.all('groups'));}
  if(pathname==='/api/groups'&&method==='POST'){const sess=requireAuth(req,res);if(!sess)return;const gu=db.find('users',sess.uid);if(!gu||gu.role!=='admin')return json(res,{error:'Admin only'},403);const{name,description,channels,agents,code,color}=body;if(!name)return json(res,{error:'Name required'},400);const g=db.insert('groups',{id:genId(),name,code:code||name.replace(/[^A-Za-z0-9]/g,'').substring(0,10).toUpperCase(),description:description||'',color:color||'#00e5b4',channels:channels||[],agents:agents||[],created:now()});return json(res,g);}
  const grpMatch=pathname.match(/^\/api\/groups\/([^/]+)$/);
  if(grpMatch){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='GET'){return json(res,db.find('groups',grpMatch[1]));}
    if(method==='PATCH'){const sess2=requireAuth(req,res);if(!sess2)return;const gu2=db.find('users',sess2.uid);if(!gu2||gu2.role!=='admin')return json(res,{error:'Admin only'},403);const g=db.update('groups',grpMatch[1],{...body,updated:now()});return json(res,g||{});}
    if(method==='DELETE'){const gu3=db.find('users',sess.uid);if(!gu3||gu3.role!=='admin')return json(res,{error:'Admin only'},403);db.remove('groups',grpMatch[1]);return json(res,{ok:true});}
  }

  if(pathname==='/api/widget/start'&&method==='POST'){
    const{name,email}=body;
    if(!name)return json(res,{error:'Name required'},400);
    let cust=email?db.findBy('customers','email',email):null;
    if(!cust)cust=db.insert('customers',{id:genId(),name,email:email||'',phone:'',created:now()});
    // Chat threading config
    const chatCfg=getCfg();
    const chatThreadOpen=chatCfg.chat_thread_open||'continue';
    const chatThreadResolved=chatCfg.chat_thread_resolved||'new';
    const chatTimeoutMins=parseInt(chatCfg.chat_thread_timeout||1440);
    if(email){
      const existingConvs=(db.get().convs||[]).filter(c=>c.email===email&&c.channel==='chat');
      // Check for open ticket
      const openConv=existingConvs.find(c=>c.status==='open'||c.status==='waiting');
      if(openConv&&chatThreadOpen==='continue'){
        return json(res,{convId:openConv.id,ticketId:openConv.ticketId,continued:true});
      }
      // Check for recent resolved ticket
      if(chatThreadResolved==='reopen'||chatThreadResolved==='reopen_if_recent'){
        const resolvedConv=existingConvs.find(c=>c.status==='resolved');
        if(resolvedConv){
          const minsAgo=(Date.now()-new Date(resolvedConv.updated||resolvedConv.created).getTime())/60000;
          const withinWindow=chatTimeoutMins===0||minsAgo<=chatTimeoutMins;
          if(chatThreadResolved==='reopen'||withinWindow){
            db.update('convs',resolvedConv.id,{status:'open',updated:now()});
            db.addActivity(resolvedConv.id,{type:'reopened',text:'Chat reopened by customer',agent:'System'});
            broadcast('conv_update',db.find('convs',resolvedConv.id));
            return json(res,{convId:resolvedConv.id,ticketId:resolvedConv.ticketId,continued:true});
          }
        }
      }
    }
    const ticketId=db.nextId('chat');
    const conv=db.insert('convs',{
      id:genId(),ticketId,custId:cust.id,name,email:email||'',
      channel:'chat',status:'open',agent:'',tags:[],subject:'Live Chat',
      priority:'normal',platform:'chat',created:now(),updated:now(),
      activity:[{type:'created',text:`${ticketId} — customer started chat`,time:now()}]
    });
    autoAssign(conv);broadcast('new_conv',conv);
    // Auto-reply if enabled
    const _arCfg=getCfg();
    if(_arCfg.auto_reply_chat_enabled&&_arCfg.auto_reply_chat_msg){
      setTimeout(()=>{const m=db.insert('messages',{id:genId(),convId:conv.id,type:'bot',name:'Nexus',content:_arCfg.auto_reply_chat_msg,created:now()});db.update('convs',conv.id,{updated:now()});broadcast('new_msg',{cid:conv.id,msg:m});},1500);
    }
    return json(res,{convId:conv.id,ticketId:conv.ticketId});
  }
  // ── WIDGET: GET MESSAGES ────────────────────────────────────────────────
  if(pathname.startsWith('/api/widget/msgs/')&&method==='GET'){
    const convId=pathname.split('/').pop();
    const conv=db.find('convs',convId);
    if(!conv)return json(res,{error:'Not found'},404);
    const msgs=(db.get().messages||[]).filter(m=>m.convId===convId);
    return json(res,{messages:msgs,conv:{id:conv.id,ticketId:conv.ticketId,status:conv.status,assignee:conv.assignee}});
  }

  // ── WIDGET: TYPING INDICATOR ─────────────────────────────────────────────
  if(pathname==='/api/widget/typing'&&method==='POST'){
    const{convId,name,isTyping}=body;
    if(convId)broadcast('typing',{convId,name:name||'Customer',isTyping:!!isTyping});
    return json(res,{ok:true});
  }

  // ── WIDGET: RESOLVE ──────────────────────────────────────────────────────
  if(pathname==='/api/widget/resolve'&&method==='POST'){
    const{convId}=body;
    if(convId){db.update('convs',convId,{status:'resolved',updated:now()});broadcast('conv_update',db.find('convs',convId));}
    return json(res,{ok:true});
  }

  if(pathname==='/api/widget/send'&&method==='POST'){
    const{convId,content,name}=body;
    if(!convId||!content)return json(res,{error:'convId and content required'},400);
    const conv=db.find('convs',convId);if(!conv)return json(res,{error:'Not found'},404);
    const msg=db.insert('messages',{id:genId(),convId,type:'customer',name:name||conv.name||'Customer',content,created:now()});
    db.update('convs',convId,{updated:now(),status:conv.status==='resolved'?'open':conv.status});
    broadcast('new_msg',{cid:convId,msg});
    const rules=db.all('botRules').filter(r=>r.active);const low=content.toLowerCase();let hit=false;
    for(const r of rules){if(low.includes(r.keyword.toLowerCase())){hit=true;setTimeout(()=>{const m=db.insert('messages',{id:genId(),convId,type:'bot',name:'Bot',content:r.response,created:now()});db.update('convs',convId,{updated:now()});broadcast('new_msg',{cid:convId,msg:m});},800);break;}}
    if(!hit)aiReply(content,convId).then(ai=>{if(ai){const m=db.insert('messages',{id:genId(),convId,type:'bot',name:'AI',content:ai,created:now()});db.update('convs',convId,{updated:now()});broadcast('new_msg',{cid:convId,msg:m});}});
    return json(res,msg);
  }
  if(pathname.startsWith('/api/widget/msgs/')&&method==='GET'){
    const convId=pathname.split('/api/widget/msgs/')[1];
    const msgs=db.all('messages').filter(m=>m.convId===convId).sort((a,b)=>new Date(a.created)-new Date(b.created));
    return json(res,msgs);
  }

  if(pathname==='/api/login'&&method==='POST'){
    const u=db.findBy('users','email',(body.email||'').toLowerCase().trim());
    if(!u||u.password!==hash((body.password||'').trim()))return json(res,{error:'Wrong email or password'},401);
    db.update('users',u.id,{status:'offline',updated:now()});
    const ag=db.findBy('agents','email',u.email);
    if(ag) db.update('agents',ag.id,{status:'offline'});
    const sid=makeSession(); sessions.get(sid).uid=u.id; persistSessions();
    res.setHeader('Set-Cookie',`nsid=${sid}; HttpOnly; Path=/; Max-Age=86400`);
    return json(res,{id:u.id,name:u.name,email:u.email,role:u.role,channel:u.channel||'both',visibility:u.visibility||'all',status:'offline',phone:u.phone,avatar:u.avatar||''});
  }
  if(pathname==='/api/logout'&&method==='POST'){
    const sess=getSession(req);
    if(sess){
      const u=db.find('users',sess.uid);
      if(u){db.update('users',u.id,{status:'offline',updated:now()});const ag=db.findBy('agents','email',u.email);if(ag)db.update('agents',ag.id,{status:'offline'});}
      sessions.delete(sess.id);
    }
    res.setHeader('Set-Cookie','nsid=; HttpOnly; Path=/; Max-Age=0');return json(res,{ok:true});
  }
  if(pathname==='/api/me'){
    if(method==='GET'){const sess=requireAuth(req,res);if(!sess)return;const u=db.find('users',sess.uid);if(!u){res.setHeader('Set-Cookie','nsid=; HttpOnly; Path=/; Max-Age=0');return json(res,{error:'User not found'},401);}return json(res,{id:u.id,name:u.name,email:u.email,role:u.role,channel:u.channel||'both',visibility:u.visibility||'all',status:u.status||'offline',phone:u.phone||'',avatar:u.avatar||'',theme:u.theme||''});}
    if(method==='PATCH'){
      const sess=requireAuth(req,res);if(!sess)return;
      const updates={updated:now()};
      if(body.name!==undefined)   updates.name=body.name;
      if(body.phone!==undefined)  updates.phone=body.phone;
      if(body.status!==undefined) updates.status=body.status;
      if(body.avatar!==undefined) updates.avatar=body.avatar;
      if(body.theme!==undefined)  updates.theme=body.theme;
      const u=db.update('users',sess.uid,updates);
      if(u){const ag=db.findBy('agents','email',u.email);if(ag)db.update('agents',ag.id,{name:u.name,status:u.status||'offline'});}
      if(body.status!==undefined) broadcast('agent_status',{name:u?.name,status:body.status});
      if(body.status!==undefined && u){
        const d2=db.get(); if(!d2.agentLogs)d2.agentLogs=[];
        d2.agentLogs.push({id:genId(),agentId:u.id,agentName:u.name,status:body.status,time:now()});
        writeDB(d2);
      }
      if(body.status && body.status !== 'offline') {
        const unassigned = db.all('convs').filter(c => c.status === 'open' && !c.agent);
        for(const conv of unassigned) autoAssign(conv);
      }
      return json(res,u||{});
    }
  }

  if(pathname==='/api/config'){
    if(method==='GET'){const sess=requireAuth(req,res);if(!sess)return;const c=getCfg();return json(res,{gmail_user:c.gmail_user||'',gmail_ok:!!(c.gmail_user&&c.gmail_pass),ai_key_set:!!c.ai_key,ai_enabled:!!c.ai_enabled,theme:c.theme||'dark',assignment_mode:c.assignment_mode||'none',max_load:c.max_load||10,kb_url:c.kb_url||'',kb_synced:!!(c.kb_content&&c.kb_content.length>100),kb_synced_at:c.kb_synced_at||'',slack_webhook:c.slack_webhook?'set':'',slack_channel:c.slack_channel||'',slack_channel_url:c.slack_channel_url||'',email_assign_mode:c.email_assign_mode||c.assignment_mode||'none',email_max_load:c.email_max_load||c.max_load||10,chat_assign_mode:c.chat_assign_mode||c.assignment_mode||'none',chat_max_load:c.chat_max_load||c.max_load||10,social_assign_mode:c.social_assign_mode||c.assignment_mode||'none',social_max_load:c.social_max_load||c.max_load||10,csat_enabled:!!c.csat_enabled,csat_rating_type:c.csat_rating_type||'stars',csat_question:c.csat_question||'How would you rate your support experience?',csat_send_when:c.csat_send_when||'resolve',csat_delay_hours:c.csat_delay_hours||0,biz_hours_enabled:!!c.biz_hours_enabled,biz_hours_start:c.biz_hours_start||9,biz_hours_end:c.biz_hours_end||18,biz_hours_days:c.biz_hours_days||[1,2,3,4,5],auto_reply_email_enabled:!!c.auto_reply_email_enabled,auto_reply_email_msg:c.auto_reply_email_msg||'Thanks for reaching out! We\'ve received your email and will respond within 8 hours.',auto_reply_chat_enabled:!!c.auto_reply_chat_enabled,auto_reply_chat_msg:c.auto_reply_chat_msg||'Thanks for starting a chat! An agent will be with you shortly.',auto_reply_social_enabled:!!c.auto_reply_social_enabled,auto_reply_social_msg:c.auto_reply_social_msg||'Thanks for your message! Our team will respond soon.',saved_filters:c.saved_filters||[]});}
    if(method==='POST'||method==='PATCH'){const sess=requireAuth(req,res);if(!sess)return;const _u2=db.find('users',sess.uid);if(_u2&&_u2.role==='manager')return json(res,{error:'Managers cannot change settings'},403);const old=getCfg(),n={...old};
      ['gmail_user','gmail_pass','ai_key','theme','assignment_mode','kb_url','slack_webhook','slack_channel','slack_channel_url','email_assign_mode','email_max_load','email_rr_index','chat_assign_mode','chat_max_load','chat_rr_index','social_assign_mode','social_max_load','routingRules','sla_email_first','sla_email_resolve','autoclose_email','sla_chat_first','autoclose_chat','sla_social_first','csat_question','csat_rating_type','csat_send_when','csat_delay_hours','biz_hours_start','biz_hours_end','auto_reply_email_msg','auto_reply_chat_msg','auto_reply_social_msg','saved_filters'].forEach(k=>{if(body[k]!==undefined)n[k]=typeof body[k]==='string'?body[k].trim():body[k];});
      if(body.ai_enabled!==undefined)n.ai_enabled=!!body.ai_enabled;
      if(body.csat_enabled!==undefined)n.csat_enabled=!!body.csat_enabled;
      if(body.biz_hours_enabled!==undefined)n.biz_hours_enabled=!!body.biz_hours_enabled;
      if(body.biz_hours_days!==undefined)n.biz_hours_days=body.biz_hours_days;
      if(body.auto_reply_email_enabled!==undefined)n.auto_reply_email_enabled=!!body.auto_reply_email_enabled;
      if(body.auto_reply_chat_enabled!==undefined)n.auto_reply_chat_enabled=!!body.auto_reply_chat_enabled;
      if(body.auto_reply_social_enabled!==undefined)n.auto_reply_social_enabled=!!body.auto_reply_social_enabled;
      if(body.max_load!==undefined)n.max_load=parseInt(body.max_load)||10;
      setCfg(n);startPolling();return json(res,{ok:true});}
  }

  if(pathname==='/api/ticket-logic'){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='GET'){const d=db.get();return json(res,d.ticketLogic||[]);}
    if(method==='POST'){
      const d=db.get();if(!d.ticketLogic)d.ticketLogic=[];
      const rule={id:genId(),channel:body.channel||'email',event:body.event||'customer_reply_after_resolve',condition:body.condition||'days_since_resolve',value:Number(body.value)||0,action:body.action||'reopen',label:body.label||'Rule',active:true,created:now()};
      d.ticketLogic.push(rule);writeDB(d);return json(res,rule);
    }
  }
  const tlMatch=pathname.match(/^\/api\/ticket-logic\/([^/]+)$/);
  if(tlMatch){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='PATCH'){const d=db.get();const i=(d.ticketLogic||[]).findIndex(x=>x.id===tlMatch[1]);if(i!==-1){d.ticketLogic[i]={...d.ticketLogic[i],...body};writeDB(d);}return json(res,{ok:true});}
    if(method==='DELETE'){const d=db.get();d.ticketLogic=(d.ticketLogic||[]).filter(x=>x.id!==tlMatch[1]);writeDB(d);return json(res,{ok:true});}
  }

  if(pathname==='/api/slack/config'){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='GET'){const c=getCfg();return json(res,{channels:c.slack_channels||[],pocs:c.slack_pocs||[]});}
    if(method==='POST'){
      const old=getCfg(),n={...old};
      if(body.channels!==undefined)n.slack_channels=body.channels;
      if(body.pocs!==undefined)n.slack_pocs=body.pocs;
      setCfg(n);return json(res,{ok:true});
    }
  }
  if(pathname==='/api/slack/flag'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const{convId,channelWebhook,channelName,channelUrl,title,details,priority,opsCat,sku,uhr,assignee,pocName,pocMemberId,ticketId,customerName,customerEmail,ticketChannel,agentName,ticketStatus}=body;
    if(!channelWebhook)return json(res,{error:'No webhook — select a configured Slack channel'},400);
    if(!title||!details)return json(res,{error:'Title and details are required'},400);
    const sevEmoji={Critical:'🔴',High:'🟠',Medium:'🟡',Low:'🟢'}[priority]||'🟠';
    const pocTag=pocMemberId?`<@${pocMemberId}>`:(pocName?`@${pocName}`:'');
    const blocks=[
      {type:'header',text:{type:'plain_text',text:`🚩 Ticket Raised — ${ticketId}`,emoji:true}},
      {type:'section',fields:[
        {type:'mrkdwn',text:`*Created By:*\n${agentName}`},
        {type:'mrkdwn',text:`*Assignee:*\n${assignee||'Unassigned'}${pocTag?' · POC: '+pocTag:''}`}
      ]},
      {type:'section',text:{type:'mrkdwn',text:`*Title:*\n${title}`}},
      {type:'section',text:{type:'mrkdwn',text:`*Details:*\n${details}`}},
      {type:'divider'},
      {type:'section',fields:[
        {type:'mrkdwn',text:`*Email ID:*\n${customerEmail||'—'}`},
        {type:'mrkdwn',text:`*Ops Category:*\n${opsCat||'—'}`},
        {type:'mrkdwn',text:`*SKU:*\n${sku||'—'}`},
        {type:'mrkdwn',text:`*UHR:*\n${uhr||'—'}`},
        {type:'mrkdwn',text:`*Priority:*\n${sevEmoji} ${priority||'High'}`},
        {type:'mrkdwn',text:`*Status:*\n${ticketStatus?ticketStatus.charAt(0).toUpperCase()+ticketStatus.slice(1):'Open'}`}
      ]},
      {type:'section',text:{type:'mrkdwn',text:`*Conversation ID:* ${ticketId}   *Channel:* ${ticketChannel}`}},
      {type:'context',elements:[{type:'mrkdwn',text:`Posted at ${new Date().toLocaleString()} via Nexus Support Desk${pocTag?' · Assigned to '+pocTag:''}`}]}
    ];
    if(pocTag){blocks.splice(1,0,{type:'section',text:{type:'mrkdwn',text:`👀 Hey ${pocTag} — please review this ticket!`}});}
    const payload={blocks};
    try{
      const payloadStr=JSON.stringify(payload);
      const whUrl=new URL(channelWebhook);
      const result=await new Promise((resolve,reject)=>{
        const opts={hostname:whUrl.hostname,path:whUrl.pathname+whUrl.search,method:'POST',headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(payloadStr)}};
        const r2=https.request(opts,r=>{let d='';r.on('data',c=>d+=c);r.on('end',()=>resolve(d));});
        r2.on('error',reject);r2.setTimeout(10000,()=>{r2.destroy();reject(new Error('timeout'));});r2.write(payloadStr);r2.end();
      });
      if(result==='ok'){
        if(convId)db.addActivity(convId,{type:'flagged',text:`Raised on Slack (${channelName}) — ${priority}${pocName?' · POC: '+pocName:''}`,agent:agentName});
        return json(res,{ok:true,channelUrl:channelUrl||channelName||'#slack'});
      } else {
        return json(res,{error:'Slack returned: '+result},500);
      }
    }catch(e){return json(res,{error:e.message},500);}
  }

  if(pathname==='/api/mentions'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const{mentions,convId,convName,fromName}=body;
    if(!mentions||!mentions.length)return json(res,{ok:true});
    for(const targetName of mentions){broadcast('mention',{targetName,fromName,convId,convName});}
    return json(res,{ok:true});
  }

  if(pathname.startsWith('/api/watchers/')&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const convId=pathname.split('/api/watchers/')[1];
    if(!convId)return json(res,{error:'missing convId'},400);
    const{name}=body;const ts=Date.now();
    if(!global._watchers)global._watchers={};
    if(!global._watchers[convId])global._watchers[convId]=[];
    const existing=global._watchers[convId].find(w=>w.name===name);
    if(existing)existing.ts=ts;
    else global._watchers[convId].push({name,ts});
    global._watchers[convId]=global._watchers[convId].filter(w=>ts-w.ts<20000);
    const watchers=global._watchers[convId].map(w=>({name:w.name}));
    broadcast('watcher_update',{convId,watchers});
    return json(res,{ok:true,watchers});
  }

  if(pathname==='/api/translate'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();
    if(!cfg.ai_key)return json(res,{error:'No API key configured. Add OpenAI key in Settings → AI Assistant.'},400);
    const text=body.text||'';const target=body.target||body.lang||'English';
    if(!text.trim())return json(res,{error:'No text provided'},400);
    const prompt=`Translate the following text to ${target}. Return ONLY the translated text, nothing else, no explanations.\n\n${text}`;
    try{
      const result=await callOpenAI([{role:'user',content:prompt}],'You are a professional translator. Translate accurately and naturally.',400);
      if(result?.error)return json(res,{error:result.error.message||'Translation failed'},500);
      const translated=(result?.choices?.[0]?.message?.content||'').trim();
      if(!translated)return json(res,{error:'Empty translation response'},500);
      return json(res,{translated,result:translated,target});
    }catch(e){return json(res,{error:e.message},500);}
  }

  if(pathname==='/api/kb/suggest'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();
    if(!cfg.ai_key)return json(res,{suggestions:[],error:'no_api_key'});
    const query=body.query||'';const convId=body.convId||'';
    if(!query)return json(res,{suggestions:[],error:'no_query'});
    const kb=cfg.kb_content?'\n\nKNOWLEDGE BASE (use this to answer accurately):\n'+cfg.kb_content.substring(0,12000):'';
    const hist=db.all('messages').filter(m=>m.convId===convId).slice(-5).map(m=>(m.type==='customer'?'Customer':'Agent')+': '+m.content).join('\n');
    const userMsg='Ticket: '+query+(hist?'\nConversation:\n'+hist:'')+(kb?kb:'');
    const sys='You are an customer support expert. Write 1 concise, accurate reply (2-3 sentences) the agent can send directly. Be warm and professional. Use the knowledge base if available.\n\nRespond ONLY with this exact JSON (no markdown, no extra text):\n{"suggestions":[{"reply":"your reply here","source":"null"}]}';
    try{
      const result=await callOpenAI([{role:'user',content:userMsg}],sys,500);
      if(!result){return json(res,{suggestions:[],error:'no_response'});}
      if(result.error){return json(res,{suggestions:[],error:result.error.message||'api_error'});}
      const text=(result.choices&&result.choices[0]&&result.choices[0].message&&result.choices[0].message.content)||'';
      if(!text)return json(res,{suggestions:[],error:'empty_response'});
      const jsonMatch=text.match(/\{[\s\S]*\}/);
      if(!jsonMatch)return json(res,{suggestions:[],error:'no_json'});
      const parsed=JSON.parse(jsonMatch[0]);
      return json(res,{suggestions:parsed.suggestions||[]});
    }catch(e){return json(res,{suggestions:[],error:e.message});}
  }

  if(pathname==='/api/kb/sync'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();const kbUrl=body.url||cfg.kb_url||'';
    if(!kbUrl)return json(res,{error:'No KB URL set'},400);
    try{
      const parsed=new URL(kbUrl);
      const isHttps=parsed.protocol==='https:';
      const lib=isHttps?https:http;
      const rawText=await new Promise((resolve,reject)=>{
        const opts={hostname:parsed.hostname,path:parsed.pathname+(parsed.search||''),headers:{'User-Agent':'Mozilla/5.0 (compatible; Nexus/1.0)','Accept':'text/html,application/xhtml+xml'}};
        const req2=lib.request(opts,r=>{
          if((r.statusCode===301||r.statusCode===302||r.statusCode===307)&&r.headers.location){r.resume();return resolve('REDIRECT:'+r.headers.location);}
          let d='';r.on('data',c=>d+=c);r.on('end',()=>resolve(d));
        });
        req2.on('error',reject);req2.setTimeout(15000,()=>{req2.destroy();reject(new Error('timeout'));});req2.end();
      });
      const text=rawText.replace(/<script[\s\S]*?<\/script>/gi,'').replace(/<style[\s\S]*?<\/style>/gi,'').replace(/<[^>]+>/g,' ').replace(/\s{3,}/g,'\n').replace(/\n{4,}/g,'\n\n').trim().substring(0,60000);
      if(text.length<100)return json(res,{error:'Could not extract content — the page may require JavaScript to render. Try pasting content manually.',words:0},422);
      const n={...cfg,kb_url:kbUrl,kb_content:text,kb_synced_at:now()};setCfg(n);
      return json(res,{ok:true,words:text.split(/\s+/).length,chars:text.length});
    }catch(e){return json(res,{error:e.message},500);}
  }
  if(pathname==='/api/kb/content'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();
    return json(res,{content:cfg.kb_content||'',url:cfg.kb_url||'',synced_at:cfg.kb_synced_at||''});
  }
  if(pathname==='/api/kb/manual'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();
    if(!body.content)return json(res,{error:'No content'},400);
    const n={...cfg,kb_content:body.content.substring(0,60000),kb_url:cfg.kb_url||'manual',kb_synced_at:now()};
    setCfg(n);return json(res,{ok:true,words:body.content.split(/\s+/).length});
  }

  if(pathname==='/api/gmail/test'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const cfg=getCfg();if(!cfg.gmail_user||!cfg.gmail_pass)return json(res,{error:'Save credentials first'},400);
    const result=await new Promise(resolve=>{
      let buf='',st='greeting',tn=1,fin=false;
      const done=r=>{if(!fin){fin=true;try{sock.destroy();}catch(_){};resolve(r);}};
      setTimeout(()=>done({ok:false,error:'Connection timeout'}),15000);
      const sock=tls.connect({host:'imap.gmail.com',port:993,rejectUnauthorized:false},()=>sock.setEncoding('utf8'));
      const send=cmd=>{if(!sock.destroyed)sock.write(cmd+'\r\n');};
      sock.on('data',chunk=>{
        buf+=chunk;const lines=buf.split('\r\n');buf=lines.pop();
        for(const l of lines){
          if(st==='greeting'&&(l.startsWith('* OK')||l.includes('IMAP'))){st='login';send(`T${tn++} LOGIN "${cfg.gmail_user}" "${cfg.gmail_pass}"`);}
          else if(st==='login'&&l.includes(`T${tn-1} OK`)){st='select';send(`T${tn++} SELECT INBOX`);}
          else if(st==='login'&&(l.includes('NO')||l.includes('AUTHENTICATIONFAILED'))){done({ok:false,error:'Authentication failed. Use a 16-char Google App Password.'});}
          else if(st==='select'&&l.includes(`T${tn-1} OK`)){st='search';send(`T${tn++} UID SEARCH ALL`);}
          else if(st==='search'&&l.startsWith('* SEARCH')){const n=l.replace('* SEARCH','').trim().split(' ').filter(Boolean).length;done({ok:true,count:n,msg:`Connected! Found ${n} emails.`});}
          else if(st==='search'&&l.includes(`T${tn-1} OK`)&&!fin){done({ok:true,count:0,msg:'Connected! Inbox empty.'});}
        }
      });
      sock.on('error',e=>done({ok:false,error:e.message}));
    });
    if(result.ok)pollGmail();
    return json(res,result);
  }
  if(pathname==='/api/gmail/poll'&&method==='POST'){const sess=requireAuth(req,res);if(!sess)return;pollGmail();return json(res,{ok:true});}
  if(pathname==='/api/fix-ticket-ids'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const d=db.get();
    if(!d.counters)d.counters={email:0,chat:0,social:0};
    const channels={email:'TKT',chat:'CHT',social:'SOC'};
    let fixed=0;
    for(const conv of (d.convs||[])){
      if(!conv.ticketId){
        const ch=conv.channel||'email';
        const prefix=channels[ch]||'TKT';
        d.counters[ch]=(d.counters[ch]||0)+1;
        conv.ticketId=`${prefix}-${String(d.counters[ch]).padStart(4,'0')}`;
        fixed++;
      }
      if(!conv.activity)conv.activity=[{type:'created',text:'Ticket created',time:conv.created||now()}];
    }
    writeDB(d);
    broadcast('reload',{});
    return json(res,{ok:true,fixed});
  }
  if(pathname==='/api/gmail/reimport'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const d=db.get(); d.seenUids=[]; writeDB(d);
    pollGmail();
    return json(res,{ok:true,msg:'UIDs cleared — re-importing all emails now'});
  }

  if(pathname==='/api/social'){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='GET')return json(res,db.all('socialAccounts').map(a=>({...a,access_token:a.access_token?'***':'',page_access_token:a.page_access_token?'***':'',bearer_token:a.bearer_token?'***':'',api_key:a.api_key?'***':''})));
    if(method==='POST'){const acc=db.insert('socialAccounts',{id:genId(),...body,active:true,created:now(),updated:now()});startSocialPolling();return json(res,{...acc,access_token:'***',page_access_token:'***',bearer_token:'***'});}
  }
  const socialMatch=pathname.match(/^\/api\/social\/([^/]+)$/);
  if(socialMatch){
    const sess=requireAuth(req,res);if(!sess)return;
    if(method==='DELETE'){db.remove('socialAccounts',socialMatch[1]);startSocialPolling();return json(res,{ok:true});}
    if(method==='PATCH'){const a=db.update('socialAccounts',socialMatch[1],{...body,updated:now()});startSocialPolling();return json(res,a||{});}
  }
  if(pathname==='/api/social/poll'&&method==='POST'){const sess=requireAuth(req,res);if(!sess)return;pollAllSocial();return json(res,{ok:true});}

  if(pathname==='/api/convs'){
    if(method==='GET'){
      const sess=requireAuth(req,res);if(!sess)return;
      const u=db.find('users',sess.uid);
      let r=db.all('convs');
      if(u&&u.role!=='admin'){
        const ch=u.channel||'both';
        if(ch==='chat')   r=r.filter(c=>c.channel==='chat');
        else if(ch==='email')  r=r.filter(c=>c.channel==='email');
        else if(ch==='social') r=r.filter(c=>c.channel==='social');
      }
      r=filterConvsByVisibility(r,u);
      if(parsed.query.ch&&parsed.query.ch!=='social')r=r.filter(c=>c.channel===parsed.query.ch);
      if(parsed.query.ch==='social')r=r.filter(c=>c.channel==='social');
      if(parsed.query.platform)r=r.filter(c=>c.platform===parsed.query.platform);
      if(parsed.query.st)r=r.filter(c=>c.status===parsed.query.st);
      return json(res,r);
    }
    if(method==='POST'){
      const sess=requireAuth(req,res);if(!sess)return;
      const u=db.find('users',sess.uid);
      const{name,email,channel,subject,priority}=body;
      if(!name)return json(res,{error:'Name required'},400);
      let cust=email?db.findBy('customers','email',email):null;
      if(!cust)cust=db.insert('customers',{id:genId(),name,email:email||'',phone:'',created:now()});
      const ticketId=db.nextId(channel||'chat');
      const c=db.insert('convs',{
        id:genId(), ticketId, custId:cust.id, name, email:email||'',
        channel:channel||'chat', status:'open', agent:'', tags:[],
        subject:subject||'', priority:priority||'normal', platform:channel||'chat',
        created:now(), updated:now(),
        activity:[{type:'created',text:`${ticketId} created by ${u?.name||'Agent'}`,time:now()}]
      });
      autoAssign(c); broadcast('new_conv',c); return json(res,c);
    }
  }

  const actMatch=pathname.match(/^\/api\/convs\/([^/]+)\/activity$/);
  if(actMatch&&method==='GET'){const sess=requireAuth(req,res);if(!sess)return;const conv=db.find('convs',actMatch[1]);return json(res,(conv?.activity||[]).slice().reverse());}

  const convPatch=pathname.match(/^\/api\/convs\/([^/]+)$/);
  if(convPatch&&method==='PATCH'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);
    const agentName=u?.name||'Agent';
    const convId=convPatch[1];
    const prev=db.find('convs',convId);
    if(prev){
      if(body.status && body.status!==prev.status) {
        db.addActivity(convId,{type:body.status,text:`Status → ${body.status}`,agent:agentName});
        // Trigger CSAT on resolve
        if(body.status==='resolved' && prev.channel==='email' && prev.email) {
          const _csatCfg=getCfg();
          if(_csatCfg.csat_enabled) {
            const _delayMs=(parseInt(_csatCfg.csat_delay_hours||0))*3600000;
            setTimeout(()=>sendCSATEmail(prev),Math.max(_delayMs,2000));
          }
        }
      }
      if(body.agent!==undefined && body.agent!==prev.agent)
        db.addActivity(convId,{type:'assigned',text:body.agent?`Assigned to ${body.agent}`:'Unassigned',agent:agentName});
      if(body.tags && JSON.stringify(body.tags)!==JSON.stringify(prev.tags)){
        const added=(body.tags||[]).filter(t=>!(prev.tags||[]).includes(t));
        const removed=(prev.tags||[]).filter(t=>!(body.tags||[]).includes(t));
        if(added.length)db.addActivity(convId,{type:'tag',text:`Tag added: ${added.join(', ')}`,agent:agentName});
        if(removed.length)db.addActivity(convId,{type:'tag',text:`Tag removed: ${removed.join(', ')}`,agent:agentName});
      }
      if(body.priority && body.priority!==prev.priority)
        db.addActivity(convId,{type:'priority',text:`Priority → ${body.priority}`,agent:agentName});
    }
    const{_agentName,...saveBody}=body;
    const c=db.update('convs',convId,{...saveBody,updated:now()});
    broadcast('conv_update',c||{});
    return json(res,c||{});
  }

  const msgsMatch=pathname.match(/^\/api\/convs\/([^/]+)\/msgs$/);
  if(msgsMatch){
    if(method==='GET'){const sess=requireAuth(req,res);if(!sess)return;const msgs=db.all('messages').filter(m=>m.convId===msgsMatch[1]).sort((a,b)=>new Date(a.created)-new Date(b.created));return json(res,msgs);}
    if(method==='POST'){
      const sess=requireAuth(req,res);if(!sess)return;
      const{content,type='agent',name='Agent'}=body;if(!content)return json(res,{error:'Content required'},400);
      const u=db.find('users',sess.uid);
      const msg=db.insert('messages',{id:genId(),convId:msgsMatch[1],type,name:name||(u?.name||'Agent'),content,created:now()});
      db.update('convs',msgsMatch[1],{updated:now()}); broadcast('new_msg',{cid:msgsMatch[1],msg});
      if(type==='customer'){
        const rules=db.all('botRules').filter(r=>r.active);const low=content.toLowerCase();let hit=false;
        for(const r of rules){if(low.includes(r.keyword.toLowerCase())){hit=true;setTimeout(()=>{const m=db.insert('messages',{id:genId(),convId:msgsMatch[1],type:'bot',name:'Bot',content:r.response,created:now()});db.update('convs',msgsMatch[1],{updated:now()});broadcast('new_msg',{cid:msgsMatch[1],msg:m});},900);break;}}
        if(!hit)aiReply(content,msgsMatch[1]).then(ai=>{if(ai){const m=db.insert('messages',{id:genId(),convId:msgsMatch[1],type:'bot',name:'AI',content:ai,created:now()});db.update('convs',msgsMatch[1],{updated:now()});broadcast('new_msg',{cid:msgsMatch[1],msg:m});}});
      }
      if(type==='agent') db.addActivity(msgsMatch[1],{type:'reply',text:`Agent replied`,agent:u?.name||'Agent'});
      return json(res,msg);
    }
  }

  if(pathname==='/api/email/send'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);
    const{to,subject,mailbody,cid,inReplyTo,references}=body;
    try{
      // Use OAuth send with threading headers if available
      const cfg2=getCfg();
      if(cfg2.gmail_refresh_token){
        // Build raw email with In-Reply-To for proper threading
        const token2=await getGmailToken();
        if(token2){
          const fromAddr=cfg2.gmail_user||'me';
          let rawLines=[
            `From: ${fromAddr}`,`To: ${to}`,
            `Subject: =?UTF-8?B?${Buffer.from(subject).toString('base64')}?=`,
            'MIME-Version: 1.0','Content-Type: text/html; charset=UTF-8',
          ];
          if(inReplyTo) rawLines.push(`In-Reply-To: ${inReplyTo}`);
          if(references) rawLines.push(`References: ${references}`);
          rawLines.push('','');
          rawLines.push(mailbody||'');
          const raw=Buffer.from(rawLines.join('\r\n')).toString('base64url');
          const sendBody={raw};
          // If we have a threadId, add it to keep in same Gmail thread
          if(cid){const conv2=db.find('convs',cid);if(conv2&&conv2.gmailThreadId)sendBody.threadId=conv2.gmailThreadId;}
          await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send',{
            method:'POST',headers:{'Authorization':'Bearer '+token2,'Content-Type':'application/json'},
            body:JSON.stringify(sendBody)
          });
        }
      } else {
        await sendGmail(to,subject,mailbody||'');
      }
      if(cid){
        const m=db.insert('messages',{id:genId(),convId:cid,type:'agent',name:u?.name||'Agent',content:mailbody,created:now()});
        db.update('convs',cid,{updated:now()});
        db.addActivity(cid,{type:'email_sent',text:`Email sent to ${to}`,agent:u?.name||'Agent'});
        broadcast('new_msg',{cid,msg:m});
      }
      return json(res,{ok:true});
    }catch(e){console.error('[EmailSend]',e.message);return json(res,{error:e.message},500);}
  }

  // ── MERGE TICKETS ──────────────────────────────────────────────────────────
  if(pathname==='/api/convs/merge'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);
    const{primaryId,mergeIds}=body;
    if(!primaryId||!mergeIds||!mergeIds.length)return json(res,{error:'primaryId and mergeIds required'},400);
    const primary=db.find('convs',primaryId);
    if(!primary)return json(res,{error:'Primary ticket not found'},404);
    const merged=[];
    for(const mid of mergeIds){
      if(mid===primaryId)continue;
      const conv=db.find('convs',mid);
      if(!conv){merged.push({id:mid,error:'not found'});continue;}
      // Move all messages from merged ticket to primary
      const msgs=db.all('messages').filter(m=>m.convId===mid);
      for(const m of msgs){db.update('messages',m.id,{convId:primaryId});}
      // Merge tags (deduplicate)
      const combinedTags=[...new Set([...(primary.tags||[]),...(conv.tags||[])])];
      db.update('convs',primaryId,{tags:combinedTags,updated:now()});
      // Copy activity log entries
      const acts=(conv.activity||[]).map(a=>({...a,text:'[Merged from '+conv.ticketId+'] '+a.text}));
      const primActs=primary.activity||[];
      db.update('convs',primaryId,{activity:[...primActs,...acts]});
      // Mark merged ticket as resolved with note
      db.update('convs',mid,{status:'resolved',updated:now()});
      db.addActivity(mid,{type:'merged',text:'Merged into '+primary.ticketId,agent:u?.name||'Agent'});
      db.addActivity(primaryId,{type:'merged',text:'Ticket '+conv.ticketId+' merged in',agent:u?.name||'Agent'});
      merged.push({id:mid,ticketId:conv.ticketId,ok:true});
    }
    // Refresh primary conv
    const updatedPrimary=db.find('convs',primaryId);
    broadcast('conv_update',updatedPrimary);
    return json(res,{ok:true,primary:updatedPrimary,merged});
  }

  const annAck=pathname.match(/^\/api\/announcements\/([^/]+)\/ack$/);
  if(annAck&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);if(!u)return json(res,{error:'Not found'},400);
    const ann=db.find('announcements',annAck[1]);if(!ann)return json(res,{error:'Not found'},404);
    const acks=ann.acks||[];
    if(!acks.find(a=>a.userId===sess.uid)){acks.push({userId:sess.uid,name:u.name,time:now()});db.update('announcements',annAck[1],{acks});}
    return json(res,{ok:true,acks});
  }

  function crud(table,makeRow){
    const sess=requireAuth(req,res);if(!sess)return true;
    if(method==='GET'){json(res,db.all(table));return true;}
    if(method==='POST'){const row=makeRow(body);if(!row)return(json(res,{error:'Missing fields'},400),true);json(res,db.insert(table,row));return true;}
    const m=pathname.match(/\/([^/]+)$/);
    if(m&&method==='PATCH'){json(res,db.update(table,m[1],{...body,updated:now()})||{});return true;}
    if(m&&method==='DELETE'){db.remove(table,m[1]);json(res,{ok:true});return true;}
    return false;
  }
  if(pathname.startsWith('/api/groups'))       return crud('groups',      b=>b.name?{id:genId(),name:b.name,description:b.description||'',channels:b.channels||[],agents:b.agents||[],color:b.color||'#00e5b4',created:now()}:null);
  if(pathname.startsWith('/api/ticket-logic')) return crud('ticketLogic', b=>b.channel&&b.action&&b.event?{id:genId(),channel:b.channel,event:b.event,value:b.value||0,action:b.action,label:b.label||b.action,active:b.active!==false,created:now()}:null);
  if(pathname.startsWith('/api/tags'))          return crud('tags',         b=>b.name               ?{id:genId(),name:b.name,color:b.color||'#3b82f6',created:now()}:null);
  if(pathname.startsWith('/api/canned'))        return crud('canned',       b=>b.title&&b.content   ?{id:genId(),title:b.title,shortcut:b.shortcut||'',content:b.content,created:now()}:null);
  if(pathname.startsWith('/api/bot-rules'))     return crud('botRules',     b=>b.keyword&&b.response?{id:genId(),keyword:b.keyword,response:b.response,active:true,created:now()}:null);
  if(pathname.startsWith('/api/agents'))        return crud('agents',       b=>b.name&&b.email      ?{id:genId(),name:b.name,email:b.email,role:b.role||'agent',channel:b.channel||'both',status:'online',created:now()}:null);
  if(pathname.startsWith('/api/announcements')) return crud('announcements',b=>b.title&&b.body      ?{id:genId(),title:b.title,body:b.body,pinned:!!b.pinned,acks:[],created:now()}:null);
  if(pathname.startsWith('/api/customers')&&method==='GET'){const sess=requireAuth(req,res);if(!sess)return;return json(res,db.all('customers'));}

  if(pathname.startsWith('/api/users')){const sess=requireAuth(req,res);if(!sess)return;if(method==='GET')return json(res,db.all('users').map(u=>({...u,password:undefined})));const _mgr=db.find('users',sess.uid);if(_mgr&&_mgr.role==='manager'&&method!=='GET')return json(res,{error:'Managers cannot modify users'},403);
    if(method==='POST'){
      if(db.findBy('users','email',body.email))return json(res,{error:'Email already exists'},400);
      const u=db.insert('users',{id:genId(),name:body.name,email:body.email,password:hash(body.password||'nexus123'),role:body.role||'agent',channel:body.channel||'both',visibility:body.visibility||'all',phone:'',status:'online',avatar:'',created:now()});
      if(!db.findBy('agents','email',body.email))
        db.insert('agents',{id:genId(),name:body.name,email:body.email,role:body.role||'agent',channel:body.channel||'both',status:'online',created:now()});
      return json(res,{...u,password:undefined});
    }
    const m=pathname.match(/\/api\/users\/([^/]+)$/);
    if(m&&method==='PATCH'){
      const u=db.update('users',m[1],{...body,password:body.password?hash(body.password):undefined,updated:now()});
      if(u){const ag=db.findBy('agents','email',u.email);if(ag)db.update('agents',ag.id,{name:u.name,channel:u.channel||'both'});}
      return json(res,{...u,password:undefined}||{});
    }
    if(m&&method==='DELETE'){
      const u=db.find('users',m[1]);
      if(u){const ag=db.findBy('agents','email',u.email);if(ag)db.remove('agents',ag.id);}
      db.remove('users',m[1]);return json(res,{ok:true});
    }
  }

  if(pathname==='/api/me/avatar'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const{avatar}=body;
    if(!avatar||!avatar.startsWith('data:image/'))return json(res,{error:'Invalid image'},400);
    if(avatar.length>2*1024*1024)return json(res,{error:'Image too large (max 2MB)'},400);
    const u=db.update('users',sess.uid,{avatar,updated:now()});
    return json(res,{ok:true,avatar});
  }

  if(pathname==='/api/analytics'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const allConvs=db.all('convs'),msgs=db.all('messages'),custs=db.all('customers'),allUsers=db.all('users');
    const from=parsed.query.from?new Date(parsed.query.from):null;
    const to=parsed.query.to?new Date(parsed.query.to+'T23:59:59'):null;
    const chF=parsed.query.channel||'all';
    const convs=allConvs.filter(c=>{
      if(chF!=='all'&&c.channel!==chF)return false;
      if(!from&&!to)return true;
      const d=new Date(c.created);return(!from||d>=from)&&(!to||d<=to);
    });
    function fmtMin(ms){if(!ms||ms<0)return null;const m=Math.round(ms/60000);if(m<60)return m+'m';return Math.floor(m/60)+'h '+m%60+'m';}
    function avg(arr){return arr.length?Math.round(arr.reduce((a,b)=>a+b,0)/arr.length):null;}
    const byTag={},byDay={},byPlatform={},byHour=Array(24).fill(0),agentMap={};
    const allFRT=[],allRT=[];
    convs.forEach(c=>{
      (c.tags||[]).forEach(t=>byTag[t]=(byTag[t]||0)+1);
      const dd=(c.created||'').split('T')[0];if(dd)byDay[dd]=(byDay[dd]||0)+1;
      byPlatform[c.platform||c.channel||'unknown']=(byPlatform[c.platform||c.channel||'unknown']||0)+1;
      const h=new Date(c.created).getHours();byHour[h]=(byHour[h]||0)+1;
      const cM=msgs.filter(m=>m.convId===c.id).sort((a,b)=>new Date(a.created)-new Date(b.created));
      const fC=cM.find(m=>m.type==='customer'),fA=cM.find(m=>m.type==='agent'||m.type==='bot');
      const rA=(c.activity||[]).find(a=>a.type==='resolved');
      const frt=fC&&fA?new Date(fA.created)-new Date(fC.created):null;
      const rt=rA?new Date(rA.time)-new Date(c.created):null;
      if(frt!==null)allFRT.push(frt);
      if(rt!==null)allRT.push(rt);
      const ag=c.agent||'Unassigned';
      if(!agentMap[ag])agentMap[ag]={name:ag,total:0,resolved:0,frts:[],rts:[]};
      agentMap[ag].total++;
      if(c.status==='resolved')agentMap[ag].resolved++;
      if(frt!==null)agentMap[ag].frts.push(frt);
      if(rt!==null)agentMap[ag].rts.push(rt);
    });
    const byChSt=(ch,st)=>allConvs.filter(c=>c.channel===ch&&c.status===st).length;
    // ── FIX 1: agentStatuses reads from users table (source of truth for live status) ──
    const openConvsByAgent={};
    allConvs.filter(c=>c.status==='open'&&c.agent).forEach(c=>{openConvsByAgent[c.agent]=(openConvsByAgent[c.agent]||0)+1;});
    const agentStatuses=allUsers.map(u=>({
      name:u.name||u.email,
      status:u.status||'offline',
      channel:u.channel||'both',
      role:u.role||'agent',
      load:openConvsByAgent[u.name||u.email]||0
    }));
    const liveCount=agentStatuses.filter(a=>a.status==='online'||a.status==='away'||a.status==='lunch').length;
    const agentPerf=Object.values(agentMap).map(a=>({name:a.name,total:a.total,resolved:a.resolved,resolutionRate:a.total?Math.round(a.resolved/a.total*100):0,avgFirstResp:fmtMin(avg(a.frts)),avgResolve:fmtMin(avg(a.rts))}));
    // ── FIX 2: per-channel unassigned counts ──
    const emailUnassigned=allConvs.filter(c=>c.channel==='email'&&c.status==='open'&&!c.agent).length;
    const chatUnassigned=allConvs.filter(c=>c.channel==='chat'&&c.status==='open'&&!c.agent).length;
    const socialUnassigned=allConvs.filter(c=>c.channel==='social'&&c.status==='open'&&!c.agent).length;
    // Per-channel FRT and AHT
    function chFrt(ch){
      const frtArr=[];
      allConvs.filter(c=>c.channel===ch).forEach(c=>{
        const cM=msgs.filter(m=>m.convId===c.id).sort((a,b)=>new Date(a.created)-new Date(b.created));
        const fC=cM.find(m=>m.type==='customer'),fA=cM.find(m=>m.type==='agent'||m.type==='bot');
        if(fC&&fA)frtArr.push(new Date(fA.created)-new Date(fC.created));
      });
      return fmtMin(avg(frtArr));
    }
    function chAht(ch){
      const ahtArr=[];
      allConvs.filter(c=>c.channel===ch).forEach(c=>{
        const cM=msgs.filter(m=>m.convId===c.id).sort((a,b)=>new Date(a.created)-new Date(b.created));
        const fC=cM.find(m=>m.type==='customer');
        const rA=(c.activity||[]).find(a=>a.type==='resolved');
        if(fC&&rA)ahtArr.push(new Date(rA.time)-new Date(fC.created));
      });
      return fmtMin(avg(ahtArr));
    }
    const emailFrt=chFrt('email'),chatFrt=chFrt('chat'),socialFrt=chFrt('social');
    const emailAht=chAht('email'),chatAht=chAht('chat'),socialAht=chAht('social');
    return json(res,{
      total:convs.length,open:convs.filter(c=>c.status==='open').length,
      resolved:convs.filter(c=>c.status==='resolved').length,
      waiting:convs.filter(c=>c.status==='waiting').length,
      flagged:convs.filter(c=>c.status==='flagged').length,
      unassigned:convs.filter(c=>c.status==='open'&&!c.agent).length,
      emailUnassigned,chatUnassigned,socialUnassigned,
      emailUnassigned,chatUnassigned,socialUnassigned,
      chats:allConvs.filter(c=>c.channel==='chat').length,
      emails:allConvs.filter(c=>c.channel==='email').length,
      social:allConvs.filter(c=>c.channel==='social').length,
      emailOpen:byChSt('email','open'),emailWait:byChSt('email','waiting'),emailFlag:byChSt('email','flagged'),emailRes:byChSt('email','resolved'),
      chatOpen:byChSt('chat','open'),chatWait:byChSt('chat','waiting'),chatFlag:byChSt('chat','flagged'),chatRes:byChSt('chat','resolved'),
      socialOpen:byChSt('social','open'),socialWait:byChSt('social','waiting'),socialFlag:byChSt('social','flagged'),socialRes:byChSt('social','resolved'),
      customers:custs.length,messages:msgs.length,
      botMsgs:msgs.filter(m=>m.type==='bot').length,
      agentMsgs:msgs.filter(m=>m.type==='agent').length,
      avgFirstResponse:fmtMin(avg(allFRT)),
      emailFrt,chatFrt,socialFrt,
      emailAht,chatAht,socialAht,
      avgResolutionTime:fmtMin(avg(allRT)),
      slaBreaches:allFRT.filter(x=>x>8*3600000).length,
      liveCount,agentStatuses,agentPerf,
      byTag:Object.entries(byTag).sort((a,b)=>b[1]-a[1]).slice(0,15).map(([tag,count])=>({tag,count})),
      byDay:Object.entries(byDay).sort((a,b)=>a[0]<b[0]?-1:1).slice(-30).map(([d,c])=>({d,c})),
      byPlatform:Object.entries(byPlatform).map(([p,c])=>({p,c})),
      byHour:byHour.map((c,h)=>({h,c}))
    });
  }

  if(pathname==='/api/agent-logs'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const from=parsed.query.from?new Date(parsed.query.from):null;
    const to=parsed.query.to?new Date(parsed.query.to+'T23:59:59'):null;
    const agentName=parsed.query.agent||'';
    const d=db.get();const logs=(d.agentLogs||[]);
    function fmtDur(ms){if(!ms||ms<0)return'0m';const m=Math.round(ms/60000);if(m<60)return m+'m';return Math.floor(m/60)+'h '+m%60+'m';}
    let filtered=logs.filter(l=>{
      if(agentName&&l.agentName!==agentName)return false;
      if(!from&&!to)return true;
      const t=new Date(l.time);return(!from||t>=from)&&(!to||t<=to);
    }).sort((a,b)=>new Date(a.time)-new Date(b.time));
    const byAgent={};
    filtered.forEach(l=>{if(!byAgent[l.agentName])byAgent[l.agentName]=[];byAgent[l.agentName].push(l);});
    const sessions=[];
    Object.entries(byAgent).forEach(([name,evts])=>{
      let sStart=null,sSt=null;
      evts.forEach(ev=>{
        if((ev.status==='online'||ev.status==='away'||ev.status==='lunch')&&!sStart){sStart=ev.time;sSt=ev.status;}
        else if(ev.status==='offline'&&sStart){const dur=new Date(ev.time)-new Date(sStart);sessions.push({agentName:name,loginTime:sStart,logoutTime:ev.time,loginStatus:sSt,durationMs:dur,durationFmt:fmtDur(dur)});sStart=null;}
        else if((ev.status==='away'||ev.status==='lunch')&&sStart){const dur=new Date(ev.time)-new Date(sStart);sessions.push({agentName:name,loginTime:sStart,logoutTime:ev.time,loginStatus:sSt,durationMs:dur,durationFmt:fmtDur(dur)});sStart=ev.time;sSt=ev.status;}
      });
      if(sStart){const dur=new Date()-new Date(sStart);sessions.push({agentName:name,loginTime:sStart,logoutTime:null,loginStatus:sSt,durationMs:dur,durationFmt:fmtDur(dur)+' (active)'});}
    });
    const summary={};
    sessions.forEach(s=>{if(!summary[s.agentName])summary[s.agentName]={name:s.agentName,totalOnlineMs:0,totalBreakMs:0,sessions:0};if(s.loginStatus==='online')summary[s.agentName].totalOnlineMs+=s.durationMs;else summary[s.agentName].totalBreakMs+=s.durationMs;summary[s.agentName].sessions++;});
    return json(res,{logs:filtered,sessions,summary:Object.values(summary).map(s=>({...s,totalOnlineFmt:fmtDur(s.totalOnlineMs),totalBreakFmt:fmtDur(s.totalBreakMs)}))});
  }

  if(pathname==='/api/report/tickets'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const from=parsed.query.from?new Date(parsed.query.from):null;
    const to=parsed.query.to?new Date(parsed.query.to+'T23:59:59'):null;
    const channel=parsed.query.channel||'all';
    const fields=(parsed.query.fields||'').split(',').filter(Boolean);
    const allC=db.all('convs'),msgsR=db.all('messages');
    let rows=allC.filter(c=>{if(channel!=='all'&&c.channel!==channel)return false;if(!from&&!to)return true;const dd=new Date(c.created);return(!from||dd>=from)&&(!to||dd<=to);});
    function fmtDt(iso){if(!iso)return'';try{return new Date(iso).toLocaleString('en-IN',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});}catch{return iso;}}
    function fmtMs(ms){if(!ms||ms<0)return'';const m=Math.round(ms/60000);if(m<60)return m+'m';return Math.floor(m/60)+'h '+m%60+'m';}
    const ALL=['Ticket ID','Customer Name','Email','Channel','Platform','Status','Priority','Subject','Created At','First Response Time','Avg Handling Time','Resolved At','Resolved By','Closed At','Tags','Assigned To','CSAT Score','Reopened','Reopen Time','Reopen By','Total Messages','Bot Messages','Agent Messages'];
    const expF=fields.length?fields:ALL;
    const csvRows=[expF];
    rows.forEach(c=>{
      const cM=msgsR.filter(m=>m.convId===c.id).sort((a,b)=>new Date(a.created)-new Date(b.created));
      const fC=cM.find(m=>m.type==='customer'),fA=cM.find(m=>m.type==='agent'||m.type==='bot');
      const rA=(c.activity||[]).find(a=>a.type==='resolved'),reA=(c.activity||[]).find(a=>a.type==='reopened');
      const frt=fC&&fA?new Date(fA.created)-new Date(fC.created):null;
      const aht=rA&&fC?new Date(rA.time)-new Date(fC.created):null;
      const map={'Ticket ID':c.ticketId||c.id,'Customer Name':c.name||'','Email':c.email||'','Channel':c.channel||'','Platform':c.platform||c.channel||'','Status':c.status||'','Priority':c.priority||'normal','Subject':c.subject||'','Created At':fmtDt(c.created),'First Response Time':fmtMs(frt),'Avg Handling Time':fmtMs(aht),'Resolved At':rA?fmtDt(rA.time):'','Resolved By':rA?rA.agent||'':'','Closed At':rA?fmtDt(rA.time):'','Tags':(c.tags||[]).join(', '),'Assigned To':c.agent||'Unassigned','CSAT Score':c.csat||'','Reopened':reA?'Yes':'No','Reopen Time':reA?fmtDt(reA.time):'','Reopen By':reA?reA.agent||'':'','Total Messages':cM.filter(m=>m.type!=='system').length,'Bot Messages':cM.filter(m=>m.type==='bot').length,'Agent Messages':cM.filter(m=>m.type==='agent').length,'System Events':cM.filter(m=>m.type==='system').map(m=>m.content).join(' | ')};
      csvRows.push(expF.map(f=>{const v=String(map[f]||'').replace(/"/g,'""');return v.includes(',')||v.includes('"')||v.includes('\n')?'"'+v+'"':v;}));
    });
    const csv=csvRows.map(r=>r.join(',')).join('\n');
    res.writeHead(200,{'Content-Type':'text/csv','Content-Disposition':'attachment; filename="nexus-'+channel+'-'+new Date().toISOString().split('T')[0]+'.csv"'});
    res.end(csv);return;
  }

  if(pathname==='/api/report/agent-activity'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const from=parsed.query.from?new Date(parsed.query.from):null;
    const to=parsed.query.to?new Date(parsed.query.to+'T23:59:59'):null;
    const d=db.get();const logs=(d.agentLogs||[]);
    function fmtDt2(iso){if(!iso)return'';try{return new Date(iso).toLocaleString('en-IN',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});}catch{return iso;}}
    function fmtD2(ms){if(!ms||ms<0)return'0m';const m=Math.round(ms/60000);if(m<60)return m+'m';return Math.floor(m/60)+'h '+m%60+'m';}
    let filtered=logs.filter(l=>{if(!from&&!to)return true;const t=new Date(l.time);return(!from||t>=from)&&(!to||t<=to);}).sort((a,b)=>new Date(a.time)-new Date(b.time));
    const byAgent2={};filtered.forEach(l=>{if(!byAgent2[l.agentName])byAgent2[l.agentName]=[];byAgent2[l.agentName].push(l);});
    const csvRows=[['Agent Name','Event','Status','Time','Session Duration']];
    Object.entries(byAgent2).forEach(([name,evts])=>{let prev=null;evts.forEach(ev=>{const dur=prev?new Date(ev.time)-new Date(prev.time):null;csvRows.push([name,ev.status==='offline'?'Logged Out':ev.status==='online'?'Logged In':'Break',ev.status,fmtDt2(ev.time),dur?fmtD2(dur):'']);prev=ev;});});
    const csv=csvRows.map(r=>r.map(v=>{const s=String(v||'').replace(/"/g,'""');return s.includes(',')||s.includes('"')?'"'+s+'"':s;}).join(',')).join('\n');
    res.writeHead(200,{'Content-Type':'text/csv','Content-Disposition':'attachment; filename="nexus-agent-activity-'+new Date().toISOString().split('T')[0]+'.csv"'});
    res.end(csv);return;
  }

  // ATTACHMENT DOWNLOAD
  const attMatch=pathname.match(/^\/api\/attachments\/([^/]+)\/([^/]+)$/);
  if(attMatch&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const msgId=attMatch[1];
    const attIdx=parseInt(attMatch[2])||0;
    const msg=db.find('messages',msgId);
    if(!msg||!msg.attachmentData||!msg.attachmentData[attIdx])
      return json(res,{error:'Not found'},404);
    const att=msg.attachmentData[attIdx];
    const buf=Buffer.from(att.data,'base64');
    res.writeHead(200,{
      'Content-Type':att.mime||'application/octet-stream',
      'Content-Disposition':'attachment; filename="'+att.name+'"',
      'Content-Length':buf.length
    });
    return res.end(buf);
  }

  const gAttMatch=pathname.match(/^\/api\/gmail\/attachment\/([^/]+)\/([^/]+)$/);
  if(gAttMatch&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const gmailMsgId=gAttMatch[1];const attId=gAttMatch[2];
    const token=await getGmailToken();if(!token)return json(res,{error:'No token'},401);
    try{
      const att=await gmailApiGet('/gmail/v1/users/me/messages/'+gmailMsgId+'/attachments/'+attId,token);
      if(!att.data)return json(res,{error:'No data'},404);
      const buf=Buffer.from(att.data.replace(/-/g,'+').replace(/_/g,'/'),'base64');
      res.writeHead(200,{'Content-Type':'application/octet-stream','Content-Disposition':'attachment','Content-Length':buf.length});
      return res.end(buf);
    }catch(e){return json(res,{error:e.message},500);}
  }
  // CSAT public survey page
  const csatSubmit = pathname.match(/^\/csat\/([^/]+)\/([0-9]+)$/);
  if(csatSubmit && method==='GET') {
    const convId = csatSubmit[1], score = parseInt(csatSubmit[2]);
    const conv = db.find('convs', convId);
    if(conv) {
      db.update('convs', convId, { csat: score, csat_at: now(), updated: now() });
      db.addActivity(convId, { type:'csat', text:`CSAT rating: ${score}`, agent:'Customer' });
      broadcast('conv_update', db.find('convs', convId));
    }
    res.writeHead(200,{'Content-Type':'text/html'});
    res.end(`<!DOCTYPE html><html><head><meta charset="UTF-8"/><title>Thank you!</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#09090b;color:#e4e4e7}.card{text-align:center;padding:40px;background:#18181b;border-radius:16px;border:1px solid #27272a}.emoji{font-size:64px;margin-bottom:16px}.title{font-size:22px;font-weight:700;margin-bottom:8px}.sub{font-size:14px;color:#a1a1aa}</style></head><body><div class="card"><div class="emoji">${score>=4?'🎉':score===3?'👍':'😊'}</div><div class="title">Thank you for your feedback!</div><div class="sub">Your rating of ${score}/5 has been recorded for ticket ${conv?.ticketId||convId}.</div></div></body></html>`);
    return;
  }

  // Saved filters API
  if(pathname==='/api/saved-filters' && method==='GET'){const sess=requireAuth(req,res);if(!sess)return;const cfg=getCfg();return json(res,cfg.saved_filters||[]);}
  if(pathname==='/api/saved-filters' && method==='POST'){const sess=requireAuth(req,res);if(!sess)return;const cfg=getCfg();const filters=cfg.saved_filters||[];const{name,filters:f}=body;if(!name||!f)return json(res,{error:'Name and filters required'},400);const newF={id:genId(),name,filters:f,created:now()};filters.push(newF);cfg.saved_filters=filters;setCfg(cfg);return json(res,newF);}
  const sfMatch=pathname.match(/^\/api\/saved-filters\/([^/]+)$/);
  if(sfMatch&&method==='DELETE'){const sess=requireAuth(req,res);if(!sess)return;const cfg=getCfg();cfg.saved_filters=(cfg.saved_filters||[]).filter(f=>f.id!==sfMatch[1]);setCfg(cfg);return json(res,{ok:true});}


  // ── CHANGE OWN PASSWORD ────────────────────────────────────────────────
  if(pathname==='/api/me/password'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);if(!u)return json(res,{error:'User not found'},404);
    const{current,newPass}=body;
    if(!current||!newPass)return json(res,{error:'Both fields required'},400);
    if(u.password!==hash(current))return json(res,{error:'Current password is incorrect'},401);
    if(newPass.length<8)return json(res,{error:'Password must be at least 8 characters'},400);
    db.update('users',sess.uid,{password:hash(newPass),updated:now()});
    return json(res,{ok:true});
  }

  // ── INVITE USER ────────────────────────────────────────────────────────
  if(pathname==='/api/invite'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const u=db.find('users',sess.uid);
    if(!u||u.role!=='admin')return json(res,{error:'Admin only'},403);
    const{email,name,role,channel,visibility}=body;
    if(!email||!name)return json(res,{error:'Email and name required'},400);
    const existingUser = db.findBy('users','email',email.toLowerCase());
    if(existingUser) {
      return json(res,{error:'User already exists: ' + email + ' — delete them from User Settings first, then re-invite.'},400);
    }

    // Create invite token (48hr expiry)
    const token=crypto.randomBytes(32).toString('hex');
    const expires=new Date(Date.now()+48*3600*1000).toISOString();
    const d2=db.get();
    if(!d2.invites)d2.invites=[];
    // Remove any existing invite for this email
    d2.invites=d2.invites.filter(i=>i.email!==email.toLowerCase());
    d2.invites.push({token,email:email.toLowerCase(),name,role:role||'agent',channel:channel||'both',visibility:visibility||'all',expires,created:now()});
    writeDB(d2);

    // Send invitation email
    // Invite links ALWAYS point to Railway (the shared server everyone can access)
    // localhost is only accessible on your machine — never use it for invite links
    const inviteBaseUrl = 'process.env.BASE_URL || 'http://localhost:3000'';
    const inviteLink=`${inviteBaseUrl}/accept-invite?token=${token}`;
    const emailBody=`<div style="font-family:sans-serif;max-width:520px;margin:auto;padding:24px;background:#09090b;color:#e4e4e7;border-radius:12px">
<div style="display:flex;align-items:center;gap:10px;margin-bottom:24px">
  <img src="${inviteBaseUrl}/logo.png" width="36" height="36" style="border-radius:8px;object-fit:contain;vertical-align:middle" alt="U"/>
  <span style="font-size:16px;font-weight:700">Nexus CX</span>
</div>
<h2 style="font-size:20px;font-weight:700;margin-bottom:8px;color:#fff">You're invited to Nexus</h2>
<p style="color:#a1a1aa;margin-bottom:20px;line-height:1.6">Hi ${name},<br/><br/>
<b style="color:#e4e4e7">${u.name||'An admin'}</b> has invited you to join <b style="color:#e4e4e7">Nexus CX</b> as a <b style="color:#00e5b4">${role||'agent'}</b>.</p>
<a href="${inviteLink}" style="display:inline-block;padding:14px 28px;background:#00e5b4;color:#000;text-decoration:none;border-radius:10px;font-weight:700;font-size:14px">Accept Invitation →</a>
<p style="color:#71717a;font-size:12px;margin-top:20px">This link expires in 48 hours. If you did not expect this invitation, you can ignore this email.</p>
<hr style="border:none;border-top:1px solid #27272a;margin:20px 0"/>
<p style="color:#52525b;font-size:11px">Nexus CX · CX Support Platform</p>
</div>`;

    try{
      // Try OAuth first, fall back to SMTP App Password
      const cfg2=getCfg();
      if(cfg2.gmail_refresh_token){
        await sendViaGmailOAuth(email,`You're invited to Nexus CX`,emailBody);
      } else if(cfg2.gmail_user&&cfg2.gmail_pass){
        await sendGmail(email,`You're invited to Nexus CX`,emailBody);
      } else {
        throw new Error('Gmail not configured — connect Gmail in Workspace settings');
      }
      console.log('[Invite] Email sent to',email,'link:',inviteLink);
      return json(res,{ok:true,inviteLink});
    }catch(e){
      console.error('[Invite] Email failed:',e.message);
      return json(res,{ok:true,inviteLink,warning:'Email could not be sent (' + e.message + '). Share this link manually: ' + inviteLink});
    }
  }

  // ── ACCEPT INVITE PAGE ────────────────────────────────────────────────
  if(pathname==='/accept-invite'&&method==='GET'){
    const token=parsed.query.token;
    if(!token){res.writeHead(302,{Location:'/'});res.end();return;}
    const d2=db.get();
    const invite=(d2.invites||[]).find(i=>i.token===token);
    if(!invite){
      res.writeHead(200,{'Content-Type':'text/html'});
      res.end('<!DOCTYPE html><html><body style="font-family:sans-serif;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><div style="font-size:48px;margin-bottom:16px">❌</div><h2>Invalid or expired invitation</h2><p style="color:#71717a;margin-top:8px">This link has expired or already been used.</p><a href="/" style="display:inline-block;margin-top:20px;color:#00e5b4">← Back to login</a></div></body></html>');
      return;
    }
    if(new Date(invite.expires)<new Date()){
      res.writeHead(200,{'Content-Type':'text/html'});
      res.end('<!DOCTYPE html><html><body style="font-family:sans-serif;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;height:100vh;margin:0"><div style="text-align:center"><div style="font-size:48px;margin-bottom:16px">⏰</div><h2>Invitation expired</h2><p style="color:#71717a;margin-top:8px">This invitation has expired. Ask your admin to send a new one.</p><a href="/" style="display:inline-block;margin-top:20px;color:#00e5b4">← Back to login</a></div></body></html>');
      return;
    }
    // Serve the set-password page
    res.writeHead(200,{'Content-Type':'text/html'});
    res.end(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Set Password — Nexus</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet"/>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Inter',sans-serif;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}.card{background:#18181b;border:1px solid #27272a;border-radius:16px;padding:36px 32px;width:100%;max-width:380px;position:relative}.card::before{content:'';position:absolute;top:0;left:20px;right:20px;height:1px;background:linear-gradient(90deg,transparent,rgba(0,229,180,.5),transparent)}.logo{display:flex;align-items:center;gap:10px;margin-bottom:28px}.logo-box{width:36px;height:36px;border-radius:8px;background:linear-gradient(135deg,#00e5b4,#4f8ef7);display:flex;align-items:center;justify-content:center;font-weight:800;color:#000;font-size:15px}.logo-name{font-size:14px;font-weight:700}h2{font-size:22px;font-weight:800;margin-bottom:4px;letter-spacing:-.5px}.sub{font-size:12px;color:#71717a;margin-bottom:24px}label{display:block;font-size:10px;font-weight:700;color:#71717a;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px;margin-top:14px}input{width:100%;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:12px 14px;color:#e4e4e7;font-size:13px;font-family:'Inter',sans-serif;outline:none}.input:focus{border-color:rgba(0,229,180,.4)}.btn{width:100%;padding:13px;background:linear-gradient(135deg,#00e5b4,#00c49a);border:none;border-radius:8px;color:#000;font-size:13px;font-weight:700;cursor:pointer;margin-top:18px;font-family:'Inter',sans-serif}.err{display:none;background:rgba(248,113,113,.08);border:1px solid rgba(248,113,113,.2);border-radius:7px;padding:9px 12px;font-size:11px;color:#f87171;margin-top:10px}.welcome{background:rgba(0,229,180,.07);border:1px solid rgba(0,229,180,.2);border-radius:8px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#a1a1aa}.welcome b{color:#00e5b4}</style></head>
<body><div class="card">
<div class="logo"><div class="logo-box">U</div><div class="logo-name">Nexus CX</div></div>
<h2>Set your password</h2>
<p class="sub">Create a password to activate your account</p>
<div class="welcome">Welcome, <b>${invite.name}</b>! You have been invited as a <b>${invite.role}</b>.</div>
<form onsubmit="setPass(event)">
<label>New Password</label>
<input type="password" id="p1" placeholder="Minimum 8 characters" required autocomplete="new-password"/>
<label>Confirm Password</label>
<input type="password" id="p2" placeholder="Type again" required autocomplete="new-password"/>
<div class="err" id="err"></div>
<button type="submit" class="btn" id="btn">Activate Account →</button>
</form>
</div>
<script>
async function setPass(e){
  e.preventDefault();
  var p1=document.getElementById('p1').value,p2=document.getElementById('p2').value,err=document.getElementById('err'),btn=document.getElementById('btn');
  err.style.display='none';
  if(p1.length<8){err.textContent='Password must be at least 8 characters';err.style.display='block';return;}
  if(p1!==p2){err.textContent='Passwords do not match';err.style.display='block';return;}
  btn.textContent='Activating…';btn.disabled=true;
  var r=await fetch('/api/invite/accept',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:'${token}',password:p1})});
  var d=await r.json();
  if(r.ok&&d.ok){btn.textContent='✓ Welcome, '+d.name+'!';btn.style.background='#00e5b4';setTimeout(function(){location.replace('/');},1200);}
  else{err.textContent=d.error||'Failed';err.style.display='block';btn.textContent='Activate Account →';btn.disabled=false;}
}
</script></body></html>`);
    return;
  }

  // ── ACCEPT INVITE (POST — set password) ───────────────────────────────
  if(pathname==='/api/invite/accept'&&method==='POST'){
    const{token,password}=body;
    if(!token||!password)return json(res,{error:'Token and password required'},400);
    if(password.length<8)return json(res,{error:'Password must be at least 8 characters'},400);
    // Read fresh copy of DB
    const d2=db.get();
    const invIdx=(d2.invites||[]).findIndex(i=>i.token===token);
    if(invIdx===-1)return json(res,{error:'Invalid or expired invitation'},400);
    const invite=d2.invites[invIdx];
    if(new Date(invite.expires)<new Date())return json(res,{error:'Invitation has expired'},400);
    if(db.findBy('users','email',invite.email))return json(res,{error:'Account already exists'},400);

    // FIX: Remove invite FIRST on the stale d2, then write, THEN insert user
    // so the user insert (which does its own writeDB) doesn't get overwritten
    d2.invites=d2.invites.filter((_,i)=>i!==invIdx);
    writeDB(d2);

    // Now create the user — db.insert does its own fresh read+write
    const uid=genId();
    const hashedPw=hash((password||'').trim());
    const newUser=db.insert('users',{
      id:uid,name:invite.name,email:invite.email,
      password:hashedPw,role:invite.role||'agent',
      channel:invite.channel||'both',visibility:invite.visibility||'all',
      phone:'',status:'offline',avatar:'',created:now()
    });
    if(!db.findBy('agents','email',invite.email))
      db.insert('agents',{id:genId(),name:invite.name,email:invite.email,role:invite.role||'agent',channel:invite.channel||'both',status:'offline',created:now()});

    console.log('[Invite] User created:', invite.name, invite.email, 'role:', invite.role);

    // Auto-create session
    const newSid=makeSession();
    sessions.get(newSid).uid=newUser.id;
    persistSessions();
    res.setHeader('Set-Cookie',`nsid=${newSid}; HttpOnly; Path=/; SameSite=Lax; Max-Age=86400`);
    return json(res,{ok:true,name:invite.name,autoLogin:true});
  }

  // ── GET PENDING INVITES ────────────────────────────────────────────────
  if(pathname==='/api/invites'&&method==='GET'){
    const sess=requireAuth(req,res);if(!sess)return;
    const d2=db.get();
    const active=(d2.invites||[]).filter(i=>new Date(i.expires)>new Date()).map(i=>({email:i.email,name:i.name,role:i.role,expires:i.expires,created:i.created}));
    return json(res,active);
  }

  // ── RESEND / CANCEL INVITE ─────────────────────────────────────────────
  if(pathname==='/api/invite/cancel'&&method==='POST'){
    const sess=requireAuth(req,res);if(!sess)return;
    const{email}=body;
    const d2=db.get();
    d2.invites=(d2.invites||[]).filter(i=>i.email!==email.toLowerCase());
    writeDB(d2);
    return json(res,{ok:true});
  }

  json(res,{error:'Not found'},404);
});

loadPersistedSessions();
// Ensure admin account always exists
(function ensureAdmin() {
  const d = db.get();
  if (!d.users || d.users.length === 0) {
    console.log('[INIT] No users found - creating default admin');
    db.insert('users', {
      id: genId(), name: 'Admin', email: 'admin@nexus.com',
      password: hash('admin123'), role: 'admin',
      channel: 'both', visibility: 'all',
      phone: '', status: 'offline', avatar: '', created: now()
    });
  }
})();
startGmailAPIPolling();
startSocialPolling();
server.listen(PORT,()=>{
  console.log(`\n🚀  Nexus CX v1 → http://localhost:${PORT}`);
  console.log(`    Login: admin@nexus.com / admin123\n`);
});

// ── SHIFT AUTO-STATUS ──────────────────────────────────────────────────
setInterval(()=>{
  try{
    const users=db.all('users');
    const now2=new Date();
    const dayOfWeek=now2.getDay();
    const currentTime=now2.getHours()*60+now2.getMinutes();
    users.forEach(u=>{
      if(!u.shift||!u.shift.start||!u.shift.end)return;
      const days=u.shift.days||[1,2,3,4,5];
      if(!days.includes(dayOfWeek))return;
      const[sh,sm]=u.shift.start.split(':').map(Number);
      const[eh,em]=u.shift.end.split(':').map(Number);
      const start=sh*60+sm, end=eh*60+em;
      const inShift=currentTime>=start&&currentTime<end;
      const currentStatus=u.status||'offline';
      if(inShift&&currentStatus==='offline'){
        db.update('users',u.id,{status:'online'});
        broadcast('agent_status',{name:u.name,status:'online'});
      } else if(!inShift&&currentStatus==='online'){
        db.update('users',u.id,{status:'offline'});
        broadcast('agent_status',{name:u.name,status:'offline'});
      }
    });
  }catch(e){}
},60000);

// ── SCHEDULED WEEKLY REPORTS ──────────────────────────────────────────────
setInterval(async()=>{
  try{
    const cfg=getCfg();
    if(!cfg.scheduledReport||!cfg.scheduledReport.enabled)return;
    const now2=new Date();
    const dayMatch=now2.getDay()===(cfg.scheduledReport.day||1);
    const[th,tm]=(cfg.scheduledReport.time||'09:00').split(':').map(Number);
    const timeMatch=now2.getHours()===th&&now2.getMinutes()===tm;
    if(!dayMatch||!timeMatch)return;
    // Build report
    const cutoff=Date.now()-604800000;
    const convs=(db.get().convs||[]).filter(c=>new Date(c.updated||c.created).getTime()>cutoff);
    const resolved=convs.filter(c=>c.status==='resolved').length;
    const total=convs.length;
    const csatConvs=convs.filter(c=>c.csat);
    const avgCsat=csatConvs.length?Math.round(csatConvs.reduce((s,c)=>s+(c.csat||0),0)/csatConvs.length*10)/10:0;
    const emailBody=`<div style="font-family:sans-serif;max-width:600px;margin:auto;padding:24px;background:#09090b;color:#e4e4e7;border-radius:12px">
<h2 style="color:#00e5b4">📊 Nexus Weekly Report</h2>
<p style="color:#a1a1aa">Week ending ${now2.toDateString()}</p>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin:20px 0">
<div style="background:#18181b;padding:16px;border-radius:8px;text-align:center"><div style="font-size:28px;font-weight:800;color:#00e5b4">${total}</div><div style="color:#71717a;font-size:12px">Total Tickets</div></div>
<div style="background:#18181b;padding:16px;border-radius:8px;text-align:center"><div style="font-size:28px;font-weight:800;color:#4f8ef7">${resolved}</div><div style="color:#71717a;font-size:12px">Resolved</div></div>
<div style="background:#18181b;padding:16px;border-radius:8px;text-align:center"><div style="font-size:28px;font-weight:800;color:#facc15">${avgCsat||'—'}</div><div style="color:#71717a;font-size:12px">Avg CSAT</div></div>
</div>
<p style="color:#71717a;font-size:12px">Nexus CX · Automated Weekly Report</p></div>`;
    const _srTo=cfg.scheduledReport.email||cfg.gmail_user;
    const _srSubj='📊 Nexus Weekly Report — '+now2.toDateString();
    if(cfg.gmail_refresh_token){
      await sendViaGmailOAuth(_srTo,_srSubj,emailBody);
    } else if(cfg.gmail_user&&cfg.gmail_pass){
      await sendGmail(_srTo,_srSubj,emailBody);
    }
    console.log('[Report] Weekly report sent');
  }catch(e){console.error('[Report] Failed:',e.message);}
},60000);

// ── SLA BREACH CHECKER ───────────────────────────────────────────────────────
function checkSLABreaches() {
  const cfg = getCfg();
  const allConvs = db.all('convs').filter(c => c.status === 'open');
  const msgs = db.all('messages');
  const nowMs = Date.now();

  for (const conv of allConvs) {
    const ch = conv.channel || 'email';
    const slaFirstKey = ch === 'email' ? 'sla_email_first' : ch === 'chat' ? 'sla_chat_first' : 'sla_social_first';
    const slaFirst = parseInt(cfg[slaFirstKey] || 0);
    if (!slaFirst) continue;

    // Check if within business hours
    if (cfg.biz_hours_enabled) {
      const now = new Date();
      const day = now.getDay(); // 0=Sun, 6=Sat
      const bhStart = parseInt(cfg.biz_hours_start || 9);
      const bhEnd = parseInt(cfg.biz_hours_end || 18);
      const bhDays = cfg.biz_hours_days || [1,2,3,4,5];
      const hour = now.getHours();
      if (!bhDays.includes(day) || hour < bhStart || hour >= bhEnd) continue;
    }

    // Has agent replied?
    const convMsgs = msgs.filter(m => m.convId === conv.id).sort((a,b) => new Date(a.created)-new Date(b.created));
    const firstCust = convMsgs.find(m => m.type === 'customer');
    const firstAgent = convMsgs.find(m => m.type === 'agent' || m.type === 'bot');
    if (firstAgent) continue; // Already responded

    if (!firstCust) continue;
    const elapsed = (nowMs - new Date(firstCust.created).getTime()) / 60000; // minutes
    if (elapsed >= slaFirst && !conv.sla_breached) {
      db.update('convs', conv.id, { sla_breached: true, sla_breach_time: new Date().toISOString(), updated: now() });
      db.addActivity(conv.id, { type: 'sla_breach', text: `SLA breach — no response in ${slaFirst}m`, agent: 'System' });
      broadcast('sla_breach', { convId: conv.id, ticketId: conv.ticketId, name: conv.name, channel: ch, minutes: Math.round(elapsed) });
      // Slack notification for SLA breach
      const slaCfg=getCfg();
      if(slaCfg.slack_webhook_url){
        fetch(slaCfg.slack_webhook_url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:`🚨 *SLA Breach* — Ticket ${conv.ticketId} (${conv.name}) has breached response time target. Channel: ${conv.channel}. <${slaCfg.slack_webhook_url}|View ticket>`})}).catch(e=>console.log('[Slack SLA]',e.message));
      }
    }
  }
}
setInterval(checkSLABreaches, 60000); // Run every minute

// ── AUTO-CLOSE CHECKER ───────────────────────────────────────────────────────
function checkAutoClose() {
  const cfg = getCfg();
  const allConvs = db.all('convs').filter(c => c.status === 'open' || c.status === 'waiting');
  const msgs = db.all('messages');
  const nowMs = Date.now();

  for (const conv of allConvs) {
    const ch = conv.channel || 'email';
    const acKey = ch === 'email' ? 'autoclose_email' : ch === 'chat' ? 'autoclose_chat' : null;
    if (!acKey) continue;
    const acDays = parseInt(cfg[acKey] || 0);
    if (!acDays) continue;

    const convMsgs = msgs.filter(m => m.convId === conv.id).sort((a,b) => new Date(b.created)-new Date(a.created));
    const lastMsg = convMsgs[0];
    if (!lastMsg) continue;

    const daysSince = (nowMs - new Date(lastMsg.created).getTime()) / 86400000;
    if (daysSince >= acDays) {
      db.update('convs', conv.id, { status: 'resolved', updated: now() });
      db.addActivity(conv.id, { type: 'resolved', text: `Auto-closed after ${acDays} days of inactivity`, agent: 'System' });
      broadcast('conv_update', db.find('convs', conv.id));
    }
  }
}
setInterval(checkAutoClose, 3600000); // Run hourly

// ── CSAT ENDPOINTS ────────────────────────────────────────────────────────────
// Already handled in main request handler below — adding CSAT send helper
async function sendCSATEmail(conv) {
  const cfg = getCfg();
  if (!cfg.csat_enabled) return;
  if (!conv.email || conv.channel !== 'email') return;
  const ratingType = cfg.csat_rating_type || 'stars';
  const question = cfg.csat_question || 'How would you rate your support experience?';
  const baseUrl = process.env.RAILWAY_STATIC_URL
    ? 'process.env.BASE_URL || 'http://localhost:3000''
    : 'http://localhost:3000';
  let ratingHtml = '';
  if (ratingType === 'stars') {
    ratingHtml = [1,2,3,4,5].map(n =>
      `<a href="${baseUrl}/csat/${conv.id}/${n}" style="display:inline-block;margin:0 4px;text-decoration:none;font-size:28px">${n <= 3 ? '⭐' : '⭐'}</a>`
    ).join('');
  } else if (ratingType === 'numbers') {
    ratingHtml = [1,2,3,4,5,6,7,8,9,10].map(n =>
      `<a href="${baseUrl}/csat/${conv.id}/${n}" style="display:inline-block;margin:2px;padding:8px 12px;background:#f4f4f5;border-radius:6px;text-decoration:none;font-size:14px;font-weight:700;color:#18181b">${n}</a>`
    ).join('');
  } else if (ratingType === 'emoji') {
    const emojis = ['😞','😕','😐','😊','😍'];
    ratingHtml = emojis.map((em,i) =>
      `<a href="${baseUrl}/csat/${conv.id}/${i+1}" style="display:inline-block;margin:0 6px;text-decoration:none;font-size:32px">${em}</a>`
    ).join('');
  }
  const mailBody = `<div style="font-family:sans-serif;max-width:520px;margin:auto;padding:24px">
<h2 style="color:#18181b;font-size:18px">${question}</h2>
<p style="color:#52525b;font-size:14px">Ticket: <b>${conv.ticketId}</b> — ${conv.subject || conv.name}</p>
<div style="margin:24px 0;text-align:center">${ratingHtml}</div>
<p style="color:#a1a1aa;font-size:12px">Click a rating above to submit. Thank you for your feedback!</p>
</div>`;
  try { await sendGmail(conv.email, `How did we do? — ${conv.ticketId}`, mailBody); } catch(e) { console.log('[CSAT] Send failed:', e.message); }
}

