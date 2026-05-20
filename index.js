require('dotenv').config();
const express=require('express'),cors=require('cors'),axios=require('axios'),nodemailer=require('nodemailer'),Imap=require('imap'),{simpleParser}=require('mailparser'),cron=require('node-cron'),path=require('path'),fs=require('fs'),multer=require('multer'),csvParser=require('csv-parser');
// ── Turso via HTTP REST API (no SDK — avoids migration system bug) ────────
const TURSO_URL = (process.env.TURSO_URL||'').replace(/^libsql:\/\//,'https://');
const TURSO_TOKEN = process.env.TURSO_TOKEN||'';

async function turso(sql, args=[]){
  if(!TURSO_URL||!TURSO_TOKEN) throw new Error('TURSO_URL or TURSO_TOKEN not set');
  const url = TURSO_URL.endsWith('/')?TURSO_URL.slice(0,-1):TURSO_URL;
  try{
    const r = await axios.post(`${url}/v2/pipeline`, {
      requests:[
        {type:'execute',stmt:{sql, args: args.map(v=>{
          if(v===null||v===undefined) return {type:'null'};
          if(typeof v==='number'||Number.isInteger(Number(v))) return {type:'integer',value:String(Math.floor(Number(v)))};
          return {type:'text',value:String(v)};
        })}},
        {type:'close'}
      ]
    },{
      headers:{Authorization:`Bearer ${TURSO_TOKEN}`,'Content-Type':'application/json'},
      timeout:15000
    });
    const result = r.data?.results?.[0];
    if(result?.type==='error') throw new Error(result.error?.message||'Turso query error');
    const resp = result?.response?.result;
    const cols = (resp?.cols||[]).map(c=>c.name);
    const rows = (resp?.rows||[]).map(row=>{
      const obj={};
      cols.forEach((c,i)=>{const v=row[i];obj[c]=v?.type==='null'?null:v?.value??null;});
      return obj;
    });
    return{rows, lastInsertRowid: resp?.last_insert_rowid?Number(resp.last_insert_rowid):null};
  }catch(e){
    // Extract clean error message — avoid dumping raw socket objects
    const msg = e.response?.data?.message || e.response?.data?.error || e.response?.statusText || e.code || e.message || 'Unknown error';
    const status = e.response?.status||'no response';
    throw new Error(`Turso HTTP ${status}: ${msg}`);
  }
}

const db={
  execute: async(sqlOrObj, args=[])=>{
    if(typeof sqlOrObj==='object'&&sqlOrObj.sql) return turso(sqlOrObj.sql, sqlOrObj.args||[]);
    return turso(sqlOrObj, args);
  }
};

async function initDB(){
  await turso(`CREATE TABLE IF NOT EXISTS leads(id INTEGER PRIMARY KEY AUTOINCREMENT,company TEXT,website TEXT,industry TEXT,size TEXT,location TEXT,source TEXT DEFAULT 'manual',notes TEXT,status TEXT DEFAULT 'new',created_at TEXT,updated_at TEXT)`);
  await turso(`CREATE TABLE IF NOT EXISTS contacts(id INTEGER PRIMARY KEY AUTOINCREMENT,lead_id INTEGER,name TEXT DEFAULT 'Unknown',first_name TEXT DEFAULT 'there',role TEXT,email TEXT,linkedin TEXT,status TEXT DEFAULT 'new',emails_sent INTEGER DEFAULT 0,last_sent TEXT,replied_at TEXT,created_at TEXT)`);
  await turso(`CREATE TABLE IF NOT EXISTS emails(id INTEGER PRIMARY KEY AUTOINCREMENT,contact_id INTEGER,lead_id INTEGER,direction TEXT,subject TEXT,body TEXT,from_addr TEXT,to_addr TEXT,template_num INTEGER,message_id TEXT,sent_at TEXT)`);
  await turso(`CREATE TABLE IF NOT EXISTS activity_log(id INTEGER PRIMARY KEY AUTOINCREMENT,icon TEXT,title TEXT,detail TEXT,created_at TEXT)`);
  await turso(`CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT)`);
  console.log('✅ Turso DB connected and ready');
}

// ── KV helpers (for Serper daily tracking) ───────────────────────────────
async function kvGet(key){try{const r=await db.execute({sql:'SELECT value FROM kv WHERE key=?',args:[key]});return r.rows[0]?JSON.parse(r.rows[0].value):null;}catch(e){return null;}}
async function kvSet(key,val){await db.execute({sql:'INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)',args:[key,JSON.stringify(val)]});}

// ── DB functions (all async now) ─────────────────────────────────────────
async function getAllLeads(){
  const[leads,contacts]=await Promise.all([
    db.execute('SELECT * FROM leads ORDER BY id DESC'),
    db.execute('SELECT lead_id,status,emails_sent FROM contacts')
  ]);
  return leads.rows.map(l=>{
    const cs=contacts.rows.filter(c=>Number(c.lead_id)===Number(l.id));
    // Parse score from notes field: "Score: 8/10"
    const scoreMatch=(l.notes||'').match(/Score:\s*(\d+)\/10/);
    const score=scoreMatch?Number(scoreMatch[1]):null;
    return{...l,id:Number(l.id),contact_count:cs.length,
      total_sent:cs.reduce((a,x)=>a+(Number(x.emails_sent)||0),0),
      has_reply:cs.some(x=>x.status==='replied')?1:0,
      score};
  });
}
async function getLeadById(id){const r=await db.execute({sql:'SELECT * FROM leads WHERE id=?',args:[Number(id)]});const l=r.rows[0];return l?{...l,id:Number(l.id)}:null;}
async function insertLead(d){const r=await db.execute({sql:'INSERT INTO leads(company,website,industry,size,location,source,notes,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',args:[d.company||'',d.website||null,d.industry||null,d.size||null,d.location||null,d.source||'manual',d.notes||null,'new',new Date().toISOString(),new Date().toISOString()]});return{lastInsertRowid:Number(r.lastInsertRowid)};}
async function getContactsByLeadId(lid){const r=await db.execute({sql:'SELECT * FROM contacts WHERE lead_id=?',args:[Number(lid)]});return r.rows.map(c=>({...c,id:Number(c.id),lead_id:Number(c.lead_id),emails_sent:Number(c.emails_sent)||0}));}
async function getContactById(id){const r=await db.execute({sql:'SELECT * FROM contacts WHERE id=?',args:[Number(id)]});const c=r.rows[0];return c?{...c,id:Number(c.id),lead_id:Number(c.lead_id),emails_sent:Number(c.emails_sent)||0}:null;}
async function insertContact(d){const r=await db.execute({sql:'INSERT INTO contacts(lead_id,name,first_name,role,email,linkedin,status,emails_sent,created_at) VALUES(?,?,?,?,?,?,?,?,?)',args:[Number(d.lead_id),d.name||'Unknown',d.first_name||'there',d.role||'',d.email||null,d.linkedin||null,'new',0,new Date().toISOString()]});return{lastInsertRowid:Number(r.lastInsertRowid)};}
async function updateContactEmail(id,email){await db.execute({sql:'UPDATE contacts SET email=? WHERE id=?',args:[email,Number(id)]});}
async function markContactSent(id){await db.execute({sql:"UPDATE contacts SET emails_sent=emails_sent+1,last_sent=?,status=CASE WHEN status='new' THEN 'sent' ELSE 'followup' END WHERE id=?",args:[new Date().toISOString(),Number(id)]});}
async function markContactReplied(id){await db.execute({sql:"UPDATE contacts SET status='replied',replied_at=? WHERE id=?",args:[new Date().toISOString(),Number(id)]});}
async function getContactsDueForFollowup(){
  const days=(process.env.FOLLOWUP_DAYS||'3,7,14').split(',').map(Number),min=Math.min(...days);
  const r=await db.execute("SELECT c.*,l.company FROM contacts c JOIN leads l ON l.id=c.lead_id WHERE c.status IN('sent','followup') AND c.email IS NOT NULL AND c.last_sent IS NOT NULL");
  return r.rows.filter(c=>Number(c.emails_sent)<days.length+1&&(Date.now()-new Date(c.last_sent).getTime())/86400000>=min).map(c=>({...c,id:Number(c.id),lead_id:Number(c.lead_id),emails_sent:Number(c.emails_sent)||0}));
}
async function getContactsNotYetEmailed(){
  const max=parseInt(process.env.MAX_EMAILS_PER_DAY)||30;
  const r=await db.execute({sql:"SELECT c.*,l.company FROM contacts c JOIN leads l ON l.id=c.lead_id WHERE c.status='new' AND c.email IS NOT NULL LIMIT ?",args:[max]});
  return r.rows.map(c=>({...c,id:Number(c.id),lead_id:Number(c.lead_id),emails_sent:Number(c.emails_sent)||0}));
}
async function getEmailsByLeadId(lid){
  const r=await db.execute({sql:'SELECT e.*,c.name as contact_name,c.role as contact_role FROM emails e LEFT JOIN contacts c ON c.id=e.contact_id WHERE e.lead_id=?',args:[Number(lid)]});
  return r.rows.map(e=>({...e,id:Number(e.id)}));
}
async function insertEmail(d){await db.execute({sql:'INSERT INTO emails(contact_id,lead_id,direction,subject,body,from_addr,to_addr,template_num,message_id,sent_at) VALUES(?,?,?,?,?,?,?,?,?,?)',args:[Number(d.contact_id),Number(d.lead_id),d.direction,d.subject||'',d.body||'',d.from_addr||'',d.to_addr||'',d.template_num||null,d.message_id||null,new Date().toISOString()]});}
async function dbLog(icon,title,detail){console.log(`[${icon}] ${title}${detail?' — '+detail:''}`);try{await db.execute({sql:'INSERT INTO activity_log(icon,title,detail,created_at) VALUES(?,?,?,?)',args:[icon,title,detail||'',new Date().toISOString()]});}catch(e){}}
async function getRecentActivity(limit=50){const r=await db.execute({sql:'SELECT * FROM activity_log ORDER BY id DESC LIMIT ?',args:[limit]});return r.rows;}
async function getStats(){
  const[l,c,ef,ct,ts,fu,tr,eo]=await Promise.all([
    db.execute('SELECT COUNT(*) as n FROM leads'),
    db.execute('SELECT COUNT(*) as n FROM contacts'),
    db.execute('SELECT COUNT(*) as n FROM contacts WHERE email IS NOT NULL'),
    db.execute('SELECT COUNT(*) as n FROM contacts WHERE emails_sent>0'),
    db.execute('SELECT COALESCE(SUM(emails_sent),0) as n FROM contacts'),
    db.execute("SELECT COUNT(*) as n FROM contacts WHERE status='followup'"),
    db.execute("SELECT COUNT(*) as n FROM contacts WHERE status='replied'"),
    db.execute("SELECT COUNT(*) as n FROM emails WHERE direction='out'"),
  ]);
  return{total_leads:Number(l.rows[0].n),total_contacts:Number(c.rows[0].n),emails_found:Number(ef.rows[0].n),contacted:Number(ct.rows[0].n),total_sent:Number(ts.rows[0].n),followups_due:Number(fu.rows[0].n),total_replies:Number(tr.rows[0].n),emails_out:Number(eo.rows[0].n)};
}
async function leadExists(company){const r=await db.execute({sql:'SELECT id FROM leads WHERE LOWER(TRIM(company))=LOWER(TRIM(?))',args:[company]});return r.rows.length>0;}

// ── Serper daily guard (stored in Turso kv table) ─────────────────────────
// Serper — no limits, runs all queries every time

// ── Email templates ───────────────────────────────────────────────────────
const TPLS={
  1:{sub:'IT Outsourcing Services for {{company_name}}?',body:'Hi {{first_name}},\n\nI noticed {{company_name}} is looking for IT support — we can help.\n\nByteOn Technologies provides IT outsourcing services including:\n\u2022 Software Development (React, Node.js, Python, Java, Mobile)\n\u2022 QA & Testing | DevOps & Cloud (AWS, Azure, GCP)\n\u2022 IT Support & Managed Services | Data Engineering & AI\n\u2022 Cybersecurity | Project Management\n\nAll resources are pre-vetted, ready in 5-7 business days, 40-60% less than full-time hiring.\n\nWould you be open to a quick 15-min call this week?\n\nBest,\n{{your_name}}\n{{your_company}}\n{{your_phone}} | {{your_website}}'},
  2:{sub:'Re: IT Outsourcing for {{company_name}}',body:'Hi {{first_name}},\n\nFollowing up on my previous note.\n\nWe recently helped a US company cut IT costs by 50% by switching to our outsourcing model — same quality, fraction of the cost, zero hiring overhead.\n\nWhether you need 1 resource or a full team, ready in under a week.\n\nWorth a 15-min call?\n\n{{your_name}}\n{{your_company}}'},
  3:{sub:'One more thought — {{company_name}}',body:"Hi {{first_name}},\n\nThe biggest advantage our clients mention is flexibility — scale up when you need more capacity, scale down when you don't. No layoffs, no notice periods.\n\nHappy to send a one-pager on our services and pricing.\n\n{{your_name}}\n{{your_company}}"},
  4:{sub:'Closing the loop — {{company_name}}',body:"Hi {{first_name}},\n\nI'll leave it here — if IT outsourcing needs come up in the future, feel free to reach out.\n\nWishing {{company_name}} all the best.\n\n{{your_name}}\n{{your_company}}"}
}
function fillTpl(t,v){let s=t.sub,b=t.body;for(const[k,val]of Object.entries(v)){s=s.replaceAll(`{{${k}}}`,val||'');b=b.replaceAll(`{{${k}}}`,val||'');}return{subject:s,body:b};}

// ── Hostinger SMTP ───────────────────────────────────────────────────────
// Railway blocks port 587 on some plans — we try 465 first, fall back to 587
function makeTransport(forcePort){
  const port = forcePort || parseInt(process.env.SMTP_PORT)||465;
  const host  = process.env.SMTP_HOST||'smtp.hostinger.com';
  return require('nodemailer').createTransport({
    host,
    port,
    secure: port===465,           // 465=SSL, 587=STARTTLS
    requireTLS: port===587,
    auth:{ user:process.env.SMTP_USER, pass:process.env.SMTP_PASS },
    connectionTimeout:20000, greetingTimeout:20000, socketTimeout:20000,
    tls:{ rejectUnauthorized:false, minVersion:'TLSv1' }
  });
}

async function testSmtp(){
  // Try 465 first, then 587, then report both errors
  const ports = [465, 587];
  for(const port of ports){
    try{
      await makeTransport(port).verify();
      await dbLog('✅',`Hostinger SMTP connected (port ${port})`,process.env.SMTP_USER);
      // Save working port back to env for this session
      process.env.SMTP_PORT = String(port);
      return{ok:true, email:process.env.SMTP_USER, port};
    }catch(e){
      await dbLog('⚠️',`SMTP port ${port} failed`,e.message);
    }
  }
  return{ok:false, error:'Both port 465 and 587 timed out. Check SMTP_USER and SMTP_PASS in Railway variables, or try SMTP_HOST=mail.hostinger.com'};
}

async function sendEmail({contact,lead,emailNum=1,dryRun=false}){
  const t=TPLS[emailNum]||TPLS[1];
  const{subject,body}=fillTpl(t,{
    first_name:contact.first_name||contact.name?.split(' ')[0]||'there',
    company_name:lead.company, role:contact.role,
    your_name:process.env.SMTP_FROM_NAME||'Your Name',
    your_company:process.env.YOUR_COMPANY||'',
    your_phone:process.env.YOUR_PHONE||'',
    your_website:process.env.YOUR_WEBSITE||''
  });
  if(dryRun){await dbLog('👁','DRY RUN',`To:${contact.email}|${subject}`);return{ok:true,dryRun:true,subject,body};}
  try{
    const info = await makeTransport().sendMail({
      from:`"${process.env.SMTP_FROM_NAME}"<${process.env.SMTP_USER}>`,
      to:contact.email, subject, text:body
    });
    insertEmail({contact_id:contact.id,lead_id:lead.id||contact.lead_id,direction:'out',subject,body,from_addr:process.env.SMTP_USER,to_addr:contact.email,template_num:emailNum,message_id:info.messageId});
    markContactSent(contact.id);
    await dbLog('📤',`Email #${emailNum} sent`,`${lead.company}→${contact.name}`);
    return{ok:true,messageId:info.messageId};
  }catch(e){
    await dbLog('❌','Send failed',e.message);
    return{ok:false,error:e.message};
  }
}

async function sendNotif({contactName,companyName,replyBody}){
  if(!process.env.NOTIFY_EMAIL) return;
  try{
    await makeTransport().sendMail({
      from:`"LeadForge"<${process.env.SMTP_USER}>`,
      to:process.env.NOTIFY_EMAIL,
      subject:`Reply from ${companyName} — ${contactName}`,
      text:`New reply!\n\nCompany: ${companyName}\nContact: ${contactName}\n\n${replyBody||''}`
    });
  }catch(e){}
}

function delay(s){return new Promise(r=>setTimeout(r,Math.max(5000,(s+(Math.random()-.5)*60)*1000)));}

// ── Hunter.io email finder ─────────────────────────────────────────────────
async function findEmailHunter(firstName, lastName, domain){
  const key = process.env.HUNTER_API_KEY;
  if(!key) return null;
  // Skip if no real name — Hunter needs first+last name to find specific person
  if(!firstName || firstName.toLowerCase()==='unknown' || firstName.toLowerCase()==='there') return null;
  try {
    const r = await axios.get('https://api.hunter.io/v2/email-finder', {
      params: { first_name:firstName, last_name:lastName, domain, api_key:key },
      timeout: 10000
    });
    return r.data?.data?.email || null;
  } catch(e) { return null; }
}
async function findEmailsByDomain(domain){
  const key = process.env.HUNTER_API_KEY;
  if(!key) return [];
  try {
    const r = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key:key, limit:5 },
      timeout: 10000
    });
    const emails = r.data?.data?.emails || [];
    const err = r.data?.errors?.[0]?.details || r.data?.errors?.[0]?.id || '';
    if(err) await dbLog('⚠️','Hunter domain error',`${domain}: ${err}`);
    return emails;
  } catch(e) {
    await dbLog('⚠️','Hunter domain failed',`${domain}: ${e.response?.data?.errors?.[0]?.details||e.message}`);
    return [];
  }
}
async function findCompanyWebsite(companyName){
  const key = process.env.SERPER_API_KEY;
  if(!key) return null;
  try{
    const r = await axios.post('https://google.serper.dev/search',
      { q:`${companyName} official website`, num:1, gl:'us' },
      { headers:{ 'X-API-KEY':key, 'Content-Type':'application/json' }, timeout:10000 }
    );
    const link = r.data?.organic?.[0]?.link||'';
    if(!link) return null;
    const domain = link.replace(/^https?:\/\//,'').replace(/\/.*/,'').replace(/^www\./,'');
    // Skip generic sites
    if(['linkedin','indeed','glassdoor','crunchbase','bloomberg','forbes','wikipedia'].some(s=>domain.includes(s))) return null;
    return domain;
  }catch(e){ return null; }
}

async function enrichContactsWithHunter(){
  const key = process.env.HUNTER_API_KEY;
  if(!key){ await dbLog('⚠️','Hunter skipped','HUNTER_API_KEY not set'); return 0; }

  const contacts = (await db.execute("SELECT c.*,l.website,l.company as lead_company FROM contacts c JOIN leads l ON l.id=c.lead_id WHERE c.email IS NULL")).rows.map(c=>({...c,id:Number(c.id),lead_id:Number(c.lead_id),emails_sent:Number(c.emails_sent)||0}));
  let found = 0;
  let searched = 0;
  const HUNTER_LIMIT = 5; // free plan 25/month — use 5 per manual run

  // Job board domains — website field set to job post URL, not company domain
  const JOB_BOARDS = ['lever.co','greenhouse.io','workable.com','ashbyhq.com','wellfound.com',
    'indeed.com','linkedin.com','glassdoor.com','remoteok.com','jobicy.com',
    'remotive.com','weworkremotely.com','breezy.hr','bamboohr.com','smartrecruiters.com'];

  for(const c of contacts){
    if(searched >= HUNTER_LIMIT) break;
    const lead = await getLeadById(c.lead_id);
    if(!lead) continue;

    // Extract real company domain — skip job board URLs
    let domain = '';
    if(lead.website){
      const raw = lead.website.replace(/^https?:\/\//,'').replace(/\/.*/,'').replace(/^www\./,'');
      const isJobBoard = JOB_BOARDS.some(jb=>raw.includes(jb));
      if(!isJobBoard) domain = raw;
    }
    if(!domain){
      // Guess from company name: "Stripe Inc" → "stripe.com"
      const guessed = lead.company.toLowerCase()
        .replace(/[^a-z0-9 ]/g,'')
        .replace(/\s+(inc|llc|corp|ltd|co|technologies|tech|solutions|services|group)$/,'')
        .trim().replace(/\s+/g,'')+'.com';
      if(process.env.SERPER_API_KEY){
        const verified = await findCompanyWebsite(lead.company);
        domain = verified || guessed;
        if(verified) await db.execute({sql:'UPDATE leads SET website=? WHERE id=?',args:['https://'+verified,lead.id]});
      } else {
        domain = guessed;
      }
    }
    if(!domain) continue;
    await dbLog('🔍','Hunter trying',`${lead.company} — ${domain}`);

    searched++;
    const nameParts = (c.name||'').split(' ');
    const firstName = c.first_name||nameParts[0]||'';
    const lastName  = nameParts[1]||'';

    // Try 1: find specific person's email
    let email = await findEmailHunter(firstName, lastName, domain);

    // Try 2: always try domain search as fallback (works even when contact is "Unknown")
    if(!email){
      const domainEmails = await findEmailsByDomain(domain);
      if(domainEmails.length>0){
        email = domainEmails[0].value;
        await dbLog('🔍','Hunter domain fallback',`${lead.company} → ${email}`);
      }
    }

    if(email){
      await updateContactEmail(c.id, email);
      found++;
      await dbLog('🎯','Hunter email found',`${c.name} at ${lead.company} → ${email}`);
    } else {
      await dbLog('🔍','Hunter no result',`${lead.company} (${domain})`);
    }
    await new Promise(r=>setTimeout(r,1200));
  }

  await dbLog('🔍','Hunter enrichment done',`${found} emails found from ${searched} searches`);
  return found;
}

// ── CSV Import (from Apollo export) ───────────────────────────────────────
const upload = multer({ storage: multer.memoryStorage(), limits:{ fileSize: 10*1024*1024 } });

function parseApolloCSV(buffer){
  return new Promise((resolve, reject) => {
    const results = [];
    const stream = require('stream');
    const readable = new stream.Readable();
    readable.push(buffer);
    readable.push(null);
    readable.pipe(csvParser())
      .on('data', row => results.push(row))
      .on('end', () => resolve(results))
      .on('error', reject);
  });
}

function detectAndNormalizeRow(row){
  // ── Waalaxy export format ──
  // Waalaxy columns: First name, Last name, Occupation, Company, LinkedIn profile URL
  if('First name' in row || 'Firstname' in row || 'firstName' in row){
    const firstName = row['First name']||row['Firstname']||row['firstName']||'';
    const lastName  = row['Last name']||row['Lastname']||row['lastName']||'';
    return {
      name:     `${firstName} ${lastName}`.trim()||'Unknown',
      firstName: firstName,
      company:  row['Company']||row['company']||row['Organization']||'',
      role:     row['Occupation']||row['occupation']||row['Job title']||row['Position']||'',
      email:    row['Email']||row['email']||'',
      linkedin: row['LinkedIn profile URL']||row['LinkedIn']||row['linkedin']||row['Profile URL']||'',
      website:  row['Website']||row['Company website']||'',
      location: row['Location']||row['City']||'',
      industry: row['Industry']||'',
      source:   'waalaxy',
    };
  }
  // ── Apollo export format ──
  const firstName = row['First Name']||row['first_name']||'';
  const lastName  = row['Last Name']||row['last_name']||'';
  const fullName  = row['Name']||row['name']||row['Full Name']||`${firstName} ${lastName}`.trim()||'Unknown';
  return {
    name:     fullName,
    firstName: fullName.split(' ')[0]||'there',
    company:  row['Company']||row['company']||row['Organization Name']||'',
    role:     row['Title']||row['title']||row['Job Title']||'',
    email:    row['Email']||row['email']||row['Work Email']||'',
    linkedin: row['LinkedIn Url']||row['linkedin_url']||row['LinkedIn']||'',
    website:  row['Website']||row['Company Website']||'',
    location: row['City']||row['city']||row['Location']||'',
    industry: row['Industry']||row['industry']||'',
    source:   'apollo',
  };
}

async function importFromCSV(buffer){
  const rows = await parseApolloCSV(buffer);
  if(!rows.length) return {leadsAdded:0,contactsAdded:0,skipped:0,total:0};

  let leadsAdded=0, contactsAdded=0, skipped=0;
  const byCompany = {};

  for(const row of rows){
    const p = detectAndNormalizeRow(row);
    if(!p.company){ skipped++; continue; }
    const coKey = p.company.toLowerCase().trim();
    if(!byCompany[coKey]) byCompany[coKey]={ company:p.company, website:p.website, industry:p.industry, location:p.location, source:p.source, people:[] };
    byCompany[coKey].people.push(p);
  }

  const maxContacts = parseInt(process.env.CONTACTS_PER_COMPANY)||2;

  for(const [coKey, {company,website,industry,location,source,people}] of Object.entries(byCompany)){
    // Skip duplicate companies — just add new contacts
    const existsR = await db.execute({sql:'SELECT * FROM leads WHERE LOWER(TRIM(company))=LOWER(TRIM(?))',args:[coKey]});
    const exists = existsR.rows[0]||null;
    if(exists){
      for(const p of people.slice(0,maxContacts)){
        const dupR = await db.execute({sql:'SELECT id FROM contacts WHERE lead_id=? AND name=?',args:[exists.id,p.name]});
          const dup = dupR.rows[0]||null;
        if(!dup){ await insertContact({lead_id:exists.id,name:p.name,first_name:p.firstName||p.name.split(' ')[0]||'there',role:p.role,email:p.email||null,linkedin:p.linkedin||null}); contactsAdded++; }
      }
      continue;
    }
    const r = await insertLead({company,website,industry,size:null,location,source,notes:`Imported from ${source==='waalaxy'?'Waalaxy':'Apollo.io'} CSV`});
    leadsAdded++;
    for(const p of people.slice(0,maxContacts)){
      await insertContact({lead_id:r.lastInsertRowid,name:p.name,first_name:p.firstName||p.name.split(' ')[0]||'there',role:p.role,email:p.email||null,linkedin:p.linkedin||null});
      contactsAdded++;
    }
  }

  await dbLog('📋',`CSV import complete (${rows[0]&&'First name' in rows[0]?'Waalaxy':'Apollo'} format)`,`${leadsAdded} companies, ${contactsAdded} contacts, ${skipped} skipped`);
  return{leadsAdded,contactsAdded,skipped,total:rows.length};
}

// ── Reply watcher ─────────────────────────────────────────────────────────
async function checkForReplies(){return new Promise(resolve=>{const imap=new Imap({user:process.env.SMTP_USER,password:process.env.SMTP_PASS,host:process.env.IMAP_HOST||'imap.hostinger.com',port:993,tls:true,tlsOptions:{rejectUnauthorized:false},connTimeout:15000,authTimeout:15000});let found=0;imap.once('ready',()=>{imap.openBox('INBOX',false,(err)=>{if(err){imap.end();return resolve(0);}const since=new Date();since.setDate(since.getDate()-30);imap.search(['UNSEEN',['SINCE',since.toLocaleDateString('en-US',{month:'short',day:'2-digit',year:'numeric'})]],async(err,uids)=>{if(err||!uids?.length){imap.end();return resolve(0);}const fetch=imap.fetch(uids,{bodies:'',markSeen:false});const promises=[];fetch.on('message',msg=>{const p=new Promise(res=>{let buf='';msg.on('body',s=>s.on('data',d=>buf+=d.toString()));msg.once('end',async()=>{try{const parsed=await simpleParser(buf);const fe=parsed.from?.value?.[0]?.address?.toLowerCase();if(!fe)return res();const contactRes=await db.execute({sql:'SELECT * FROM contacts WHERE LOWER(email)=?',args:[fe]});
              const contact=contactRes.rows[0]||null;if(contact&&contact.status!=='replied'){found++;markContactReplied(contact.id);const lead=await getLeadById(contact.lead_id);insertEmail({contact_id:contact.id,lead_id:contact.lead_id,direction:'in',subject:parsed.subject||'',body:parsed.text||'',from_addr:fe,to_addr:process.env.SMTP_USER,template_num:null,message_id:parsed.messageId||null});await dbLog('💬','REPLY!',`${lead?.company}—${contact.name}`);await sendNotif({contactName:contact.name,companyName:lead?.company||'',replyBody:(parsed.text||'').substring(0,500)});}}catch(e){}res();});});promises.push(p);});fetch.once('end',async()=>{await Promise.all(promises);imap.end();resolve(found);});});});});imap.once('error',()=>resolve(0));imap.once('end',()=>{});imap.connect();});}

// ── Job Board Scrapers (RSS + Public APIs — no bot blocking) ─────────────

function extractDomain(url=''){
  try{ return new URL(url.startsWith('http')?url:'https://'+url).hostname.replace('www.',''); }
  catch(e){ return ''; }
}


// ── 1. Indeed RSS (most reliable — official feed) ────────────────────────
async function scrapeIndeed(){
  const results = [];
  // 6 highest-signal keywords only — keeps Indeed under 12s total
  const keywords = [
    'IT+staff+augmentation+remote',
    'contract+to+hire+developer+remote',
    'contract+React+developer+remote',
    'contract+DevOps+engineer+remote',
    'contract+full+stack+developer+remote',
    'contract+backend+developer+remote',
  ];
  // Only run 5 keywords per scrape — keeps Indeed under 30s total
  const kwBatchRaw = await kvGet('indeedKwIdx').catch(()=>null);
  const kwBatchIdx = parseInt(kwBatchRaw||0)||0;
  await kvSet('indeedKwIdx', (kwBatchIdx+5) % keywords.length);
  const runKeywords = keywords.slice(kwBatchIdx % keywords.length, (kwBatchIdx % keywords.length)+5);

  for(const kw of runKeywords){
    try{
      // Rotate countries per keyword — hits different markets each scrape
      const countries = ['United+States','United+Kingdom','Canada','Australia','Germany','Singapore','United+Arab+Emirates'];
      const countryIdx = Math.floor(Math.random()*countries.length);
      const url = `https://www.indeed.com/rss?q=${kw}&l=${countries[countryIdx]}&sort=date&fromage=14`;
      const r = await axios.get(url,{
        headers:{'User-Agent':'Mozilla/5.0 (compatible; RSS/2.0)','Accept':'application/rss+xml,text/xml'},
        timeout:20000
      });
      const xml = r.data;
      // Parse items from RSS
      const items = xml.split('<item>').slice(1);
      for(const item of items.slice(0,8)){
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s)||item.match(/<title>(.*?)<\/title>/s)||[])[1]||'';
        const desc    = (item.match(/<description><!\[CDATA\[(.*?)\]\]><\/description>/s)||[])[1]||'';
        const link    = (item.match(/<link>(.*?)<\/link>/s)||[])[1]||'';
        const text    = (title+' '+desc).toLowerCase();
        // Remote only — skip on-site/hybrid
        if(!/\bremote\b/.test(text)) continue;
        if(/\bon.?site\b|\bin.?office\b|\bin.?person\b/.test(text)) continue;
        // Extract company from title: "Job Title - Company Name"
        const dashIdx = title.lastIndexOf(' - ');
        const company = dashIdx>0 ? title.substring(dashIdx+3).trim() : '';
        if(company && company.length>1 && company.length<60 && !company.toLowerCase().includes('indeed')){
          results.push({company, source:'indeed', link, location:'Remote / USA', notes:`Indeed: hiring ${kw.replace('+',' ')} · Remote`});
        }
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,800));
  }
  await dbLog('💼','Indeed RSS',`${results.length} job posts found`);
  return results;
}

// ── 2. HackerNews "Who is Hiring" (monthly thread) ───────────────────────
async function scrapeHNHiring(){
  const results = [];
  try{
    // Search HN for the latest "Who is Hiring" thread
    const search = await axios.get('https://hn.algolia.com/api/v1/search?query=Ask+HN+Who+is+hiring&tags=story&restrictSearchableAttributes=title',{timeout:10000});
    const hits = search.data?.hits||[];
    const thread = hits.find(h=>h.title?.includes('Who is Hiring'));
    if(!thread) return results;

    // Get the thread comments
    const comments = await axios.get(`https://hn.algolia.com/api/v1/items/${thread.objectID}`,{timeout:15000});
    const children = comments.data?.children||[];

    for(const comment of children.slice(0,50)){
      const text = comment?.text||'';
      if(!text) continue;
      // Extract company name — HN hiring posts start with "Company | Location | ..."
      const pipeMatch = text.match(/^<p>([^|<]{2,50})\s*\|/);
      const company = pipeMatch?pipeMatch[1].trim():'';
      if(company && company.length>1){
        // Check if it mentions outsourcing-relevant roles
        const relevant = /react|node|python|javascript|typescript|devops|backend|frontend|fullstack|full.stack|engineer|developer/i.test(text);
        if(relevant){
          results.push({company, source:'hackernews', notes:'HN Who is Hiring — tech roles'});
        }
      }
    }
    await dbLog('🔶','HackerNews Hiring',`${results.length} companies found`);
  }catch(e){ await dbLog('⚠️','HN error',e.message); }
  return results;
}

// ── 3. YCombinator Jobs (official page) ──────────────────────────────────
async function scrapeYC(){
  const results = [];
  try{
    // Use HN Algolia API to find YC job posts
    const r = await axios.get('https://hn.algolia.com/api/v1/search?query=hiring&tags=job&hitsPerPage=50',{timeout:15000});
    const hits = r.data?.hits||[];
    for(const hit of hits){
      const company = hit.author||'';
      const text = hit.title||hit.story_text||'';
      if(company && text){
        const relevant = /react|node|python|developer|engineer|devops|frontend|backend/i.test(text);
        if(relevant) results.push({company, source:'ycombinator', notes:`YC Jobs: ${text.substring(0,80)}`});
      }
    }
    await dbLog('🚀','YC Jobs',`${results.length} companies found`);
  }catch(e){ await dbLog('⚠️','YC error',e.message); }
  return results;
}

// ── 4. Jobicy + Remotive + WeWorkRemotely RSS (RemoteOK blocked) ─────────
async function scrapeRemoteOK(){
  const results = [];

  // ── Jobicy RSS — fresh daily remote jobs, no auth ─────────────────────
  const jobicyFeeds = [
    'https://jobicy.com/?feed=job_feed&job_categories=dev-engineer&job_types=full-time,contract',
    'https://jobicy.com/?feed=job_feed&job_categories=design-ux&job_types=full-time,contract',
    'https://jobicy.com/?feed=job_feed&job_categories=data-science&job_types=full-time,contract',
    'https://jobicy.com/?feed=job_feed&job_categories=devops-sysadmin&job_types=full-time,contract',
    'https://jobicy.com/?feed=job_feed&job_categories=cybersecurity&job_types=full-time,contract',
  ];
  for(const feedUrl of jobicyFeeds){
    try{
      const r = await axios.get(feedUrl,{
        headers:{'User-Agent':'Mozilla/5.0 (compatible; RSS/2.0)','Accept':'application/rss+xml,text/xml'},
        timeout:15000
      });
      const items = r.data.split('<item>').slice(1);
      for(const item of items.slice(0,12)){
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s)||item.match(/<title>(.*?)<\/title>/s)||[])[1]||'';
        const link    = (item.match(/<link>(.*?)<\/link>/s)||[])[1]||'';
        const creator = (item.match(/<dc:creator><!\[CDATA\[(.*?)\]\]><\/dc:creator>/s)||item.match(/<dc:creator>(.*?)<\/dc:creator>/s)||[])[1]||'';
        const company = creator || (title.match(/ at (.+)$/)?.[1]||'').trim();
        if(company && company.length>1 && company.length<80 && !['jobicy','unknown'].includes(company.toLowerCase())){
          results.push({company, source:'remoteok', location:'Remote / USA', job_url:link,
            job_title:title.substring(0,80),
            notes:`Jobicy: ${title.substring(0,80)}`});
        }
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,600));
  }

  // ── Remotive RSS — established remote job board ───────────────────────
  const remotiveFeeds = [
    'https://remotive.com/remote-jobs/feed/software-dev',
    'https://remotive.com/remote-jobs/feed/devops',
    'https://remotive.com/remote-jobs/feed/data',
    'https://remotive.com/remote-jobs/feed/qa',
  ];
  for(const feedUrl of remotiveFeeds){
    try{
      const r = await axios.get(feedUrl,{
        headers:{'User-Agent':'Mozilla/5.0 (compatible; RSS/2.0)','Accept':'application/rss+xml,text/xml'},
        timeout:15000
      });
      const items = r.data.split('<item>').slice(1);
      for(const item of items.slice(0,10)){
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s)||item.match(/<title>(.*?)<\/title>/s)||[])[1]||'';
        const link    = (item.match(/<link>(.*?)<\/link>/s)||[])[1]||'';
        const company = (title.match(/ at (.+)$/)?.[1]||'').trim() ||
                        (item.match(/<company><!\[CDATA\[(.*?)\]\]><\/company>/s)||[])[1]||'';
        if(company && company.length>1 && company.length<80){
          results.push({company, source:'remoteok', location:'Remote / USA', job_url:link,
            job_title:title.replace(/ at .+$/,'').trim().substring(0,80),
            notes:`Remotive: ${title.substring(0,80)}`});
        }
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,600));
  }

  // ── WeWorkRemotely RSS ────────────────────────────────────────────────
  const wwrFeeds = [
    'https://weworkremotely.com/categories/remote-programming-jobs.rss',
    'https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss',
    'https://weworkremotely.com/categories/remote-data-science-ai-statistics-jobs.rss',
  ];
  for(const feedUrl of wwrFeeds){
    try{
      const r = await axios.get(feedUrl,{
        headers:{'User-Agent':'Mozilla/5.0 (compatible; RSS/2.0)','Accept':'application/rss+xml,text/xml'},
        timeout:15000
      });
      const items = r.data.split('<item>').slice(1);
      for(const item of items.slice(0,10)){
        const title   = (item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/s)||item.match(/<title>(.*?)<\/title>/s)||[])[1]||'';
        const link    = (item.match(/<link>(.*?)<\/link>/s)||[])[1]||'';
        // WWR format: "Company: Job Title"
        const colonIdx = title.indexOf(': ');
        const company  = colonIdx>0 ? title.substring(0,colonIdx).trim() : (title.match(/ at (.+)$/)?.[1]||'').trim();
        const jobTitle = colonIdx>0 ? title.substring(colonIdx+2).trim() : title;
        if(company && company.length>1 && company.length<80 && !['anywhere','full-time'].includes(company.toLowerCase())){
          results.push({company, source:'remoteok', location:'Remote / USA', job_url:link,
            job_title:jobTitle.substring(0,80),
            notes:`WeWorkRemotely: ${title.substring(0,80)}`});
        }
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,600));
  }

  await dbLog('🌍','Remote Jobs (Jobicy+Remotive+WWR)',`${results.length} companies found`);
  return results;
}


// ── 5. Greenhouse Job Board API — rotates through 120 companies ─────────
async function scrapeGreenhouse(){
  const results = [];
  // 120 real companies on Greenhouse — rotates 10 per run so we see fresh ones each time
  const allCompanies = [
    // Tier 1 — large SaaS (known outsourcing buyers)
    'airbnb','stripe','twilio','shopify','hubspot','zendesk','intercom',
    'squarespace','asana','figma','notion','linear','vercel','netlify',
    'datadog','newrelic','pagerduty','atlassian','okta','cloudflare',
    'fastly','hashicorp','mongodb','elastic','snowflake','segment','postmark',
    // Tier 2 — mid-size SaaS
    'brex','rippling','gusto','lattice','culture-amp','leapsome',
    'personio','deel','remote','oyster','papaya-global','globalhr',
    'loom','miro','coda','retool','airplane','nango','merge',
    'hightouch','census','polytomic','rudderstack','fivetran',
    // Tier 3 — fintech
    'plaid','marqeta','checkout','adyen','tabapay','lithic',
    'unit','column','treasury-prime','synctera','bond',
    'ramp','mercury','brex','puzzle','pilot','mainstreet',
    // Tier 4 — healthtech
    'ro','cerebral','headway','alma','spring-health','lyra',
    'hinge-health','sword-health','transcarent','carerev','clipboard-health',
    // Tier 5 — infrastructure/devtools
    'supabase','planetscale','railway','render','fly','turso',
    'clerk','authzed','warrant','permit','ory',
    'resend','loops','courier','novu','knock',
    'sentry','highlight','logflare','axiom','baselime',
    // Tier 6 — e-commerce/marketplace
    'faire','gorgias','recharge','rechargepayments','loop',
    'postscript','attentive','klaviyo','yotpo','okendo',
    'nacelle','shogun','searchspring','constructor','bloomreach',
    // Tier 7 — AI/ML startups (fastest growing, need most dev help)
    'cohere','anthropic','scale','labelbox','snorkel',
    'weights-biases','determined-ai','modal','replicate','banana',
    'runway','stability','midjourney','character','inflection',
  ];
  const keywords = [
    'react','node','python','engineer','developer','devops','backend','frontend',
    'qa','cloud','data','mobile','java','security','support','manager','scrum',
    'typescript','golang','rust','kotlin','swift','flutter',
  ];

  // Rotate 10 companies per run — cycles through all 120 over 12 runs
  const batchIdxRaw = await kvGet('ghBatchIdx').catch(()=>null);
  const batchIdx = parseInt(batchIdxRaw||0)||0;
  const start = (batchIdx * 8) % allCompanies.length;
  const batch = allCompanies.slice(start, start+8);
  await kvSet('ghBatchIdx', batchIdx+1);
  await dbLog('🌱','Greenhouse',`Checking batch ${batchIdx+1}: ${batch.join(', ')}`);

  for(const co of batch){
    try{
      const r = await axios.get(`https://boards-api.greenhouse.io/v1/boards/${co}/jobs`,{timeout:5000});
      const jobs = r.data?.jobs||[];
      const relevant = jobs.filter(j=>keywords.some(k=>(j.title||'').toLowerCase().includes(k)));
      if(relevant.length>0){
        const jobTitles = relevant.slice(0,3).map(j=>j.title).join(', ');
        const displayName = co.replace(/-/g,' ').replace(/\w/g,x=>x.toUpperCase());
        results.push({
          company: displayName,
          website: `https://${co}.com`,
          source: 'greenhouse',
          job_title: relevant[0]?.title||'Developer',
          job_url: relevant[0]?.absolute_url||`https://boards.greenhouse.io/${co}`,
          job_desc: `Open roles: ${jobTitles}`,
          notes:`Greenhouse: ${relevant.length} open role${relevant.length!==1?'s':''} — ${jobTitles}`
        });
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,200));
  }
  await dbLog('🌱','Greenhouse',`${results.length} companies actively hiring this batch`);
  return results;
}


// ── 6b. Lever.co Public API — contract roles (no auth needed) ───────────
// Lever exposes a public jobs API for every company. We search for
// companies that have posted "contract" roles — perfect outsourcing signal.
async function scrapeLever(){
  const results = [];
  const leverCompanies = [
    'netflix','airbnb','lyft','pinterest','reddit','tumblr','duolingo',
    'coinbase','robinhood','carta','brex','gusto','rippling','lattice',
    'notion','figma','airtable','loom','miro','retool','linear',
    'vercel','supabase','railway','render','fly','neon',
    'stripe','plaid','marqeta','checkout','adyen',
    'datadog','newrelic','pagerduty','honeycomb','grafana',
    'cloudflare','fastly','sumo-logic','cribl','observe',
    'hubspot','intercom','zendesk','freshworks','klaviyo',
    'docusign','dropbox','box','atlassian','gitlab',
  ];
  // Engineering keywords — ANY dev role = potential outsourcing lead
  const engKeywords = ['engineer','developer','devops','frontend','backend','fullstack','full stack',
    'react','node','python','java','mobile','ios','android','cloud','aws','data','qa','security','platform'];

  const batchRaw = await kvGet('leverBatchIdx').catch(()=>null);
  const batchIdx = parseInt(batchRaw||0)||0;
  await kvSet('leverBatchIdx', batchIdx+1);
  const batchSize = 12; // check 12 per run
  const startPos = (batchIdx*batchSize) % leverCompanies.length;
  const batch = leverCompanies.slice(startPos, startPos+batchSize);

  for(const co of batch){
    try{
      const r = await axios.get(`https://api.lever.co/v0/postings/${co}?mode=json&limit=25`,{timeout:12000});
      const jobs = Array.isArray(r.data)?r.data:[];
      // Get any engineering role — company hiring engineers = potential outsourcing buyer
      const engJobs = jobs.filter(j=>engKeywords.some(k=>(j.text||'').toLowerCase().includes(k)));
      if(engJobs.length > 0){
        const j = engJobs[0];
        const displayName = co.replace(/-/g,' ').replace(/\w/g,x=>x.toUpperCase());
        const allTitles = engJobs.slice(0,3).map(x=>x.text).join(', ');
        results.push({
          company: displayName,
          website: `https://${co}.com`,
          source: 'greenhouse',
          job_title: j.text||'Developer',
          job_url: j.hostedUrl||`https://jobs.lever.co/${co}`,
          job_desc: (j.descriptionPlain||'').substring(0,300),
          notes:`Lever: ${engJobs.length} engineering role${engJobs.length!==1?'s':''} — ${allTitles}`
        });
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,250));
  }
  await dbLog('⚙️','Lever API',`${results.length} companies hiring engineers (batch ${batchIdx+1})`);
  return results;
}


async function scrapeAshby(){
  const results = [];
  const ashbyCompanies = [
    'openai','anthropic','mistral','cohere','scale','modal',
    'arc','linear','raycast','superhuman','notion','coda',
    'ramp','mercury','brex','deel','remote','rippling',
    'pika','runway','synthesia','heygen','eleven-labs',
    'warp','zed','cursor','replit','sourcegraph',
  ];
  const batchRaw = await kvGet('ashbyBatchIdx').catch(()=>null);
  const batchIdx = parseInt(batchRaw||0)||0;
  await kvSet('ashbyBatchIdx', batchIdx+1);
  const batch = ashbyCompanies.slice((batchIdx*5)%ashbyCompanies.length, (batchIdx*5)%ashbyCompanies.length+5);

  for(const co of batch){
    try{
      const r = await axios.post('https://api.ashbyhq.com/posting-api/job-board',
        {organizationHostedJobsPageName: co},
        {headers:{'Content-Type':'application/json'},timeout:5000}
      );
      const jobs = r.data?.jobPostings||[];
      const techKeywords2 = ['engineer','developer','devops','frontend','backend','react','python','java','mobile','cloud','data','ml','ai','qa','security','platform'];
      const relevant = jobs.filter(j=>techKeywords2.some(k=>(j.title||'').toLowerCase().includes(k)));
      if(relevant.length > 0){
        const j = relevant[0];
        const displayName = co.replace(/-/g,' ').replace(/\w/g,x=>x.toUpperCase());
        const titles = relevant.slice(0,3).map(j=>j.title).join(', ');
        results.push({
          company: displayName,
          website: `https://${co}.com`,
          source: 'greenhouse',
          job_title: j.title||'Developer',
          job_url: j.jobUrl||`https://jobs.ashbyhq.com/${co}`,
          job_desc: `Open roles: ${titles}`,
          notes: `Ashby: ${relevant.length} tech role${relevant.length!==1?'s':''} — ${titles}`
        });
      }
    }catch(e){}
    await new Promise(r=>setTimeout(r,150));
  }
  await dbLog('⚙️','Ashby API',`${results.length} companies with contract roles (batch ${batchIdx+1})`);
  return results;
}

// ── 6. Serper.dev Search API — 1 credit per run, broad query, AI scores leads ──
async function scrapeGoogleCustom(){
  const key = process.env.SERPER_API_KEY;
  if(!key) return [];

  const LEADS_PER_RUN = 50;
  const results = [];

  // ── One broad query covers the whole web in 1 credit ─────────────────
  // Catches: Reddit, LinkedIn, job boards, forums, news, Facebook, Twitter, HN
  // "OR" chains mean Google searches across ALL these signals simultaneously
  // Rotate through 6 focused queries — each covers a different intent angle
  // ── Strategy: use site-specific queries that CANNOT return articles ────
  // site:reddit.com = only Reddit posts (zero articles possible)
  // site:lever.co = only job postings (zero articles possible)
  // site:boards.greenhouse.io = only job postings
  // site:wellfound.com/jobs = only job listings
  // This is better than broad queries + article filters

  const allQueries = [
    // ── TIER 1: Reddit — physically impossible to return articles ──────────
    // Reddit posts only. No blog, no vendor, no article can appear here.
    'site:reddit.com/r/startups "looking for" developers OR "dev agency" OR outsource OR "contract developer" 2025 OR 2026',
    'site:reddit.com/r/entrepreneur "need" developers OR engineers OR "dev team" OR "software agency" OR outsource 2025 OR 2026',
    'site:reddit.com/r/SaaS "need developers" OR "looking for developers" OR "dev team" OR "contract engineer" 2025 OR 2026',
    'site:reddit.com/r/smallbusiness "IT support" OR "managed services" OR "software development" "looking for" OR "need" 2025 OR 2026',
    'site:reddit.com/r/forhire hiring developer OR engineer OR "dev team" budget remote 2025 OR 2026',
    'site:reddit.com/r/webdev "looking for" agency OR developer OR team hire budget outsource 2025 OR 2026',

    // ── TIER 2: LinkedIn posts section only — not company pages or articles ─
    'site:linkedin.com/posts "looking for" "contract developer" OR "contract engineer" OR "dev agency" 2026',
    'site:linkedin.com/posts "anyone recommend" "software" OR "development" OR "IT" company OR agency 2026',
    'site:linkedin.com/posts "need" developers OR engineers OR "dev team" contract OR outsource 2026',
    'site:linkedin.com/posts "staff augmentation" OR "contract to hire" "looking for" OR need 2026',

    // ── TIER 3: Job boards — only job listings, no articles possible ───────
    'site:lever.co "contract" OR "contract to hire" developer OR engineer OR DevOps remote',
    'site:boards.greenhouse.io "contract" OR "contractor" developer OR engineer remote',
    'site:wellfound.com/jobs "contract" OR "remote" developer OR engineer OR DevOps OR QA',
    'site:jobs.ashbyhq.com "contract" OR "contractor" developer OR engineer remote',

    // ── TIER 4: HN + IndieHackers — posts only, no articles ────────────────
    'site:news.ycombinator.com "looking for" OR "need" developer OR engineer OR "dev team" contract OR outsource',
    'site:indiehackers.com "looking for" developer OR "dev team" OR agency outsource hire 2025 OR 2026',
  ];

  // Rotate 1 query per run — 1 credit, focused results, zero article noise
  const idxRaw = await kvGet('serperQueryIdx').catch(()=>null);
  const idx = parseInt(idxRaw||0)||0;
  const query = allQueries[idx % allQueries.length];
  await kvSet('serperQueryIdx', (idx+1) % allQueries.length);
  await dbLog('🔎','Serper',`Query ${idx+1}/${allQueries.length}: ${query.substring(0,80)}...`);

  try{
    const r = await axios.post('https://google.serper.dev/search',
      { q: query, num: 10, gl: 'us', hl: 'en' },
      { headers:{ 'X-API-KEY': key, 'Content-Type':'application/json' }, timeout:15000 }
    );

    const items = [
      ...(r.data?.organic||[]),
      ...(r.data?.news||[]),
    ].slice(0,12);

    for(const item of items){
      if(results.length >= LEADS_PER_RUN) break;

      const title  = item.title||'';
      const link   = item.link||'';
      const desc   = item.snippet||item.description||'';
      const text   = (title+' '+desc).toLowerCase();

      // ── STRONG article/seller filters ────────────────────────────────
      // Skip vendors promoting themselves
      const isSeller = /our services|hire us|contact us|we offer|we provide|we specialize|get a quote|free consultation|outsourcing company|development agency|we are a|we help companies|our team|our clients|our portfolio/i.test(title+' '+desc);
      if(isSeller) continue;

      // Skip blog articles — broader filter
      const isBlog = /top \d+|best \d+|how to |guide to|tips for|what is |what are |vs\.|comparison|\d+ ways|benefits of|ultimate guide|complete guide|everything you need|step by step|a guide|an overview|introduction to|explained|definition of/i.test(title);
      if(isBlog) continue;

      // Skip known article/news sites that never have hiring posts
      const isNewsSite = /forbes|techcrunch|medium\.com|substack|hackernoon|dev\.to|dzone|infoq|zdnet|wired|businessinsider|entrepreneur\.com\/article/i.test(link);
      if(isNewsSite) continue;

      // Must have some company or hiring signal
      const hasSignal = /(hiring|looking for|need|contract|outsourc|augment|agency|startup|raised|funded)/i.test(text);
      if(!hasSignal) continue;

      // ── Extract company name ──────────────────────────────────────────
      let company = '';
      let jobTitle = '';

      // Job board URL: lever.co/stripe → Stripe
      const jbMatch = link.match(/(?:lever\.co|greenhouse\.io|workable\.com|ashbyhq\.com|wellfound\.com|breezy\.hr|bamboohr\.com|smartrecruiters\.com)\/([a-z0-9\-]+)/i);
      if(jbMatch) company = jbMatch[1].replace(/-/g,' ').replace(/\w/g,x=>x.toUpperCase());

      // LinkedIn company: linkedin.com/company/stripe
      const liCoMatch = link.match(/linkedin\.com\/company\/([a-z0-9\-]+)/i);
      if(!company && liCoMatch) company = liCoMatch[1].replace(/-/g,' ').replace(/\w/g,x=>x.toUpperCase());

      // "Role at Company" in title
      if(!company){
        const atMatch = title.match(/ at ([A-Z][A-Za-z0-9 &,\.]{1,40})(?:\s*[-|]|$)/);
        if(atMatch) company = atMatch[1].trim();
      }

      // "Company is looking for" in snippet
      if(!company){
        const snipMatch = desc.match(/([A-Z][A-Za-z0-9 &]{2,35}) (?:is looking|is hiring|needs|want)/);
        if(snipMatch) company = snipMatch[1].trim();
      }

      // "Company - Role" in title
      if(!company){
        const dashMatch = title.match(/^([A-Z][A-Za-z0-9 &]{2,35}) [-–|] /);
        if(dashMatch) company = dashMatch[1].trim();
      }

      // Domain name fallback (skip job boards + known platforms)
      if(!company){
        const domMatch = link.match(/https?:\/\/(?:www\.)?([^\/]+)/);
        const dom = domMatch?.[1]||'';
        const skip = ['reddit','linkedin','facebook','twitter','x.com','github','medium','substack',
          'lever','greenhouse','workable','ashbyhq','wellfound','breezy','bamboohr','smartrecruiters',
          'indeed','glassdoor','ziprecruiter','monster','dice','angel','ycombinator','techcrunch',
          'crunchbase','indiehackers','producthunt','hackernews','news.ycombinator'];
        const domName = dom.split('.')[0];
        if(!skip.some(s=>dom.includes(s)) && domName.length>2)
          company = domName.charAt(0).toUpperCase()+domName.slice(1);
      }

      company = company.trim().replace(/[.,]$/, '').substring(0,60);
      if(!company || company.length < 2) continue;

      // ── Build lead score signal for AI later ─────────────────────────
      const signals = [];
      if(/contract|outsourc|augment|vendor|agency/i.test(text)) signals.push('outsourcing intent');
      if(/series a|seed round|raised|funded/i.test(text)) signals.push('funded startup');
      if(/looking for|need|hiring|want/i.test(text)) signals.push('active search');
      if(/remote/i.test(text)) signals.push('remote');
      if(/reddit|linkedin|indiehackers/i.test(link)) signals.push('social post');

      const salaryMatch = text.match(/\$[\d,]+(?:k|\/hr| per hour)?/i);

      results.push({
        company,
        source: 'serper',
        job_url: link,
        website: link,
        job_title: title.substring(0,80),
        job_desc: desc.substring(0,250),
        salary: salaryMatch?.[0]||'',
        location: 'Remote / USA',
        notes: `Serper: ${title.substring(0,80)}${signals.length?' · ['+signals.join(', ')+']':''}`,
      });
    }

    await dbLog('🔎','Serper query done',`${results.length} total leads so far`);
  }catch(e){
    if(e.response?.status===403||e.response?.status===429){
      await dbLog('⚠️','Serper quota','Credit limit hit — stopping');
      return results; // stop if quota hit
    } else {
      await dbLog('⚠️','Serper error',e.message);
    }
  }
  await dbLog('🔎','Serper done',`${results.length} leads`);
  return results;
}


// ── 7. LinkedIn Posts via Serper News API ───────────────────────────────
// Uses Serper's news search which indexes LinkedIn activity posts better
// than organic search. Costs 1 credit per call.
async function scrapeLinkedInPosts(){
  const key = process.env.SERPER_API_KEY;
  if(!key) return [];
  const results = [];

  // These queries find people POSTING about needing dev help on LinkedIn
  // Using news search finds recent posts (last 7 days) not old indexed pages
  const liQueries = [
    'site:linkedin.com/posts "looking for" developers OR engineers OR "dev team" OR agency 2026',
    'site:linkedin.com/posts "need" "contract developer" OR "contract engineer" OR "dev agency" 2026',
    'site:linkedin.com/posts "hiring" "contract" developer OR engineer remote 2026',
    'site:linkedin.com/posts "anyone recommend" software OR development company OR agency 2026',
    'site:linkedin.com/posts "outsource" development OR engineering "looking" 2026',
  ];

  // Pick 1 LinkedIn query per run, rotated separately from main Serper queries
  const idxRaw = await kvGet('liQueryIdx').catch(()=>null);
  const idx = parseInt(idxRaw||0)||0;
  const query = liQueries[idx % liQueries.length];
  await kvSet('liQueryIdx', (idx+1) % liQueries.length);

  try{
    // Use news endpoint — better for recent LinkedIn posts
    const r = await axios.post('https://google.serper.dev/news',
      { q: query, num: 10, gl: 'us', hl: 'en' },
      { headers:{ 'X-API-KEY': key, 'Content-Type':'application/json' }, timeout:15000 }
    );

    const items = r.data?.news||[];
    await dbLog('🔵','LinkedIn Posts',`Query: ${query.substring(0,60)}... → ${items.length} results`);

    for(const item of items){
      const title = item.title||'';
      const link  = item.link||'';
      const desc  = item.snippet||'';
      const text  = (title+' '+desc).toLowerCase();

      // Must be a LinkedIn post URL
      if(!link.includes('linkedin.com')) continue;

      // Skip company pages and job listings — we want posts from people
      if(link.includes('/jobs/') || link.includes('/company/')) continue;

      // Must have outsourcing/hiring intent
      if(!/(looking for|need|hiring|recommend|outsource|contract|agency)/i.test(text)) continue;

      // Extract company from post — LinkedIn posts usually mention company name
      let company = '';
      const atMatch = desc.match(/at ([A-Z][A-Za-z0-9 &]{2,35})(?:\s*[,.|]|$)/);
      const coMatch = desc.match(/([A-Z][A-Za-z0-9 &]{2,35}) (?:is looking|is hiring|needs|want)/);
      const ceoMatch = desc.match(/(?:CEO|CTO|founder|VP|director) (?:at|of) ([A-Z][A-Za-z0-9 &]{2,35})/i);
      company = (atMatch?.[1]||coMatch?.[1]||ceoMatch?.[1]||'').trim();

      // Fallback: use the poster's company from title if format is "Name - Company"
      if(!company){
        const titleDash = title.match(/[-–] ([A-Z][A-Za-z0-9 &]{2,35})$/);
        if(titleDash) company = titleDash[1].trim();
      }

      if(!company || company.length < 2) continue;

      results.push({
        company,
        source: 'serper',
        job_url: link,
        website: '',
        job_title: `LinkedIn: ${title.substring(0,70)}`,
        job_desc: desc.substring(0,250),
        salary: '',
        location: 'Remote',
        notes: `LinkedIn Post: ${title.substring(0,80)} · [social post, outsourcing intent]`,
      });
    }
  }catch(e){
    await dbLog('⚠️','LinkedIn Posts error',e.message);
  }

  await dbLog('🔵','LinkedIn Posts done',`${results.length} leads found`);
  return results;
}

// ── AI Lead Scorer — Gemini scores each lead 1-10 for outsourcing intent ────
async function scoreLeadWithAI(lead){
  const key = process.env.GEMINI_API_KEY;
  if(!key) return 7; // default score if no Gemini key — still send
  try{
    const prompt = `Score this lead 1-10 for IT outsourcing sales potential.
Score 8-10: Company actively looking to outsource/hire contractors. Strong budget signals. Decision maker visible.
Score 5-7: Company hiring developers (could be converted to outsourcing pitch). Some signals.
Score 1-4: Blog post, vendor promoting themselves, no company, or irrelevant.

Lead:
Company: ${lead.company}
Source: ${lead.source}
Job/Post title: ${lead.job_title||''}
Description: ${(lead.job_desc||'').substring(0,200)}
URL: ${lead.job_url||''}
Notes: ${lead.notes||''}

Reply with ONLY a JSON object: {"score": 7, "reason": "one line reason"}`;

    const r = await axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${key}`,
      {contents:[{parts:[{text:prompt}]}]},
      {headers:{'Content-Type':'application/json'},timeout:10000}
    );
    const raw = r.data?.candidates?.[0]?.content?.parts?.[0]?.text||'{"score":5}';
    const parsed = JSON.parse(raw.replace(/```json|```/g,'').trim());
    return{score:Number(parsed.score)||5, reason:parsed.reason||''};
  }catch(e){
    return{score:6, reason:'AI scoring failed — default'};
  }
}

// ── Master scraper ────────────────────────────────────────────────────────
async function scrapeAllJobBoards(){
  await dbLog('🌐','Scrape started','Running each source sequentially...');

  // Helper — caps each scraper, never lets one hang the whole run
  async function run(name, fn, ms=18000){
    try{
      const result = await Promise.race([
        fn(),
        new Promise(r=>setTimeout(()=>r([]),ms))
      ]);
      await dbLog('✅',name,`${(result||[]).length} leads`);
      return result||[];
    }catch(e){
      await dbLog('⚠️',name+' failed',e.message);
      return [];
    }
  }

  // Sequential — no parallel overload, each gets its time slot
  const indeed     = await run('Indeed',     scrapeIndeed,     20000);
  const remote     = await run('RemoteOK',   scrapeRemoteOK,   15000);
  const greenhouse = await run('Greenhouse', scrapeGreenhouse, 15000);
  const lever      = await run('Lever',      scrapeLever,      15000);
  const ashby      = await run('Ashby',      scrapeAshby,      15000);
  const google     = await run('Serper',     scrapeGoogleCustom,15000);
  const linkedin   = await run('LinkedIn',   scrapeLinkedInPosts,12000);
  const hn         = await run('HackerNews', scrapeHNHiring,   10000);
  const yc         = await run('YC',         scrapeYC,         10000);

  const all = [
    ...indeed, ...remote, ...greenhouse,
    ...lever, ...ashby, ...google,
    ...linkedin, ...hn, ...yc,
  ];

  // Deduplicate — check existing leads in DB
  const seen = new Set();
  const existingCheck = await Promise.all(all.map(r=>leadExists(r.company)));
  const unique = all.filter((r,i)=>{
    const key = r.company.toLowerCase().trim();
    if(!key||key.length<2||seen.has(key)||existingCheck[i]) return false;
    seen.add(key);
    return true;
  });

  await dbLog('📊','Scrape complete',`${all.length} total → ${unique.length} new unique companies`);

  let added = 0;

  for(const co of unique){

    // ── Rule-based lead scoring (no AI credits needed) ──────────────────
    // Score signals from the scraped data itself
    let score = 5; // default
    const t = ((co.job_title||'')+(co.job_desc||'')+(co.notes||'')).toLowerCase();

    // Strong outsourcing signals +2 each
    if(/contract to hire|c2h|staff augmentation|outsourc|dedicated team|vendor|offshore/i.test(t)) score += 2;
    if(/contract|contractor|freelance|part.time/i.test(t)) score += 2;

    // Funding signals — has budget +2
    if(/series a|series b|seed round|raised|funded|million/i.test(t)) score += 2;

    // Active search signals +1 each
    if(/looking for|need|hiring|want to hire/i.test(t)) score += 1;
    if(/remote/i.test(t)) score += 1;
    if(/urgent|asap|immediately|fast|quickly/i.test(t)) score += 1;

    // Penalty — vendor promoting themselves -3
    if(/our services|we offer|we provide|hire us|get a quote|free consultation/i.test(t)) score -= 3;
    // Penalty — pure blog article -2
    if(/top \d+|best \d+|how to hire|guide to|benefits of outsourc/i.test(co.job_title||'')) score -= 2;

    score = Math.max(1, Math.min(10, score));

    // Save ALL leads — score stored in notes so dashboard can filter
    const jobDetails = [
      co.notes||'',
      co.job_title  ? `Job Title: ${co.job_title}` : '',
      co.job_url    ? `Job Post: ${co.job_url}` : '',
      co.job_desc   ? `Description: ${co.job_desc.substring(0,300)}` : '',
      co.salary     ? `Salary: ${co.salary}` : '',
      co.date_posted? `Posted: ${co.date_posted}` : '',
      `Score: ${score}/10`,
    ].filter(Boolean).join('\n');

    const r = await insertLead({
      company:co.company, website:co.website||null,
      industry:'Technology', size:null,
      location:co.location||'USA', source:co.source,
      notes:jobDetails,
    });
    await insertContact({
      lead_id:r.lastInsertRowid, name:'Unknown',
      first_name:'there', role:'CTO / HR Manager',
      email:null, linkedin:null,
    });
    added++;
  }

  await dbLog('✅','Scrape done',`${added} new leads saved (all scored 1-10, filter in dashboard)`);
  return {total:all.length, unique:unique.length, added};
}

// ── Agent ─────────────────────────────────────────────────────────────────
let agentRunning=false;
async function runAgentCycle({dryRun=false}={}){if(agentRunning)return;agentRunning=true;await dbLog('⚡','Agent started',dryRun?'DRY RUN':'LIVE');try{const r=await checkForReplies();if(r>0)await dbLog('🎉',`${r} replies`,'');// Scrape job boards for new leads
try{await scrapeAllJobBoards();}catch(e){await dbLog('⚠️','Scraper error',e.message);}try{const hunterFound=await enrichContactsWithHunter();if(hunterFound>0)await dbLog('🔍','Hunter enrichment',`${hunterFound} emails found`);}catch(e){}
const nc=await getContactsNotYetEmailed();let sent=0;const max=parseInt(process.env.MAX_EMAILS_PER_DAY)||30;for(const c of nc){if(sent>=max)break;const l=await getLeadById(c.lead_id);if(!l)continue;const res=await sendEmail({contact:c,lead:l,emailNum:1,dryRun});if(res.ok)sent++;if(!dryRun)await delay(parseInt(process.env.EMAIL_DELAY_SECONDS)||90);}const fc=await getContactsDueForFollowup();for(const c of fc){if(sent>=max)break;const l=await getLeadById(c.lead_id);if(!l)continue;const res=await sendEmail({contact:c,lead:l,emailNum:Math.min(c.emails_sent+1,4),dryRun});if(res.ok)sent++;if(!dryRun)await delay(parseInt(process.env.EMAIL_DELAY_SECONDS)||90);}await dbLog('✅','Cycle done',`${sent} emails`);}catch(e){await dbLog('❌','Agent error',e.message);}finally{agentRunning=false;}}
function startScheduler(){
  // ── AUTO-SCHEDULER DISABLED — manual mode only ──────────────────────────
  // Uncomment below when ready to go live:
  // cron.schedule('0 15 * * 2-4',()=>runAgentCycle());
  // cron.schedule('0 19 * * 2-4',()=>runAgentCycle());
  // cron.schedule('0 14 * * 1',()=>runAgentCycle());
  // cron.schedule('0 6 * * 1-5',()=>scrapeAllJobBoards().catch(console.error));
  // cron.schedule('*/15 * * * *',()=>checkForReplies());

  // ── Self-ping every 10min to prevent Railway from sleeping ────────────
  cron.schedule('*/10 * * * *',async()=>{
    try{
      const http=require('http');
      app.use(express.static(path.join(__dirname, 'public')));
      app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});
      const port=process.env.PORT||3000;
      http.get(`http://localhost:${port}/healthz`,()=>{}).on('error',()=>{});
    }catch(e){}
  });

  dbLog('📅','Manual mode','All actions manual — self-ping every 10min to stay awake');
}

// ── Express ───────────────────────────────────────────────────────────────
const app = express();

app.use(cors());
app.use(express.json());

// Serve frontend files
app.use(express.static(path.join(__dirname, 'public')));

// Root route
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/healthz',(req,res)=>res.status(200).send('OK'));
app.get('/api/ping',(req,res)=>res.json({ok:true,ts:Date.now(),uptime:process.uptime()}));
app.get('/api/stats',async(req,res)=>res.json(await getStats()));
app.get('/api/leads',async(req,res)=>res.json(await getAllLeads()));
app.get('/api/leads/:id',async(req,res)=>{const l=await getLeadById(req.params.id);if(!l)return res.status(404).json({error:'Not found'});const[contacts,thread]=await Promise.all([getContactsByLeadId(l.id),getEmailsByLeadId(l.id)]);res.json({...l,contacts,thread});});
app.post('/api/leads',async(req,res)=>{const{company,website,industry,size,location,source,notes,contacts}=req.body;if(!company)return res.status(400).json({error:'company required'});const r=await insertLead({company,website,industry,size,location,source:source||'manual',notes});const lid=r.lastInsertRowid;if(contacts?.length)for(const c of contacts)if(c.name||c.email)await insertContact({lead_id:lid,name:c.name||'Unknown',first_name:c.name?.split(' ')[0]||'there',role:c.role||'',email:c.email||null,linkedin:c.linkedin||null});await dbLog('➕','Lead added',company);res.json({ok:true,id:lid});});
app.delete('/api/leads/:id',async(req,res)=>{await db.execute({sql:'DELETE FROM leads WHERE id=?',args:[Number(req.params.id)]});await db.execute({sql:'DELETE FROM contacts WHERE lead_id=?',args:[Number(req.params.id)]});res.json({ok:true});});
app.patch('/api/contacts/:id/email',async(req,res)=>{await updateContactEmail(req.params.id,req.body.email);res.json({ok:true});});
app.patch('/api/contacts/:id/replied',async(req,res)=>{await markContactReplied(req.params.id);res.json({ok:true});});
app.post('/api/contacts/:id/send',async(req,res)=>{const c=await getContactById(req.params.id);if(!c)return res.status(404).json({error:'Not found'});if(!c.email)return res.status(400).json({error:'No email'});const l=await getLeadById(c.lead_id);res.json(await sendEmail({contact:c,lead:l,emailNum:c.emails_sent+1}));});
app.get('/api/activity',async(req,res)=>res.json(await getRecentActivity(parseInt(req.query.limit)||50)));
// ── Gemini AI Personalised Email ─────────────────────────────────────────
app.post('/api/ai/generate-email',async(req,res)=>{
  const key = process.env.GEMINI_API_KEY;
  if(!key) return res.json({ok:false,error:'GEMINI_API_KEY not set in Railway'});

  const {lead_id,contact_id,email_num=1} = req.body;
  const lead = await getLeadById(lead_id);
  if(!lead) return res.status(404).json({error:'Lead not found'});
  const contacts = await getContactsByLeadId(lead_id);
  const contact = contact_id ? contacts.find(c=>c.id===Number(contact_id)) : contacts[0];

  const notes = lead.notes||'';
  const jobTitle = (notes.match(/Job Title: (.+)/)?.[1]||'').trim();
  const salary   = (notes.match(/Salary: (.+)/)?.[1]||'').trim();
  const jobDesc  = notes.includes('Description: ') ? notes.split('Description: ')[1].split('\nSalary:')[0].split('\nPosted:')[0].trim() : '';
  const jobUrl   = (notes.match(/Job Post: (.+)/)?.[1]||'').trim();

  const prompt = `You are a BD expert writing a cold email for ByteOn Technologies, an IT outsourcing company based in India serving US/UK clients.

TARGET:
Company: ${lead.company}
Contact: ${contact?.name||'there'} (${contact?.role||'Decision Maker'})
${jobTitle?'Job they posted: '+jobTitle:''}
${salary?'Budget: '+salary:''}
${jobDesc?'Details: '+jobDesc.substring(0,250):''}
${jobUrl?'Job post URL: '+jobUrl:''}

ABOUT US: ByteOn Technologies — IT outsourcing: Dev, QA, DevOps, Cloud, IT Support, Data, Cybersecurity, PM. Resources ready in 5-7 days. 40-60% cheaper than hiring locally.

Write email #${email_num}:
${email_num===1?'First touch — reference their SPECIFIC job post or need. Be direct, concise, max 120 words.':''}
${email_num===2?'Follow-up day 3 — add a specific stat or case study. New angle. Max 80 words.':''}
${email_num===3?'Follow-up day 7 — address a different pain point (speed, quality, flexibility). Max 60 words.':''}
${email_num===4?'Break-up email day 14 — short, creates mild urgency, leaves door open. Max 50 words.':''}

Rules:
- Sound like a real human, not a sales robot
- NO "I hope this finds you well" or similar openers
- Reference ${lead.company} specifically
- One clear CTA: 15-min call
- Sign as: Anas | ByteOn Technologies | byteonai.com

Respond ONLY with valid JSON, no markdown, no extra text: {"subject":"...","body":"..."}`;

  try{
    const r = await axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${key}`,
      {contents:[{parts:[{text:prompt}]}]},
      {headers:{'Content-Type':'application/json'},timeout:25000}
    );
    const raw = r.data?.candidates?.[0]?.content?.parts?.[0]?.text||'{}';
    const clean = raw.replace(/```json|```/g,'').trim();
    const parsed = JSON.parse(clean);
    if(!parsed.subject||!parsed.body) throw new Error('Gemini returned incomplete JSON');
    await dbLog('🤖','Gemini email generated',`${lead.company} — email #${email_num}`);
    res.json({ok:true, subject:parsed.subject, body:parsed.body});
  }catch(e){
    await dbLog('❌','Gemini failed',e.response?.data?.error?.message||e.message);
    res.json({ok:false, error:e.response?.data?.error?.message||e.message});
  }
});



// ── gTTS Voice TTS Endpoint ───────────────────────────────────────────────
// Returns MP3 audio of the text spoken by Google TTS (free, no API key needed)
// Uses Google's public translate TTS — same engine as Google Translate
app.post('/api/ai/voice-tts', async(req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });

  try {
    // Google Translate TTS — free, no key, rate limited to ~200 chars per call
    // Split into chunks if needed
    const chunks = [];
    const maxLen = 190;
    const words = text.split(' ');
    let current = '';
    for (const word of words) {
      if ((current + ' ' + word).trim().length > maxLen) {
        if (current) chunks.push(current.trim());
        current = word;
      } else {
        current = (current + ' ' + word).trim();
      }
    }
    if (current) chunks.push(current.trim());

    // Fetch all chunks and concatenate the MP3 buffers
    const buffers = [];
    for (const chunk of chunks) {
      const encoded = encodeURIComponent(chunk);
      const url = `https://translate.google.com/translate_tts?ie=UTF-8&q=${encoded}&tl=en&client=tw-ob`;
      const r = await axios.get(url, {
        responseType: 'arraybuffer',
        timeout: 8000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Referer': 'https://translate.google.com/'
        }
      });
      buffers.push(Buffer.from(r.data));
    }

    const mp3 = Buffer.concat(buffers);
    res.set({
      'Content-Type': 'audio/mpeg',
      'Content-Length': mp3.length,
      'Cache-Control': 'no-cache'
    });
    res.send(mp3);

  } catch(e) {
    // If Google TTS fails, return 503 so frontend falls back to browser TTS
    res.status(503).json({ error: 'TTS unavailable: ' + e.message });
  }
});

// ── Voice Agent Proxy ─────────────────────────────────────────────────────
// Proxies Claude API calls from the frontend so the API key stays server-side
app.post('/api/ai/voice-query', async(req, res) => {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) return res.json({ ok: false, error: 'ANTHROPIC_API_KEY not set in Railway variables' });

  const { question, context } = req.body;
  if (!question) return res.status(400).json({ error: 'question required' });

  // Build live pipeline context from DB
  const stats = await getStats();
  const allLeads = await getAllLeads();
  const hotLeads = allLeads.filter(l => {
    const scoreMatch = (l.notes || '').match(/Score:\s*(\d+)\/10/);
    const score = scoreMatch ? Number(scoreMatch[1]) : 0;
    return score >= 7 || l.has_reply;
  });
  const pendingFollowups = allLeads.filter(l => l.total_sent > 0 && !l.has_reply);
  const newLeads = allLeads.filter(l => l.total_sent === 0 && l.contact_count > 0);
  const recentActivity = await getRecentActivity(10);

  const systemPrompt = `You are ByteOn's Voice Agent — the intelligent brain of an IT outsourcing outreach system. You have full real-time access to the lead pipeline data.

LIVE PIPELINE DATA (as of right now):
- Total Leads: ${stats.total_leads}
- Total Contacts: ${stats.total_contacts}
- Emails Sent (total): ${stats.total_sent}
- Replies Received: ${stats.total_replies}
- Follow-ups Due: ${stats.followups_due}
- Emails Found via Hunter: ${stats.emails_found}
- Hot Leads (score 7+): ${hotLeads.length}
- New Leads not yet contacted: ${newLeads.length}
- Pending Follow-ups: ${pendingFollowups.length}
- Reply Rate: ${stats.total_contacts > 0 ? Math.round((stats.total_replies / stats.total_contacts) * 100) : 0}%

HOT LEADS (top priority):
${hotLeads.slice(0, 6).map(l => {
  const score = (l.notes || '').match(/Score:\s*(\d+)\/10/)?.[1] || 'N/A';
  const jobTitle = (l.notes || '').match(/Job Title: (.+)/)?.[1] || '';
  return `- ${l.company} | Score: ${score}/10 | Status: ${l.has_reply ? 'REPLIED ✓' : l.total_sent > 0 ? 'contacted' : 'NEW'} | Emails sent: ${l.total_sent}${jobTitle ? ' | Role: ' + jobTitle : ''}`;
}).join('\n') || 'No hot leads yet'}

CONTACTS READY TO EMAIL (not yet contacted):
${newLeads.slice(0, 5).map(l => `- ${l.company} | Source: ${l.source} | Contacts: ${l.contact_count}`).join('\n') || 'None'}

FOLLOW-UPS DUE:
${pendingFollowups.slice(0, 5).map(l => `- ${l.company} | Sent: ${l.total_sent} emails | Source: ${l.source}`).join('\n') || 'None due'}

LEADS WITH REPLIES:
${allLeads.filter(l => l.has_reply).slice(0, 5).map(l => `- ${l.company} (replied!)`).join('\n') || 'No replies yet'}

RECENT ACTIVITY (last 10 events):
${recentActivity.slice(0, 10).map(a => `- ${a.icon} ${a.title}: ${a.detail}`).join('\n') || 'No recent activity'}

YOUR ROLE AS VOICE AGENT:
- Be the CEO's intelligent assistant — give direct, decisive answers
- Tell them who to email next, which leads to skip, what follow-ups are urgent
- Use real numbers from the data above (not generic advice)
- Recommend specific companies by name when relevant
- Keep answers SHORT — 2 to 4 sentences max (this is voice output)
- Sound like a sharp sales ops advisor, not a bot
- No markdown, no bullet points — plain spoken sentences only
- If asked to do something (send email, skip lead, etc.) explain you can show them how in the dashboard`;

  try {
    const r = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-sonnet-4-20250514',
      max_tokens: 350,
      system: systemPrompt,
      messages: [{ role: 'user', content: question }]
    }, {
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json'
      },
      timeout: 20000
    });

    const answer = r.data?.content?.[0]?.text || 'I could not generate a response.';
    await dbLog('🎙', 'Voice Agent', question.substring(0, 60));
    res.json({ ok: true, answer });
  } catch(e) {
    const errMsg = e.response?.data?.error?.message || e.message;
    await dbLog('❌', 'Voice Agent error', errMsg);
    res.json({ ok: false, error: errMsg });
  }
});

app.post('/api/agent/run',async(req,res)=>{const d=req.body?.dry_run===true;runAgentCycle({dryRun:d}).catch(console.error);res.json({ok:true,message:d?'Dry run started':'Agent started'});});
app.post('/api/agent/check-replies',async(req,res)=>res.json({ok:true,replies_found:await checkForReplies()}));
app.get('/api/test/hostinger',async(req,res)=>res.json(await testSmtp()));
app.get('/api/test/db',async(req,res)=>{
  try{const r=await turso('SELECT 1 as ok');res.json({ok:true,message:'Turso connected!',url:TURSO_URL});}
  catch(e){res.json({ok:false,error:e.message,url:TURSO_URL,hint:'Check TURSO_URL format — should be https://xxx.turso.io'});}
});
app.get('/api/test/gmail',async(req,res)=>res.json(await testSmtp())); // kept for backwards compat

// Hunter.io test — shows exact credits remaining
app.get('/api/test/hunter',async(req,res)=>{
  const key=process.env.HUNTER_API_KEY;
  if(!key) return res.json({ok:false,error:'HUNTER_API_KEY not set in Railway'});
  try{
    const r=await axios.get('https://api.hunter.io/v2/account',{params:{api_key:key},timeout:10000});
    const data = r.data?.data;
    const searches = data?.requests?.searches;
    res.json({
      ok:true,
      plan: data?.plan_name||'unknown',
      searches_used: searches?.used||0,
      searches_available: searches?.available||0,
      resets: 'Monthly',
      warning: (searches?.available||0)<5 ? '⚠️ Almost out of Hunter credits! Will stop finding emails.' : null
    });
  }catch(e){res.json({ok:false,error:e.response?.data?.errors?.[0]?.details||e.message});}
});

// Gemini test — verifies API key and generates a sample email
app.get('/api/test/gemini',async(req,res)=>{
  const key=process.env.GEMINI_API_KEY;
  if(!key) return res.json({ok:false,error:'GEMINI_API_KEY not set in Railway'});
  try{
    const r=await axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${key}`,
      {contents:[{parts:[{text:'Write a 2-sentence cold email subject line for an IT outsourcing company called ByteOn Technologies reaching out to Stripe who is hiring a remote React developer. Respond ONLY with JSON: {"subject":"...","body":"..."}'}]}]},
      {headers:{'Content-Type':'application/json'},timeout:20000}
    );
    const raw=r.data?.candidates?.[0]?.content?.parts?.[0]?.text||'{}';
    const parsed=JSON.parse(raw.replace(/```json|```/g,'').trim());
    res.json({ok:true, message:'Gemini working!', model:'gemini-1.5-flash', sample_subject:parsed.subject, sample_body:parsed.body});
  }catch(e){
    res.json({ok:false, error:e.response?.data?.error?.message||e.message});
  }
});

// CSV upload endpoint
app.post('/api/import/csv', upload.single('file'), async(req,res)=>{
  if(!req.file) return res.status(400).json({error:'No file uploaded'});
  try{
    const result = await importFromCSV(req.file.buffer);
    res.json({ok:true,...result});
  }catch(e){
    res.json({ok:false,error:e.message});
  }
});

// Hunter enrichment endpoint
app.post('/api/scrape',async(req,res)=>{
  // Respond immediately — scraper runs in background
  res.json({ok:true,message:'Scraper started — check Activity Log for results'});
  scrapeAllJobBoards().catch(e=>dbLog('❌','Scraper error',e.message));
});

// GET test for Serper Search — open in browser to test
app.get('/api/test/google',async(req,res)=>{
  const key = process.env.SERPER_API_KEY;
  if(!key) return res.json({ok:false,error:'SERPER_API_KEY not set in Railway'});
  try{
    const r = await axios.post('https://google.serper.dev/search',
      { q:'remote React developer contract hiring USA', num:5, gl:'us' },
      { headers:{ 'X-API-KEY': key, 'Content-Type':'application/json' }, timeout:15000 }
    );
    const items = r.data?.organic||[];
    res.json({
      ok:true,
      message:`Serper working! Found ${items.length} results. NOTE: this used 1 credit.`,
      credits_remaining: r.data?.credits||'check serper.dev dashboard',
      sample: items.slice(0,3).map(i=>({title:i.title,link:i.link,snippet:i.snippet?.substring(0,100)}))
    });
  }catch(e){
    res.json({ok:false,error:e.response?.data?.message||e.message, details:e.response?.data});
  }
});
app.post('/api/scrape/indeed',async(req,res)=>{try{const r=await scrapeIndeed();res.json({ok:true,found:r.length});}catch(e){res.json({ok:false,error:e.message});}});
app.post('/api/scrape/google',async(req,res)=>{try{const r=await scrapeGoogleCustom();res.json({ok:true,found:r.length});}catch(e){res.json({ok:false,error:e.message});}});
app.post('/api/scrape/yc',async(req,res)=>{try{const r=await scrapeYC();res.json({ok:true,found:r.length});}catch(e){res.json({ok:false,error:e.message});}});
app.post('/api/hunter/enrich',async(req,res)=>{
  try{const found=await enrichContactsWithHunter();res.json({ok:true,emails_found:found});}
  catch(e){res.json({ok:false,error:e.message});}
});

// ── Waalaxy Webhook ──────────────────────────────────────────────────────
// When someone accepts your LinkedIn connection, Waalaxy fires this webhook
// The agent instantly adds them as a lead and starts the email sequence
app.post('/webhook/waalaxy', async(req,res)=>{
  try{
    const data = req.body;
    await dbLog('🔗','Waalaxy webhook received', JSON.stringify(data).substring(0,100));

    // Waalaxy sends prospect data in various formats — handle all of them
    const prospects = data.prospects || data.leads || (Array.isArray(data)?data:[data]);

    let added = 0;
    for(const p of prospects){
      // Extract fields from Waalaxy prospect object
      const firstName  = p.firstName||p.first_name||p.firstname||'';
      const lastName   = p.lastName||p.last_name||p.lastname||'';
      const name       = p.name||`${firstName} ${lastName}`.trim()||'Unknown';
      const company    = p.company||p.companyName||p.organization||'';
      const role       = p.occupation||p.title||p.position||p.jobTitle||'';
      const email      = p.email||p.emailAddress||'';
      const linkedin   = p.linkedinUrl||p.linkedin||p.profileUrl||'';
      const website    = p.companyWebsite||p.website||'';
      const location   = p.location||p.city||'';

      if(!company && !name){ continue; }

      // Find or create the lead (company)
      const coName = company || `${name}'s Company`;
      const leadR = await db.execute({sql:'SELECT * FROM leads WHERE LOWER(TRIM(company))=LOWER(TRIM(?))',args:[coName]});
      let lead = leadR.rows[0]||null; if(lead) lead={...lead,id:Number(lead.id)};

      if(!lead){
        const r = await insertLead({
          company:coName, website, industry:null,
          size:null, location, source:'waalaxy',
          notes:`LinkedIn connection accepted via Waalaxy`
        });
        lead = {id: r.lastInsertRowid, company: coName};
        added++;
      }

      // Add contact if not already there
      const existsC = await db.execute({sql:'SELECT id FROM contacts WHERE lead_id=? AND name=?',args:[lead.id,name]});
      const exists = existsC.rows[0]||null;
      if(!exists){
        await insertContact({
          lead_id:lead.id, name, first_name:firstName||name.split(' ')[0]||'there',
          role, email:email||null, linkedin
        });
      }

      // If email provided by Waalaxy — start sequence immediately
      if(email){
        await dbLog('📤','Waalaxy lead ready to email',`${name} at ${coName} — ${email}`);
      } else {
        await dbLog('🔍','Waalaxy lead needs email',`${name} at ${coName} — will search via Hunter`);
        // Try Hunter.io to find email
        if(process.env.HUNTER_API_KEY && website){
          const domain = website.replace(/^https?:\/\//,'').replace(/\/.*/,'');
          const hunterEmail = await findEmailHunter(firstName, lastName, domain);
          if(hunterEmail){
            const contactR = await db.execute({sql:'SELECT * FROM contacts WHERE lead_id=? AND name=?',args:[lead.id,name]});
          const contact = contactR.rows[0]||null;
            if(contact) updateContactEmail(contact.id, hunterEmail);
            await dbLog('🎯','Email found via Hunter',`${name} → ${hunterEmail}`);
          }
        }
      }
    }

    await dbLog('✅','Waalaxy webhook processed',`${added} new companies added`);
    res.json({ok:true, added, message:'Prospects received and queued for outreach'});

  }catch(e){
    await dbLog('❌','Waalaxy webhook error',e.message);
    res.json({ok:false, error:e.message});
  }
});

// Waalaxy webhook test
app.get('/webhook/waalaxy/test',(req,res)=>{
  res.json({
    ok:true,
    message:'Waalaxy webhook is ready!',
    webhook_url:`${req.protocol}://${req.get('host')}/webhook/waalaxy`,
    instructions:'Copy the webhook_url above and paste it into Waalaxy → Settings → Webhooks'
  });
});


const PORT=process.env.PORT||3000;
app.listen(PORT,'0.0.0.0',async()=>{
  console.log(`\n🚀 LeadForge running on port ${PORT}`);
  console.log(`📋 CSV upload: POST /api/import/csv`);
  console.log(`🎯 Hunter enrich: POST /api/hunter/enrich`);
  console.log(`✅ Tests: /api/test/hostinger /api/test/hunter /api/test/gemini /api/test/google\n`);
  try{
    await initDB();
    startScheduler();
    console.log('[🟢] Server fully ready');
  }catch(e){
    console.error('❌ Turso DB error:',e.message);
    console.error('⚠️  Server running but DB unavailable — check TURSO_URL and TURSO_TOKEN in Railway variables');
  }
});
