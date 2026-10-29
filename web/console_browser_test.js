'use strict';
// Run with NODE_PATH pointing to an installed Playwright runtime; no package install needed.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const {chromium} = require('playwright');
let browser,qaTitle='',qaSequence=0;
test.beforeEach(t=>{qaTitle=t.name;});
test.before(async()=>{browser=await chromium.launch({...(process.env.CHROME_PATH?{executablePath:process.env.CHROME_PATH}:{}),headless:true});});
test.after(async()=>{if(browser)await browser.close();});
const apps={hub:'/hub/',upstream:'/mock/upstream/',platform1:'/mock/p1/',platform2:'/mock/p2/'};
async function open(app,hash='',respond=()=>({items:[]})){
 const context=await browser.newContext(),page=await context.newPage(),calls=[],errors=[];
 page.setDefaultTimeout(1800);
 page.on('pageerror',e=>errors.push(e.message));
 await page.route('http://localhost/**',async route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.pathname===apps[app])return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(__dirname,app,'index.html'),'utf8').replace('/*CONSOLE_CSS*/',fs.readFileSync(path.join(__dirname,'console.css'),'utf8')).replace('/*CONSOLE_JS*/',fs.readFileSync(path.join(__dirname,'console.js'),'utf8')).replace('/*APP_JS*/',fs.readFileSync(path.join(__dirname,app+'-extra.js'),'utf8'))});
  calls.push({url,method:request.method(),headers:request.headers(),body:request.postData()});
  const result=await respond(url,request);
  return route.fulfill({status:result?.httpStatus||200,headers:result?.headers||{},contentType:'application/json',body:result?.rawBody===undefined?JSON.stringify(result?.data||result):result.rawBody});
 });
 await page.goto('http://localhost'+apps[app]+hash);
 await page.locator('#token').fill('test-only-token');
 return {page,calls,errors,close:async()=>{
  try{if(process.env.QA_SCREENSHOT_DIR){
   fs.mkdirSync(process.env.QA_SCREENSHOT_DIR,{recursive:true});
   const name='edge-'+String(++qaSequence).padStart(3,'0')+'-'+app;
   await page.screenshot({path:path.join(process.env.QA_SCREENSHOT_DIR,name+'.png'),fullPage:true,mask:[page.locator('#token')]});
   fs.appendFileSync(path.join(process.env.QA_SCREENSHOT_DIR,'index.jsonl'),JSON.stringify({test:qaTitle,path:name+'.png'})+'\n');
  }}finally{await context.close();}
 }};
}
test('each independent application restores its own labelled navigation on refresh',async()=>{
 for(const [app,view,label] of [['hub','logs','诊断日志'],['upstream','results','测试记录'],['platform1','binding','绑定管理'],['platform2','controls','测试控制']]){
  const p=await open(app,'#'+view);
  try{assert.equal(await p.page.locator('[data-view="'+view+'"]').isVisible(),true);assert.match(await p.page.locator('[aria-current="page"]').innerText(),new RegExp(label));await p.page.reload();assert.equal(await p.page.locator('[data-view="'+view+'"]').isVisible(),true);await p.page.setViewportSize({width:640,height:900});assert.match(await p.page.locator('aside').innerText(),new RegExp(label));assert.deepEqual(p.errors,[]);}finally{await p.close();}
 }
});
test('message filters and next page preserve exact trace, status, platform and client',async()=>{
 const p=await open('hub','#messages',()=>({items:[{msgId:'message-1',traceId:'duplicate-trace',clientId:'000123',status:'RECOVERY_PENDING',platformResults:[]}],nextCursor:'next-cursor'}));
 try{await p.page.locator('#trace-filter').fill('duplicate-trace');await p.page.locator('#status-filter').selectOption('RECOVERY_PENDING');await p.page.locator('#platform').selectOption('PLATFORM_B');await p.page.locator('#client').fill('000123');await p.page.locator('#query').click();await p.page.locator('#next').waitFor({state:'visible'});await p.page.waitForFunction(()=>!document.getElementById('next').disabled);await p.page.locator('#next').click();await p.page.waitForFunction(()=>!document.getElementById('query').disabled);const calls=p.calls.filter(c=>c.url.pathname.endsWith('/messages'));assert.equal(calls.length,2);assert.equal(calls[1].url.searchParams.get('traceId'),'duplicate-trace');assert.equal(calls[1].url.searchParams.get('status'),'RECOVERY_PENDING');assert.equal(calls[1].url.searchParams.get('platformId'),'PLATFORM_B');assert.equal(calls[1].url.searchParams.get('clientId'),'000123');assert.equal(calls[1].url.searchParams.get('cursor'),'next-cursor');assert.ok(p.calls.every(c=>c.method==='GET'));assert.match(await p.page.locator('#message-table').innerText(),/duplicate-trace/);}finally{await p.close();}
});
test('metrics show absent and stale values honestly and never accept unsafe Grafana destinations',async()=>{
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{grafanaUrl:'javascript:alert(1)'}:{sampledAt:'2026-10-26T10:00:00Z',window:'15m',metrics:[{key:'ingress_rate',source:'prometheus',status:'unavailable',samples:[]},{key:'archive_pending',source:'prometheus',status:'stale',samples:[{labels:{},value:7,sampledAt:'2026-10-26T09:58:00Z'}]}]});
 try{await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>document.getElementById('metric-overview').textContent.includes('7'));const text=await p.page.locator('#metric-overview').innerText();assert.match(text,/不可用|暂无样本/);assert.match(text,/过期/);assert.equal(await p.page.locator('#grafana-link').getAttribute('href'),null);assert.match(await p.page.locator('#metric-trend').innerText(),/暂无趋势/);assert.ok(p.calls.every(c=>!c.url.searchParams.has('query')));}finally{await p.close();}
});
test('diagnostics distinguish rejected input and unreachable instances from persisted messages',async()=>{
 const p=await open('hub','#logs',()=>({items:[{id:'log-1',time:'2026-10-26T10:00:00Z',level:'WARN',instance:'hub-1',bootId:'boot-a',kind:'VALIDATION_REJECTED',message:'<img src=x onerror=alert(1)>',requestId:'req-1'}],instances:[{instance:'hub-1',status:'ok',bootId:'boot-a',capacity:1000,retained:1,dropped:2},{instance:'hub-2',status:'unavailable'}],retention:{capacityPerInstance:1000,persistent:false}}));
 try{await p.page.locator('#log-q').fill('req-1');await p.page.locator('#log-level').selectOption('WARN');await p.page.locator('#query-logs').click();await p.page.waitForFunction(()=>document.getElementById('diagnostics').textContent.includes('req-1'));assert.match(await p.page.locator('#diagnostics').innerText(),/<img/);assert.equal(await p.page.locator('#diagnostics img').count(),0);assert.match(await p.page.locator('#diagnostic-retention').innerText(),/hub-2.*不可达/s);assert.match(await p.page.locator('#diagnostic-retention').innerText(),/重启/);assert.equal(p.calls.at(-1).url.searchParams.get('q'),'req-1');assert.equal(p.calls.at(-1).url.searchParams.get('level'),'WARN');}finally{await p.close();}
});
test('pending work includes attention deliveries even when aggregate message is recovery pending',async()=>{
 const p=await open('hub','#attention',url=>url.pathname.endsWith('/deliveries')?{items:[{deliveryId:'attention-delivery',msgId:'mixed-message',currentState:'NEEDS_ATTENTION',platformId:'PLATFORM_A'}]}:{items:[]});
 try{await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>document.getElementById('attention-deliveries').textContent.includes('attention-delivery'));assert.equal(p.calls.find(c=>c.url.pathname.endsWith('/deliveries')).url.searchParams.get('currentState'),'NEEDS_ATTENTION');assert.equal(p.calls.find(c=>c.url.pathname.endsWith('/messages')).url.searchParams.get('routeStatus'),'NEEDS_ATTENTION');assert.match(await p.page.locator('#attention-deliveries').innerText(),/待人工处理/);}finally{await p.close();}
});
test('message timeline uses real timestamps, paginates attempts and labels related receipts as nonunique',async()=>{
 const p=await open('hub','#chain',url=>{
  if(url.pathname.endsWith('/messages/message-1'))return {msgId:'message-1',traceId:'repeat-trace',clientId:'000123',receivedAt:'2026-10-26T10:00:00Z',storedAt:'2026-10-26T10:00:02Z',routeStatus:'ROUTED',status:'SUCCEEDED',routeSnapshot:{resolvedAt:'2026-10-26T10:00:01Z'},platformResults:[{deliveryId:'delivery-1',platformId:'PLATFORM_B',platformUid:'fixed-uid',firstOutcome:'SUCCESS',currentState:'SUCCEEDED'}],manualOperations:[]};
  if(url.pathname.endsWith('/attempts'))return {items:[{attemptNo:1,startedAt:'2026-10-26T10:00:01Z',finishedAt:'2026-10-26T10:00:01.010Z',outcome:'SUCCESS',sendStarted:true,cycleNo:1}],nextCursor:url.searchParams.has('cursor')?'':'attempt-page-2'};
  if(url.pathname.endsWith('/received'))return {items:[{receiveId:'r1',userUid:'fixed-uid',traceId:'repeat-trace',receivedAt:'2026-10-26T10:00:03Z',payload:{traceId:'repeat-trace'}}]};return {items:[]};
 });
 try{await p.page.locator('#chain-id').fill('message-1');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-detail').textContent.includes('fixed-uid'));assert.match(await p.page.locator('#chain-detail').innerText(),/数据库插入/);assert.match(await p.page.locator('#chain-detail').innerText(),/未记录/);await p.page.locator('[data-attempt-next]').click();await p.page.waitForFunction(()=>!document.querySelector('[data-attempt-next]'));assert.equal(p.calls.filter(c=>c.url.pathname.endsWith('/attempts')).length,2);await p.page.locator('[data-related]').click();await p.page.waitForFunction(()=>document.getElementById('related-receipts').textContent.includes('r1'));const call=p.calls.find(c=>c.url.pathname.endsWith('/received'));assert.equal(call.url.pathname,'/mock/p2/demo/received');assert.equal(call.url.searchParams.get('uid'),'fixed-uid');assert.equal(call.url.searchParams.get('traceId'),'repeat-trace');assert.match(await p.page.locator('#related-receipts').innerText(),/不能唯一/);}finally{await p.close();}
});
test('platform B consumer control reads state before explicit pause and does not equate broker ACK with consumption',async()=>{
 const p=await open('platform2','#controls',(_url,request)=>({paused:request.method()==='POST'}));
 try{await p.page.locator('#load-consumer').click();await p.page.waitForFunction(()=>!document.getElementById('pause-consumer').disabled);await p.page.locator('#pause-consumer').click();await p.page.waitForFunction(()=>document.getElementById('consumer-status').textContent.includes('已暂停'));const call=p.calls.find(c=>c.method==='POST');assert.equal(call.url.pathname,'/mock/p2/demo/consumer');assert.deepEqual(JSON.parse(call.body),{paused:true});assert.equal(call.headers['x-demo-token'],'test-only-token');assert.match(await p.page.locator('[data-view="controls"]').innerText(),/Broker/);}finally{await p.close();}
});
test('related receipt links prefill fixed identifiers in each independent platform',async()=>{
 for(const app of ['platform1','platform2']){const p=await open(app,'?uid=uid-123&traceId=repeat-trace#receipts');try{assert.equal(await p.page.locator('#uid').inputValue(),'uid-123');assert.equal(await p.page.locator('#trace').inputValue(),'repeat-trace');assert.equal(p.calls.length,0,'opening a query link must not write or auto submit');}finally{await p.close();}}
});
test('changing the trend window never relabels an old snapshot as the newly selected window',async()=>{
 let fail=false;const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{grafanaUrl:'https://grafana.example/d/hub-overview'}:fail?{httpStatus:503,data:{code:'QUERY_UNAVAILABLE'}}:{sampledAt:'2026-10-26T10:00:00Z',window:'15m',metrics:[{key:'ingress_rate',source:'prometheus',status:'ok',samples:[{labels:{status:'COMPLETED'},value:4,sampledAt:'2026-10-26T10:00:00Z'}]}]});
 try{await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>document.getElementById('metric-overview').textContent.includes('4'));fail=true;await p.page.locator('#metric-window').selectOption('6h');await p.page.waitForFunction(()=>document.getElementById('metrics-status').textContent.includes('刷新失败'));assert.match(await p.page.locator('#metrics-status').innerText(),/15m/);assert.match(await p.page.locator('#metric-overview').innerText(),/过期/);assert.equal(await p.page.locator('#grafana-link').getAttribute('href'),'https://grafana.example/d/hub-overview');}finally{await p.close();}
});
test('failed consumer control prevents another write until the real state is read again',async()=>{
 const p=await open('platform2','#controls',(_url,request)=>request.method()==='POST'?{httpStatus:503,data:{code:'CONTROL_UNCONFIRMED'}}:{paused:false});
 try{await p.page.locator('#load-consumer').click();await p.page.waitForFunction(()=>!document.getElementById('pause-consumer').disabled);await p.page.locator('#pause-consumer').click();await p.page.waitForFunction(()=>document.getElementById('consumer-status').textContent.includes('未确认'));assert.equal(await p.page.locator('#pause-consumer').isDisabled(),true);assert.equal(await p.page.locator('#resume-consumer').isDisabled(),true);assert.equal(p.calls.filter(c=>c.method==='POST').length,1);}finally{await p.close();}
});
test('all 18 menus switch independent content, keep labels and do not write on navigation',async()=>{
 const menus={hub:['overview','messages','chain','infra','logs','bindings','attention'],upstream:['generate','results','guide'],platform1:['receipts','binding','controls','guide'],platform2:['receipts','binding','controls','guide']};
 for(const [app,views] of Object.entries(menus)){const p=await open(app);try{for(const view of views){await p.page.locator('[data-nav="'+view+'"]').click();await p.page.locator('[data-view="'+view+'"]').waitFor({state:'visible'});assert.equal(await p.page.locator('[data-view]:visible').count(),1);assert.equal(await p.page.locator('[data-nav="'+view+'"]').getAttribute('aria-current'),'page');}assert.ok(p.calls.every(c=>c.method==='GET'));assert.deepEqual(p.errors,[]);if(process.env.CAPTURE_CONSOLE_IMAGES){await p.page.locator('[data-nav="'+views[0]+'"]').click();await p.page.locator('[data-view="'+views[0]+'"]').waitFor({state:'visible'});await p.page.screenshot({path:path.join(__dirname,'../docs/superpowers/reports/partner-hub-mvp/ui-frontend-'+app+'.png'),fullPage:true});}}finally{await p.close();}}
});
test('upstream console keeps header IDs after lost body and navigates to read-only verification',async()=>{
 const p=await open('upstream','#generate',(_url,request)=>request.method()==='POST'?{headers:{'X-Demo-Msg-Ids':'8a236f08-1bf4-4bb3-80c2-7eb27fe7c63f'},rawBody:'not-json'}:{httpStatus:404,data:{code:'RECORD_NOT_AVAILABLE'}});
 try{await p.page.locator('#submit').click();await p.page.locator('[data-view="results"]').waitFor({state:'visible'});assert.match(await p.page.locator('#items').innerText(),/8a236f08/);assert.match(await p.page.locator('#known-ids').inputValue(),/8a236f08/);await p.page.locator('#refresh').click();await p.page.waitForFunction(()=>!document.getElementById('refresh').disabled);assert.equal(p.calls.filter(c=>c.method==='POST').length,1);assert.match(await p.page.locator('#items').innerText(),/不要重提/);assert.match(await p.page.locator('#items a').getAttribute('href'),/msgId=8a236f08.*#chain/);assert.deepEqual(p.errors,[]);}finally{await p.close();}
});
test('platform binding console shows published separately and escapes the returned event data',async()=>{
 const p=await open('platform1','#binding',()=>({httpStatus:202,data:{eventId:'<img src=x onerror=alert(1)>',status:'PUBLISHED'}}));
 try{await p.page.locator('#clientId').fill('000123');await p.page.locator('#platformUid').fill('A-123');await p.page.locator('#submit').click();await p.page.waitForFunction(()=>document.getElementById('bindingStatus').textContent.includes('PUBLISHED'));assert.match(await p.page.locator('#bindingStatus').innerText(),/尚未确认生效/);assert.match(await p.page.locator('#binding-facts').innerText(),/<img/);assert.equal(await p.page.locator('#binding-facts img').count(),0);assert.equal(p.calls.filter(c=>c.method==='POST').length,1);const op=JSON.parse(p.calls[0].body);assert.equal(op.clientId,'000123');assert.equal(op.platformUid,'A-123');assert.equal(op.eventType,'BIND');assert.deepEqual(p.errors,[]);}finally{await p.close();}
});
test('late related receipts cannot overwrite a newly selected message chain',async()=>{
 let release;const pending=new Promise(resolve=>{release=resolve;});
 const p=await open('hub','#chain',async url=>{
  if(url.pathname.endsWith('/received')){await pending;return {items:[{receiveId:'old-chain-receipt',userUid:'old-uid',traceId:'old-trace',receivedAt:'2026-10-26T10:00:00Z'}]};}
  if(url.pathname.includes('/messages/'))return {msgId:url.pathname.split('/').at(-1),traceId:'old-trace',routeStatus:'ROUTED',platformResults:[{deliveryId:'d',platformId:'PLATFORM_A',platformUid:'old-uid',currentState:'SUCCEEDED'}],manualOperations:[]};return {items:[]};
 });
 try{await p.page.locator('#chain-id').fill('message-old');await p.page.locator('#load-chain').click();await p.page.locator('[data-related]').waitFor();await p.page.locator('[data-related]').click();await p.page.locator('#chain-id').fill('message-new');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-detail').textContent.includes('message-new'));release();await p.page.waitForTimeout(100);assert.doesNotMatch(await p.page.locator('#related-receipts').innerText(),/old-chain-receipt/);}finally{release();await p.close();}
});
test('attempt facts render MySQL 0/1 and boolean explicitly while preserving unknown values',async()=>{
 const values=[1,0,true,false,null,undefined,2,'0'];
 const p=await open('hub','#chain',url=>url.pathname.includes('/messages/')?{msgId:'message-1',routeStatus:'ROUTED',platformResults:[{deliveryId:'delivery-1',platformId:'PLATFORM_A',platformUid:'uid'}],manualOperations:[]}:{items:values.map((sendStarted,i)=>({attemptNo:i+1,cycleNo:1,sendStarted,outcome:'UNKNOWN'}))});
 try{await p.page.locator('#chain-id').fill('message-1');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.querySelectorAll('#attempts-detail tbody tr').length===8);const actual=await p.page.locator('#attempts-detail tbody tr').evaluateAll(rows=>rows.map(row=>row.children[3].textContent));assert.deepEqual(actual,['是','否','是','否','未记录','未记录','未记录','未记录']);}finally{await p.close();}
});
for(const failedBranch of ['deliveries','routes'])for(const first of ['failure','success']){
 test('attention '+failedBranch+' failure stays visible when '+first+' completes first and old data is retained',async()=>{
  let phase=0,releaseFailure,releaseSuccess;const failureGate=new Promise(r=>{releaseFailure=r;}),successGate=new Promise(r=>{releaseSuccess=r;});
  const p=await open('hub','#attention',async url=>{const branch=url.pathname.endsWith('/deliveries')?'deliveries':'routes';if(phase){await (branch===failedBranch?failureGate:successGate);if(branch===failedBranch)return {httpStatus:503,data:{code:'QUERY_UNAVAILABLE'}};}return {items:branch==='deliveries'?[{deliveryId:'old-delivery',msgId:'mixed-message',currentState:'NEEDS_ATTENTION',platformId:'PLATFORM_A'}]:[{msgId:'old-route',routeStatus:'NEEDS_ATTENTION',status:'RECOVERY_PENDING'}],nextCursor:'page-2'};});
  try{await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>!document.getElementById('load-attention').disabled);phase=1;await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>document.getElementById('load-attention').disabled);(first==='failure'?releaseFailure:releaseSuccess)();await p.page.waitForTimeout(50);(first==='failure'?releaseSuccess:releaseFailure)();await p.page.waitForFunction(()=>!document.getElementById('load-attention').disabled);const label=failedBranch==='deliveries'?'投递':'路由',target=failedBranch==='deliveries'?'attention-deliveries':'attention-routes';assert.match(await p.page.locator('#attention-status').innerText(),new RegExp(label+'查询失败'));assert.match(await p.page.locator('#'+target+'-status').innerText(),/过期/);assert.match(await p.page.locator('#'+target).innerText(),/old-/);assert.equal(await p.page.locator('#'+(failedBranch==='deliveries'?'attention-delivery-next':'attention-route-next')).isDisabled(),false);}finally{releaseFailure();releaseSuccess();await p.close();}
 });
}
test('first attention branch failure is not presented as no pending work and failed next page can retry the same cursor',async()=>{
 let phase=0;const p=await open('hub','#attention',url=>url.pathname.endsWith('/messages')?{items:[]}:phase===0?{httpStatus:503,data:{code:'QUERY_UNAVAILABLE'}}:phase===2?{httpStatus:503,data:{code:'QUERY_UNAVAILABLE'}}:{items:[{deliveryId:'kept-delivery',msgId:'m',currentState:'NEEDS_ATTENTION'}],nextCursor:'same-cursor'});
 try{await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>!document.getElementById('load-attention').disabled);assert.match(await p.page.locator('#attention-deliveries-status').innerText(),/未取得.*不代表无待处理/);phase=1;await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>!document.getElementById('load-attention').disabled);phase=2;await p.page.locator('#attention-delivery-next').click();await p.page.waitForFunction(()=>!document.getElementById('attention-delivery-next').disabled);assert.match(await p.page.locator('#attention-deliveries').innerText(),/kept-delivery/);phase=3;await p.page.locator('#attention-delivery-next').click();await p.page.waitForFunction(()=>!document.getElementById('attention-delivery-next').disabled);const more=p.calls.filter(c=>c.url.searchParams.has('cursor'));assert.equal(more.length,2);assert.equal(more[0].url.searchParams.get('cursor'),'same-cursor');assert.equal(more[1].url.searchParams.get('cursor'),'same-cursor');assert.doesNotMatch(await p.page.locator('#attention-status').innerText(),/查询失败/);}finally{await p.close();}
});
test('resource tabs show sampling success and age for their own middleware with Chinese names and units',async()=>{
 const p=await open('hub','#infra',url=>url.pathname.endsWith('/config')?{grafanaUrl:''}:{window:'15m',sampledAt:'2026-10-26T10:00:00Z',metrics:[{key:'resource_up',source:'prometheus',status:'ok',samples:[{labels:{kind:'mysql',instance:'hub-1'},value:1,sampledAt:'2026-10-26T10:00:00Z'},{labels:{kind:'redis',instance:'hub-1'},value:0,sampledAt:'2026-10-26T10:00:00Z'}]},{key:'resource_age',source:'prometheus',status:'ok',samples:[{labels:{kind:'mysql',instance:'hub-1'},value:2,sampledAt:'2026-10-26T10:00:00Z'},{labels:{kind:'redis',instance:'hub-1'},value:9,sampledAt:'2026-10-26T10:00:00Z'}]},{key:'mysql_buffer_pool_bytes',source:'prometheus',status:'ok',samples:[{labels:{},value:1024,sampledAt:'2026-10-26T10:00:00Z'}]}]});
 try{await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);await p.page.locator('[data-infra="mysql"]').click();const text=await p.page.locator('#infra-metrics').innerText();assert.match(text,/资源采样成功/);assert.match(text,/采样年龄（秒）/);assert.match(text,/数据占用（字节）/);assert.match(text,/MySQL/);assert.match(text,/中台实例 1/);assert.doesNotMatch(text,/Redis/);await p.page.locator('#infra-metrics summary').first().click();assert.match(await p.page.locator('#infra-metrics').innerText(),/kind.*mysql/s);}finally{await p.close();}
});

async function hubSnapshot(page,name,width=1440){
 await page.setViewportSize({width,height:1000});
 await page.evaluate(async()=>{await document.fonts.ready;window.scrollTo(0,0);await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));});
 await page.waitForTimeout(150);
 const output=process.env.QA_SCREENSHOT_DIR||path.join(__dirname,'../docs/superpowers/reports/partner-hub-mvp');fs.mkdirSync(output,{recursive:true});
 await page.screenshot({path:path.join(output,'hub-'+name+'.png'),fullPage:true,mask:[page.locator('#token')]});
}
test('hub QA metrics separate readable scope and dimensions, keep every sample and avoid header overlap',async()=>{
 const samples=Array.from({length:18},(_,i)=>({labels:{instance:'hub-'+(i+1),platform:'PLATFORM_B',outcome:'PERMANENT_FAILURE',kind:'mysql',custom_zone:'zone-'+i},value:i,sampledAt:'2026-10-27T08:00:00Z'}));
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{}:{window:'15m',metrics:[{key:'ingress_rate',status:'ok',samples},{key:'recovery_tasks',status:'stale',samples},{key:'first_rate',status:'ok',samples},{key:'resource_up',status:'unavailable',samples:[]}]});
 try{await p.page.setViewportSize({width:1440,height:1100});await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-metrics').disabled);assert.match(await p.page.locator('#metric-business thead').textContent(),/实例.*采集对象.*业务维度.*采样状态/s);await p.page.locator('#business-raw>summary').click();const row=await p.page.locator('#metric-business tbody tr').filter({hasText:'平台首轮速率'}).first().innerText();assert.match(row,/中台实例 1/);assert.match(row,/平台 B/);assert.match(row,/永久失败/);assert.match(row,/custom_zone.*zone-0/s);assert.match(row,/采样正常/);const card=p.page.locator('#metric-overview article').first(),body=card.locator('.metric-samples'),header=card.locator('h3');assert.equal(await body.locator('.metric-sample').count(),18);const before=await header.boundingBox();await body.evaluate(e=>e.scrollTop=e.scrollHeight/2);const after=await header.boundingBox(),bodyBox=await body.boundingBox();assert.equal(before.y,after.y);assert.ok(after.y+after.height<=bodyBox.y,'header must be outside the scrollable samples');await body.evaluate(e=>e.scrollTop=e.scrollHeight);assert.match(await body.innerText(),/中台实例 18/);const healthName=await p.page.locator('#metric-health tbody td').first().boundingBox();assert.ok(healthName.width>=110,'health metric names must have enough width to stay readable');const button=await p.page.locator('#refresh-metrics').boundingBox(),select=await p.page.locator('#metric-window').boundingBox();assert.ok(Math.abs(button.y+button.height-select.y-select.height)<2,'controls must align at bottom');assert.match(await p.page.locator('#metric-overview').innerText(),/样本已过期/);if(process.env.CAPTURE_HUB_IMAGES){await hubSnapshot(p.page,'overview-scroll');await p.page.setViewportSize({width:640,height:1000});const narrowHeader=await header.boundingBox(),narrowBody=await body.boundingBox();assert.ok(narrowHeader.y+narrowHeader.height<=narrowBody.y);assert.equal(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await hubSnapshot(p.page,'overview-narrow',640);}await p.page.locator('[data-nav="infra"]').click();await p.page.locator('[data-infra="mysql"]').click();assert.match(await p.page.locator('#infra-metrics').innerText(),/采样不可用/);assert.deepEqual(p.errors,[]);}finally{await p.close();}
});
test('hub QA message platform results have independent Chinese badges and explicit routing facts',async()=>{
 const p=await open('hub','#messages',()=>({items:[{msgId:'mixed-cn',status:'RECOVERY_PENDING',routeStatus:'ROUTED',platformResults:[{platformId:'PLATFORM_A',platformUid:'uid-a',currentState:'SUCCEEDED'},{platformId:'PLATFORM_B',platformUid:'uid-b',currentState:'NEEDS_ATTENTION'}]},{msgId:'none-cn',status:'NO_ROUTE',routeStatus:'NO_ROUTE'},{msgId:'future-cn',status:'FUTURE_STATE',platformResults:[]}]}));
 try{await p.page.locator('#query').click();await p.page.waitForFunction(()=>document.getElementById('message-table').textContent.includes('mixed-cn'));const row=p.page.locator('#message-table tbody tr').first();assert.match(await row.innerText(),/恢复中/);assert.match(await row.innerText(),/路由.*已确定目标/s);const details=row.locator('.platform-details');assert.equal(await details.getAttribute('open'),null);assert.match(await row.innerText(),/投递.*投递成功.*1.*待人工处理.*1/s);const targets=row.locator('.platform-result');assert.equal(await targets.nth(0).isVisible(),false);assert.doesNotMatch(await row.innerText(),/uid-a|uid-b/);const before=p.calls.length;await details.locator('summary').click();assert.equal(p.calls.length,before);assert.equal(await targets.count(),2);assert.match(await targets.nth(0).innerText(),/平台 A.*投递成功.*uid-a/s);assert.match(await targets.nth(1).innerText(),/平台 B.*待人工处理.*uid-b/s);await details.locator('summary').click();assert.equal(await targets.nth(0).isVisible(),false);await details.locator('summary').focus();await p.page.keyboard.press('Enter');assert.equal(await targets.nth(0).isVisible(),true);assert.equal(p.calls.length,before);assert.match(await p.page.locator('#message-table tbody tr').nth(1).innerText(),/无投递目标/);assert.match(await p.page.locator('#message-table tbody tr').nth(2).innerText(),/未知状态.*FUTURE_STATE/s);if(process.env.CAPTURE_HUB_IMAGES){await hubSnapshot(p.page,'messages',1600);}assert.equal(await row.locator('[data-status="RECOVERY_PENDING"]').getAttribute('title'),'RECOVERY_PENDING');assert.equal(await p.page.locator('#status-filter option[value="NEEDS_ATTENTION"]').innerText(),'待人工处理');assert.ok(p.calls.every(c=>c.method==='GET'));}finally{await p.close();}
});
test('hub QA chain, bindings and pending work use Chinese states without changing request enums',async()=>{
 const p=await open('hub','#chain',url=>url.pathname.includes('/messages/')?{msgId:'cn-chain',firstStatus:'COMPLETED',status:'SUCCEEDED',routeStatus:'ROUTED',platformResults:[{deliveryId:'cn-delivery',platformId:'PLATFORM_B',platformUid:'uid-b',firstOutcome:'SUCCESS',currentState:'SUCCEEDED'}],manualOperations:[{operationId:'op-cn',state:'APPLIED',retryCycle:2}]}:url.pathname.endsWith('/attempts')?{items:[{cycleNo:1,attemptNo:1,outcome:'UNKNOWN'}]}:url.pathname.endsWith('/bindings')?{items:[{clientId:'1',platformId:'PLATFORM_A',status:'BOUND'},{clientId:'2',platformId:'PLATFORM_B',status:'UNBOUND'}]}:url.pathname.endsWith('/deliveries')?{items:[{deliveryId:'cn-attention',msgId:'cn-chain',currentState:'NEEDS_ATTENTION',platformId:'PLATFORM_A'}]}:{items:[]});
 try{await p.page.locator('#chain-id').fill('cn-chain');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-detail').textContent.includes('uid-b'));assert.match(await p.page.locator('#chain-detail').innerText(),/首轮完成/);assert.match(await p.page.locator('#chain-detail').innerText(),/投递成功/);assert.match(await p.page.locator('#chain-detail').innerText(),/已应用/);assert.match(await p.page.locator('#chain-detail').innerText(),/Broker.*确认.*消费/s);await p.page.waitForFunction(()=>document.getElementById('attempts-detail').textContent.includes('结果未确认'));await p.page.locator('[data-nav="bindings"]').click();await p.page.locator('#hub-load-bindings').click();await p.page.waitForFunction(()=>document.getElementById('hub-bindings').textContent.includes('已绑定'));assert.match(await p.page.locator('#hub-bindings').innerText(),/已解绑/);await p.page.locator('[data-nav="attention"]').click();await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>document.getElementById('attention-deliveries').textContent.includes('cn-attention'));assert.match(await p.page.locator('#attention-deliveries').innerText(),/平台 A.*待人工处理/s);assert.equal(p.calls.find(c=>c.url.pathname.endsWith('/deliveries')).url.searchParams.get('currentState'),'NEEDS_ATTENTION');assert.deepEqual(p.errors,[]);}finally{await p.close();}
});
test('hub QA diagnostics label correlation IDs, localize events and preserve boot identity safely',async()=>{
 const p=await open('hub','#logs',()=>({items:[{time:'2026-10-27T08:00:00Z',level:'WARN',instance:'hub-2',bootId:'full-boot-identity',kind:'ingress_invalid',message:'<img src=x onerror=alert(1)>',requestId:'req-cn',msgId:'msg-cn',traceId:'trace-cn',deliveryId:'delivery-cn',platform:'PLATFORM_B',outcome:'UNKNOWN',status:'REJECTED'}],instances:[{instance:'hub-2',status:'ok',bootId:'full-boot-identity'}]}));
 try{await p.page.locator('#query-logs').click();await p.page.waitForFunction(()=>document.getElementById('diagnostics').textContent.includes('req-cn'));const table=p.page.locator('#diagnostics');assert.match(await table.locator('thead').innerText(),/所属实例.*事件与摘要.*关联信息/s);assert.match(await table.innerText(),/校验拒绝/);assert.match(await table.innerText(),/请求标识.*req-cn.*消息标识.*msg-cn.*跟踪标识.*trace-cn.*投递标识.*delivery-cn/s);assert.match(await table.innerText(),/平台 B/);assert.match(await table.innerText(),/已拒绝/);await table.locator('summary').click();assert.match(await table.innerText(),/full-boot-identity/);assert.match(await table.innerText(),/ingress_invalid/);if(process.env.CAPTURE_HUB_IMAGES){await hubSnapshot(p.page,'diagnostics',1600);}assert.equal(await table.locator('img').count(),0);assert.match(await table.locator('a').getAttribute('href'),/msgId=msg-cn.*#chain/);assert.deepEqual(p.errors,[]);}finally{await p.close();}
});

test('hub platform summary keeps missing and unknown delivery states distinct and rows expand independently',async()=>{
 const p=await open('hub','#messages',()=>({items:[{msgId:'a',routeStatus:'ROUTED',status:'SUCCEEDED',platformResults:[{platformId:'PLATFORM_A',currentState:'SUCCEEDED',platformUid:'uid-one'},{platformId:'PLATFORM_B',currentState:'SUCCEEDED',platformUid:'uid-two'}]},{msgId:'b',routeStatus:'ROUTED',platformResults:[{platformId:'OTHER',currentState:'FUTURE_DELIVERY',platformUid:'<img src=x>'},{platformId:'PLATFORM_A'}]},{msgId:'c',routeStatus:'NO_ROUTE',platformResults:[]},{msgId:'d',platformResults:[]}]}));
 try{await p.page.locator('#query').click();await p.page.waitForFunction(()=>document.querySelectorAll('#message-table tbody tr').length===4);const rows=p.page.locator('#message-table tbody tr');assert.match(await rows.nth(0).innerText(),/投递成功.*2/);assert.match(await rows.nth(1).innerText(),/未知状态（FUTURE_DELIVERY）.*1.*未记录.*1/s);assert.match(await rows.nth(2).innerText(),/无投递目标.*本条不外发/s);assert.match(await rows.nth(3).innerText(),/未提供平台明细/);const before=p.calls.length;await rows.nth(0).locator('summary').click();assert.match(await rows.nth(0).innerText(),/uid-one.*uid-two.*Broker 已确认；消费另查/s);assert.equal(await rows.nth(1).locator('.platform-result').first().isVisible(),false);await rows.nth(1).locator('summary').click();assert.match(await rows.nth(1).innerText(),/<img src=x>/);assert.equal(await rows.nth(1).locator('img').count(),0);assert.equal(p.calls.length,before);assert.deepEqual(p.errors,[]);}finally{await p.close();}
});


// 2026-10-29 acceptance: same snapshot, truthful absence, readable chain facts.
test('resource dual views compare dimensions without merging pools or copying shared samples',async()=>{
 let phase=0;
 const sample=(instance,value,pool)=>({labels:{job:'hub',instance,...(pool?{pool}: {})},value,sampledAt:'2026-10-29T06:00:00Z'});
 const p=await open('hub','#infra',url=>url.pathname.endsWith('/config')?{}:{window:'15m',metrics:[
  {key:'cpu',status:'ok',source:'prometheus',samples:phase?[sample('hub-2',3)]:[sample('hub-1',1),sample('hub-2',2)]},
  {key:'db_open',status:'stale',source:'prometheus',samples:[sample('hub-1',11,'bindings'),sample('hub-1',22,'query'),sample('hub-2',33,'query')]},
  {key:'mysql_connections',status:'ok',source:'prometheus',samples:[{labels:{},value:77,sampledAt:'2026-10-29T06:00:00Z'}]}
 ]});
 try{
  await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);
  assert.match(await p.page.locator('#infra-comparison').innerText(),/中台实例 1.*中台实例 2/s);
  const rows=p.page.locator('#infra-comparison tbody tr');assert.equal(await rows.filter({hasText:'数据库打开连接'}).count(),2);
  assert.match(await rows.filter({hasText:'bindings'}).innerText(),/11.*未采集该实例/s);
  const calls=p.calls.length;
  await p.page.locator('[data-instance-link]').filter({hasText:'中台实例 1'}).click();
  assert.equal(await p.page.locator('#infra-instance').inputValue(),JSON.stringify(['hub','hub-1']));
  assert.match(await p.page.locator('#infra-detail').innerText(),/11.*22/s);
  assert.doesNotMatch(await p.page.locator('#infra-detail').innerText(),/\b33\b/);
  await p.page.locator('[data-infra-mode="compare"]').click();
  await p.page.locator('[data-infra="mysql"]').click();
  assert.equal(await p.page.locator('#infra-shared tbody tr').filter({hasText:'77'}).count(),1);
  assert.doesNotMatch(await p.page.locator('#infra-comparison').innerText(),/77/);
  await p.page.locator('[data-infra-mode="detail"]').click();assert.equal(p.calls.length,calls,'view changes must not sample');
  await p.page.locator('[data-infra="instances"]').click();phase=1;
  await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);
  assert.equal(await p.page.locator('#infra-instance').inputValue(),JSON.stringify(['hub','hub-1']));
  assert.match(await p.page.locator('#infra-detail').innerText(),/CPU.*未采集该实例/s);
  await p.page.setViewportSize({width:640,height:900});assert.equal(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('chain timeline separates visible facts from technical fields and summarizes known delivery results safely',async()=>{
 const p=await open('hub','#chain',url=>url.pathname.endsWith('/attempts')?{items:[
  {cycleNo:1,attemptNo:1,outcome:'SUCCESS',sendStarted:true,summary:'http_status=200; outcome=SUCCESS'},
  {cycleNo:1,attemptNo:2,outcome:'SUCCESS',sendStarted:true,summary:'broker write confirmed'},
  {cycleNo:1,attemptNo:3,outcome:'UNKNOWN',sendStarted:true,summary:'<img src=x onerror=alert(1)>'},
  {cycleNo:1,attemptNo:4,outcome:'UNKNOWN',sendStarted:true,summary:'http_status=200; outcome=SUCCESS'}
 ]}:{msgId:'m-new',receivedAt:'2026-10-29T06:00:00Z',storedAt:'2026-10-29T06:00:02Z',routeSnapshot:{resolvedAt:'2026-10-29T06:00:01Z'},platformResults:[{deliveryId:'d-new',platformId:'PLATFORM_B',currentState:'SUCCEEDED'}]});
 try{
  await p.page.locator('#chain-id').fill('m-new');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  assert.equal(await p.page.locator('.timeline .event').count(),3);
  assert.doesNotMatch(await p.page.locator('.timeline').innerText(),/receivedAt|resolvedAt|storedAt|HTTP 响应/);
  assert.equal(await p.page.locator('.timeline details[open]').count(),0);
  await p.page.locator('.timeline summary').first().click();assert.match(await p.page.locator('.timeline').innerText(),/receivedAt/);
  assert.match(await p.page.locator('#chain-detail').innerText(),/非精确.*提交时间/);
  const rows=p.page.locator('#attempts-detail tbody tr');assert.match(await rows.nth(0).innerText(),/接收端确认成功.*HTTP.*200/s);
  assert.match(await rows.nth(1).innerText(),/Broker 已确认写入.*消费.*另查/s);
  assert.match(await rows.nth(2).innerText(),/<img/);assert.equal(await p.page.locator('#attempts-detail img').count(),0);
  assert.doesNotMatch(await rows.nth(3).innerText(),/接收端确认成功/);
  await rows.nth(0).locator('summary').click();assert.match(await rows.nth(0).innerText(),/http_status=200; outcome=SUCCESS/);
  assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('metric empty reason distinguishes no observations from collection failure without inventing zero P99',async()=>{
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{}:{window:'15m',metrics:[
  {key:'ingress_rate',status:'ok',source:'prometheus',samples:[{labels:{status:'200'},value:0}]},
  {key:'first_p99',status:'unavailable',reason:'no_samples',source:'prometheus',samples:[]},
  {key:'archive_pending',status:'unavailable',reason:'query_failed',source:'prometheus',samples:[]}
 ]});
 try{await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-metrics').disabled);
  const cards=p.page.locator('#metric-overview article');assert.match(await cards.nth(0).innerText(),/HTTP.*200/s);assert.match(await cards.nth(1).innerText(),/暂无有效耗时样本/);assert.equal(await cards.nth(1).locator('.metric-value').count(),0);assert.match(await cards.nth(2).innerText(),/采样查询失败/);
 }finally{await p.close();}
});
test('attention separates delivery and routing queues into labelled cards with page scoped counts',async()=>{
 const p=await open('hub','#attention',url=>url.pathname.endsWith('/deliveries')?{items:[{deliveryId:'d-q',msgId:'m-q',platformId:'PLATFORM_A',currentState:'NEEDS_ATTENTION'}],nextCursor:'next'}:{items:[]});
 try{assert.equal(await p.page.locator('.attention-card').count(),2);assert.doesNotMatch(await p.page.locator('.attention-card h3').allTextContents().then(x=>x.join(' ')),/NEEDS_ATTENTION/);
  await p.page.locator('#load-attention').click();await p.page.waitForFunction(()=>!document.getElementById('load-attention').disabled);
  assert.match(await p.page.locator('.attention-card').nth(0).innerText(),/投递待人工处理.*当前页 1 条.*下一页/s);assert.match(await p.page.locator('.attention-card').nth(1).innerText(),/路由待人工处理.*当前页 0 条/s);
 }finally{await p.close();}
});

test('resource comparison retains a known healthy instance column when its category samples disappear',async()=>{
 const p=await open('hub','#infra',url=>url.pathname.endsWith('/config')?{}:{metrics:[
  {key:'targets_up',status:'ok',samples:[{labels:{job:'hub',instance:'hub-1'},value:1},{labels:{job:'hub',instance:'hub-2'},value:1}]},
  {key:'db_open',status:'ok',samples:[{labels:{job:'hub',instance:'hub-2',pool:'query'},value:5}]}
 ]});
 try{await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);await p.page.locator('[data-infra="mysql"]').click();assert.match(await p.page.locator('#infra-comparison').innerText(),/中台实例 1/);assert.match(await p.page.locator('#infra-comparison tbody').innerText(),/未采集该实例/);}finally{await p.close();}
});

test('delivery summary does not claim receiver acknowledgement when sending never started',async()=>{
 const p=await open('hub','#messages',()=>({items:[{cycleNo:1,attemptNo:1,outcome:'SUCCESS',sendStarted:false,summary:'http_status=200; outcome=SUCCESS'}]}));
 try{await p.page.locator('#query').click();await p.page.waitForFunction(()=>!document.getElementById('query').disabled);const text=await p.page.locator('#message-table').innerText();assert.match(text,/http_status=200/);assert.doesNotMatch(text,/接收端确认成功/);}finally{await p.close();}
});
test('retained metric values explain failed latest collection separately from age expiry',async()=>{
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{}:{metrics:[{key:'archive_pending',status:'stale',reason:'query_failed',source:'prometheus',samples:[{labels:{},value:17,sampledAt:'2026-10-29T06:00:00Z'}]}]});
 try{await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-metrics').disabled);const text=await p.page.locator('#metric-overview article').nth(2).innerText();assert.match(text,/17/);assert.match(text,/样本已过期/);assert.match(text,/最近采样查询失败/);}finally{await p.close();}
});

test('hub monitoring excludes mock internals while keeping hub observed platform outcomes',async()=>{
 const p=await open('hub','#infra',url=>url.pathname.endsWith('/config')?{}:{metrics:[
 {key:'cpu',status:'ok',samples:[{labels:{job:'hub',instance:'hub-1'},value:1},{labels:{job:'mock-platform1',instance:'mock-platform1:18081'},value:99}]},
 {key:'first_rate',status:'ok',samples:[{labels:{platform:'PLATFORM_A',outcome:'SUCCESS'},value:2}]},
 {key:'receipts',status:'ok',samples:[{labels:{job:'mock-platform1'},value:999}]},
 {key:'b_lag',status:'ok',samples:[{labels:{},value:77}]}
 ]});
 try{await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);assert.doesNotMatch(await p.page.locator('#infra-metrics').innerText(),/mock-platform1|99/);assert.doesNotMatch(await p.page.locator('#infra-instance').innerText(),/mock-platform1/);await p.page.locator('[data-nav="overview"]').click();assert.doesNotMatch(await p.page.locator('#metric-business').textContent(),/外部累计实收|平台 B 消费|999/);assert.match(await p.page.locator('#metric-business').textContent(),/平台 A/);}finally{await p.close();}
});
test('resource labels have stronger metric names and distinguish dimension values',async()=>{
 const p=await open('hub','#infra',url=>url.pathname.endsWith('/config')?{}:{metrics:[{key:'db_open',status:'ok',samples:[{labels:{job:'hub',instance:'hub-1',pool:'query'},value:3}]}]});
 try{await p.page.locator('#refresh-infra').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-infra').disabled);const title=p.page.locator('.resource-metric-title').first(),group=p.page.locator('.resource-group-label').first();const weight=await title.evaluate(e=>Number(getComputedStyle(e).fontWeight));assert.ok(weight>=600);assert.ok(await group.evaluate(e=>parseFloat(getComputedStyle(e).fontSize))<await title.evaluate(e=>parseFloat(getComputedStyle(e).fontSize)));assert.match(await p.page.locator('.resource-dimension-value').first().innerText(),/query/);}finally{await p.close();}
});
test('overview health is a separate full width row and business charts preserve real history gaps',async()=>{
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{}:{window:'15m',metrics:[
 {key:'first_rate',status:'ok',samples:[{labels:{platform:'PLATFORM_A',outcome:'SUCCESS'},value:3},{labels:{platform:'PLATFORM_A',outcome:'UNKNOWN'},value:1}]},
 {key:'recovery_tasks',status:'ok',samples:[{labels:{platform:'PLATFORM_B',status:'NEEDS_ATTENTION'},value:4}]}
 ],trends:[{key:'ingress_rate',status:'ok',start:'2026-10-29T06:00:00Z',end:'2026-10-29T06:01:00Z',step:15,series:[{labels:{status:'200'},points:[{time:'2026-10-29T06:00:00Z',value:0},{time:'2026-10-29T06:00:15Z',value:null},{time:'2026-10-29T06:00:30Z',value:2}]}]}]});
 try{await p.page.setViewportSize({width:1600,height:1100});await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-metrics').disabled);const health=await p.page.locator('#health-section').boundingBox(),trend=await p.page.locator('#trend-section').boundingBox();assert.ok(health.width>=trend.width-2);assert.ok(health.y>=trend.y+trend.height);assert.equal(await p.page.locator('#metric-trend svg polyline').count(),2);assert.match(await p.page.locator('#metric-trend').innerText(),/2026/);assert.equal(await p.page.locator('[data-chart="first-distribution"] .donut-chart').count(),1);assert.match(await p.page.locator('[data-chart="first-distribution"]').innerText(),/75%/);assert.match(await p.page.locator('[data-chart="recovery"] .chart-bars').innerText(),/4/);assert.equal(await p.page.locator('#business-raw').getAttribute('open'),null);assert.deepEqual(p.errors,[]);}finally{await p.close();}
});
test('business chart zero totals and unavailable data never become a successful full donut',async()=>{
 const p=await open('hub','#overview',url=>url.pathname.endsWith('/config')?{}:{metrics:[{key:'first_rate',status:'ok',samples:[{labels:{platform:'PLATFORM_A',outcome:'SUCCESS'},value:0}]}]});
 try{await p.page.locator('#refresh-metrics').click();await p.page.waitForFunction(()=>!document.getElementById('refresh-metrics').disabled);assert.equal(await p.page.locator('[data-chart="first-distribution"] .donut-chart').count(),0);assert.match(await p.page.locator('[data-chart="first-distribution"]').innerText(),/无首轮投递流量/);assert.match(await p.page.locator('[data-chart="recovery"]').innerText(),/未取得|不可用|暂无/);}finally{await p.close();}
});

// Trace lookup must never pick the first of several messages sharing a trace.
test('trace lookup encodes exact identifiers, pages matches and waits for explicit message selection',async()=>{
 const trace='trace +/&重复';
 const p=await open('hub','#chain',url=>{
  if(url.pathname.endsWith('/messages'))return url.searchParams.has('cursor')?{items:[{msgId:'trace-message-3',traceId:trace,receivedAt:'2026-10-29T06:00:03Z'}]}:{items:[{msgId:'trace-message-1',traceId:trace},{msgId:'trace-message-2',traceId:trace}],nextCursor:'opaque+/='};
  if(url.pathname.endsWith('/messages/trace-message-2'))return {msgId:'trace-message-2',traceId:trace,platformResults:[]};
  return {items:[]};
 });
 try{
  await p.page.locator('#chain-kind').selectOption('traceId');await p.page.locator('#chain-id').fill(trace);await p.page.locator('#load-chain').click();
  await p.page.waitForFunction(()=>document.getElementById('chain-matches').textContent.includes('trace-message-2'));
  const call=p.calls.find(c=>c.url.pathname.endsWith('/messages'));assert.equal(call.url.searchParams.get('traceId'),trace);assert.equal(call.url.searchParams.get('limit'),'20');
  assert.equal(p.calls.filter(c=>/\/messages\//.test(c.url.pathname)).length,0);
  await p.page.locator('#chain-matches tbody tr').nth(1).getByRole('button',{name:'查看耗时链路'}).click();
  await p.page.waitForFunction(()=>document.getElementById('chain-detail').textContent.includes('trace-message-2'));
  assert.equal(p.calls.filter(c=>/\/messages\//.test(c.url.pathname)).length,1);
  await p.page.locator('#chain-trace-next').click();await p.page.waitForFunction(()=>document.getElementById('chain-matches').textContent.includes('trace-message-3'));
  assert.equal(p.calls.filter(c=>c.url.pathname.endsWith('/messages'))[1].url.searchParams.get('cursor'),'opaque+/=');
  assert.equal(await p.page.locator('#chain-trace-next').isDisabled(),true);
  assert.ok(p.calls.every(c=>c.method==='GET'));assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('trace lookup distinguishes empty and failed pages without inventing a selected chain',async()=>{
 let phase=0;
 const p=await open('hub','#chain',()=>phase===0?{items:[]}:phase===1?{items:[{msgId:'retained-match'}],nextCursor:'retry-cursor'}:{httpStatus:503});
 try{
  await p.page.locator('#chain-kind').selectOption('traceId');await p.page.locator('#chain-id').fill('not-found');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  assert.match(await p.page.locator('#chain-match-status').innerText(),/暂无匹配.*不能证明/s);assert.equal(await p.page.locator('#chain-detail').innerText(),'');
  phase=1;await p.page.locator('#chain-id').fill('matches');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-matches').textContent.includes('retained-match'));
  phase=2;await p.page.locator('#chain-trace-next').click();await p.page.waitForFunction(()=>!document.getElementById('chain-trace-next').disabled);
  assert.match(await p.page.locator('#chain-match-status').innerText(),/失败.*保留.*本页/s);assert.match(await p.page.locator('#chain-matches').innerText(),/retained-match/);
  await p.page.locator('#chain-trace-next').click();assert.equal(p.calls.at(-1).url.searchParams.get('cursor'),'retry-cursor');assert.ok(p.calls.every(c=>c.method==='GET'));assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('late trace lookup cannot replace a newer search result',async()=>{
 let release;const gate=new Promise(resolve=>release=resolve);
 const p=await open('hub','#chain',async url=>{if(url.searchParams.get('traceId')==='old'){await gate;return {items:[{msgId:'old-message'}]};}return {items:[{msgId:'new-message'}]};});
 try{
  await p.page.locator('#chain-kind').selectOption('traceId');await p.page.locator('#chain-id').fill('old');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('load-chain').disabled);
  await p.page.locator('#chain-id').fill('new');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-matches').textContent.includes('new-message'));
  release();await p.page.waitForTimeout(80);assert.doesNotMatch(await p.page.locator('#chain-matches').innerText(),/old-message/);assert.deepEqual(p.errors,[]);
 }finally{release();await p.close();}
});

test('trace waterfall puts concurrent attempts on one scale and exposes real durations and gaps',async()=>{
 const p=await open('hub','#chain',url=>{
  if(url.pathname.includes('/deliveries/a/'))return {items:[{attemptNo:2,cycleNo:2,startedAt:'2026-10-29T06:00:00.800Z',finishedAt:'2026-10-29T06:00:01Z',sendStarted:true,outcome:'SUCCESS',summary:'http_status=200; outcome=SUCCESS'},{attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:00.100Z',finishedAt:'2026-10-29T06:00:00.400Z',sendStarted:true,outcome:'RETRYABLE_FAILURE'}]};
  if(url.pathname.includes('/deliveries/b/'))return {items:[{attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:00.200Z',finishedAt:'2026-10-29T06:00:00.600Z',sendStarted:1,outcome:'SUCCESS',summary:'broker write confirmed'}]};
  return {msgId:'trace-visual',traceId:'trace-demo',receivedAt:'2026-10-29T06:00:00Z',storedAt:'2026-10-29T06:00:02Z',routeSnapshot:{resolvedAt:'2026-10-29T06:00:00.010Z'},platformResults:[{deliveryId:'a',platformId:'PLATFORM_A',platformUid:'uid-a'},{deliveryId:'b',platformId:'PLATFORM_B',platformUid:'uid-b'}]};
 });
 try{
  await p.page.setViewportSize({width:1440,height:1000});await p.page.locator('#chain-id').fill('trace-visual');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  const chart=p.page.locator('#chain-waterfall');assert.match(await chart.innerText(),/耗时链路/);assert.equal(await chart.locator('[data-kind="event"]').count(),3);assert.equal(await chart.locator('[data-kind="attempt"]').count(),3);
  const a=chart.locator('[data-delivery-id="a"][data-attempt-no="1"] .trace-mark'),b=chart.locator('[data-delivery-id="b"][data-attempt-no="1"] .trace-mark');
  const aBox=await a.boundingBox(),bBox=await b.boundingBox();assert.ok(aBox.x<bBox.x&&aBox.x+aBox.width>bBox.x,'concurrent intervals overlap on the same horizontal scale');assert.ok(Math.abs(bBox.width/aBox.width-4/3)<0.05,'400ms and 300ms keep their scale');
  assert.match(await chart.locator('[data-kind="gap"]').innerText(),/400 ms/);assert.match(await chart.locator('[data-kind="gap"]').innerText(),/原因未记录/);
  for(const [index,fraction] of [[0,0],[2,1]]){
   const event=chart.locator('[data-kind="event"]').nth(index),mark=await event.locator('.trace-mark').boundingBox(),lane=await event.locator('.trace-lane').boundingBox();
   assert.ok(Math.abs(mark.x+mark.width/2-(lane.x+lane.width*fraction))<2,'event marker center must keep its true time position');
   assert.equal(await event.locator('.trace-mark').evaluate(e=>{const s=getComputedStyle(e,'::before');return s.content!=='none'&&s.display!=='none';}),false,'event time must have one marker, without a second marker from legacy timeline styles');
  }
  const selectionCalls=p.calls.length;
  await chart.locator('[data-delivery-id="a"][data-attempt-no="2"] .trace-mark').click();assert.match(await p.page.locator('#trace-selection').innerText(),/uid-a/);assert.match(await p.page.locator('#trace-selection').innerText(),/200 ms/);assert.match(await p.page.locator('#trace-selection').innerText(),/接收端确认成功/);
  const target=chart.locator('[data-delivery-id="b"] button');await target.focus();await p.page.keyboard.press('Enter');assert.match(await p.page.locator('#trace-selection').innerText(),/Broker 已确认写入.*消费.*另查/s);
  assert.match(await chart.innerText(),/已记录时间跨度/);assert.doesNotMatch(await chart.innerText(),/端到端处理耗时/);
  assert.ok(p.calls.every(c=>c.method==='GET'));assert.equal(p.calls.length,selectionCalls);assert.deepEqual(p.errors,[]);
  if(process.env.CAPTURE_HUB_IMAGES)await hubSnapshot(p.page,'trace-waterfall',1440);
  await p.page.setViewportSize({width:640,height:1000});assert.equal(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.ok(await chart.locator('button').first().isVisible());
  if(process.env.CAPTURE_HUB_IMAGES)await hubSnapshot(p.page,'trace-waterfall-narrow',640);
 }finally{await p.close();}
});
test('trace waterfall preserves zero durations and never fabricates invalid intervals or network sends',async()=>{
 const p=await open('hub','#chain',url=>url.pathname.endsWith('/attempts')?{items:[
  {attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:00Z',finishedAt:'2026-10-29T06:00:00Z',sendStarted:0,outcome:'PERMANENT_FAILURE',summary:'<img src=x onerror=alert(1)>'},
  {attemptNo:2,cycleNo:1,startedAt:'bad-date',finishedAt:'2026-10-29T06:00:01Z',outcome:'UNKNOWN'},
  {attemptNo:3,cycleNo:1,startedAt:'2026-10-29T06:00:02Z',finishedAt:'2026-10-29T06:00:01Z',outcome:'UNKNOWN'},
  {attemptNo:4,cycleNo:1,startedAt:'0001-01-01T00:00:00Z',outcome:'UNKNOWN'}
 ]}:{msgId:'invalid-time',platformResults:[{deliveryId:'invalid-d',platformId:'PLATFORM_A'}]});
 try{
  await p.page.locator('#chain-id').fill('invalid-time');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  const chart=p.page.locator('#chain-waterfall');assert.equal(await chart.locator('[data-kind="attempt"] .trace-mark').count(),1);assert.equal(await chart.locator('[data-kind="gap"]').count(),0);
  await chart.locator('[data-attempt-no="1"] button').click();assert.match(await p.page.locator('#trace-selection').innerText(),/0 ms/);assert.match(await p.page.locator('#trace-selection').innerText(),/未开始外发/);assert.match(await p.page.locator('#trace-selection').innerText(),/<img/);assert.equal(await chart.locator('img').count(),0);
  assert.match(await chart.locator('[data-attempt-no="2"]').innerText(),/时间.*缺失|时间.*异常/);assert.match(await chart.locator('[data-attempt-no="3"]').innerText(),/时间.*异常/);assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('trace waterfall pages partial history and retains truthful failure state with retry',async()=>{
 let fail=true;
 const p=await open('hub','#chain',url=>{
  if(url.pathname.endsWith('/attempts')){
   if(!url.searchParams.has('cursor'))return {items:[{attemptNo:3,cycleNo:1,startedAt:'2026-10-29T06:00:03Z',finishedAt:'2026-10-29T06:00:03.010Z',outcome:'SUCCESS'}],nextCursor:'older'};
   return fail?{httpStatus:503}:{items:[{attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:01Z',finishedAt:'2026-10-29T06:00:01.100Z',outcome:'RETRYABLE_FAILURE'},{attemptNo:2,cycleNo:1,startedAt:'2026-10-29T06:00:02Z',finishedAt:'2026-10-29T06:00:02.100Z',outcome:'RETRYABLE_FAILURE'}]};
  }return {msgId:'partial-trace',receivedAt:'2026-10-29T06:00:00Z',platformResults:[{deliveryId:'partial-d',platformId:'PLATFORM_A'}]};
 });
 try{
  await p.page.locator('#chain-id').fill('partial-trace');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  const chart=p.page.locator('#chain-waterfall');assert.match(await chart.innerText(),/仍有.*历史|还有.*历史/);assert.equal(await chart.locator('[data-kind="gap"]').count(),0);
  await chart.locator('[data-trace-more]').click();await p.page.waitForFunction(()=>document.querySelector('[data-trace-more]')?.disabled===false);assert.match(await chart.innerText(),/查询失败/);assert.equal(await chart.locator('[data-kind="attempt"]').count(),1);
  fail=false;await chart.locator('[data-trace-more]').click();await p.page.waitForFunction(()=>document.querySelectorAll('#chain-waterfall [data-kind="attempt"]').length===3);
  assert.equal(await chart.locator('[data-kind="gap"]').count(),2);assert.doesNotMatch(await chart.innerText(),/查询失败|仍有.*历史/);assert.equal(p.calls.filter(c=>c.url.searchParams.get('cursor')==='older').length,2);assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('trace waterfall never connects missing attempt numbers or labels no route as an attempted send',async()=>{
 let noRoute=false;
 const p=await open('hub','#chain',url=>url.pathname.endsWith('/attempts')?{items:[{attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:00Z',finishedAt:'2026-10-29T06:00:01Z'},{attemptNo:3,cycleNo:1,startedAt:'2026-10-29T06:00:03Z',finishedAt:'2026-10-29T06:00:04Z'}]}:{msgId:noRoute?'no-route':'missing-attempt',routeStatus:noRoute?'NO_ROUTE':'ROUTED',platformResults:noRoute?[]:[{deliveryId:'skip-d',platformId:'PLATFORM_A'}]});
 try{
  await p.page.locator('#chain-id').fill('missing-attempt');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);assert.equal(await p.page.locator('#chain-waterfall [data-kind="gap"]').count(),0);
  noRoute=true;await p.page.locator('#chain-id').fill('no-route');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);
  assert.match(await p.page.locator('#chain-waterfall').innerText(),/无投递目标/);assert.equal(await p.page.locator('#chain-waterfall [data-kind="attempt"]').count(),0);assert.match(await p.page.locator('#chain-waterfall').innerText(),/暂无.*时间/);assert.deepEqual(p.errors,[]);
 }finally{await p.close();}
});
test('late attempts and failed message lookup cannot leave a previous message waterfall visible',async()=>{
 let release;const gate=new Promise(resolve=>release=resolve);
 const p=await open('hub','#chain',async url=>{
  if(url.pathname.endsWith('/attempts')){await gate;return {items:[{attemptNo:1,cycleNo:1,startedAt:'2026-10-29T06:00:00Z',finishedAt:'2026-10-29T06:00:01Z'}]};}
  if(url.pathname.endsWith('/messages/missing'))return {httpStatus:404};
  return {msgId:'old-msg',receivedAt:'2026-10-29T06:00:00Z',platformResults:[{deliveryId:'old-d',platformId:'PLATFORM_A'}]};
 });
 try{
  await p.page.locator('#chain-id').fill('old-msg');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>document.getElementById('chain-detail').textContent.includes('old-msg'));
  await p.page.locator('#chain-id').fill('missing');await p.page.locator('#load-chain').click();await p.page.waitForFunction(()=>!document.getElementById('load-chain').disabled);release();await p.page.waitForTimeout(80);
  assert.equal(await p.page.locator('#chain-waterfall').innerText(),'');assert.equal(await p.page.locator('#attempts-detail').innerText(),'');assert.doesNotMatch(await p.page.locator('#chain-detail').innerText(),/old-msg/);assert.deepEqual(p.errors,[]);
 }finally{release();await p.close();}
});
