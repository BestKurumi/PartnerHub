'use strict';
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const test=require('node:test');
function page(platform,respond){
 const elements=new Map(),timers=new Map(),calls=[];let timerId=0;
 class Element{constructor(){this.value='';this.textContent='';this.children=[];this.disabled=false;}append(e){this.children.push(e);}replaceChildren(...e){this.children=e;}click(){if(!this.disabled&&this.onclick)return this.onclick();}set innerHTML(_){throw Error('Unsafe HTML rendering');}}
 const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
 const context={document:{getElementById:get,createElement:()=>new Element()},window:{addEventListener(){}},location:{href:'http://localhost:8080/mock/p1/'},URL,URLSearchParams,AbortController,Date,Error,JSON,crypto:{randomUUID:()=> 'd34e78a0-d5ad-4a10-b183-6698632b0071'},setTimeout(fn,delay){const id=++timerId;timers.set(id,{fn,delay});return id;},clearTimeout(id){timers.delete(id);},async fetch(url,options){calls.push({url:String(url),...options});return respond(url,options);}};
 const html=fs.readFileSync(new URL(`../../../web/${platform}/index.html`,'file://'+__filename),'utf8');
 vm.runInNewContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
 async function drain(){for(let n=0;n<40;n++){await new Promise(setImmediate);const entry=[...timers].find(([,t])=>t.delay<5000);if(entry){timers.delete(entry[0]);await entry[1].fn();}else if(n>2)break;}}
 return {get,calls,drain};
}
const response=(status,data)=>({status,ok:status>=200&&status<300,json:async()=>data});
for(const platform of ['platform1','platform2']){
 test(platform+' polls receipts at most 10 times and displays malicious text literally',async()=>{const p=page(platform,()=>response(200,{items:[{receiveId:'r',receivedAt:'now',userUid:'<img src=x onerror=alert(1)>',traceId:'<script>',payload:{name:'<script>alert(1)</script>'}}],nextCursor:''}));p.get('poll').onclick();await p.drain();assert.equal(p.calls.length,10);assert.ok(p.calls.every(c=>c.method==='GET'));const rendered=p.get('records').children[0].children[1].children.map(x=>x.textContent).join(' ');assert.ok(rendered.includes('<script>'));});
 test(platform+' publishes only once while polling unavailable binding result',async()=>{const p=page(platform,(_url,options)=>options.method==='POST'?response(202,{eventId:'id',status:'PUBLISHED'}):response(404,{status:'RECORD_NOT_AVAILABLE'}));p.get('clientId').value='c';p.get('platformUid').value='00009';p.get('eventType').value='BIND';await p.get('binding').onsubmit({preventDefault(){}});await p.drain();assert.equal(p.calls.filter(c=>c.method==='POST').length,1);assert.equal(p.calls.filter(c=>c.method==='GET').length,20);assert.ok(p.get('bindingStatus').textContent.includes('等待消费'));assert.ok(!p.get('bindingStatus').textContent.includes('已生效'));});
 test(platform+' recognizes authoritative result string as effective',async()=>{const p=page(platform,(_url,options)=>options.method==='POST'?response(202,{status:'PUBLISHED'}):response(200,{eventId:'id',result:'APPLIED',revision:1}));await p.get('binding').onsubmit({preventDefault(){}});await p.drain();assert.ok(p.get('bindingStatus').textContent.includes('已生效'),p.get('bindingStatus').textContent);assert.equal(p.calls.filter(c=>c.method==='GET').length,1);});
}

test('binding cannot be replaced while authority response is in flight',async()=>{let release;const authority=new Promise(resolve=>release=resolve);const p=page('platform1',(_url,options)=>options.method==='POST'?response(202,{status:'PUBLISHED'}):authority);await p.get('binding').onsubmit({preventDefault(){}});p.get('check').onclick();await new Promise(setImmediate);assert.equal(p.get('submit').disabled,true);release(response(200,{result:'APPLIED'}));await p.drain();assert.equal(p.get('submit').disabled,false);});

// Catches busy receipt controls invalidating the only in-flight response without starting another GET.
for(const platform of ['platform1','platform2']){
 for(const control of ['refresh','poll']){
  test(platform+' keeps pending receipts visible when '+control+' is clicked again',async()=>{
   let release;const pending=new Promise(resolve=>release=resolve);let count=0;
   const data={items:[{receiveId:'received-1',receivedAt:'now',userUid:'uid',traceId:'trace',payload:{name:'kept'}}],nextCursor:'next-page'};
   const p=page(platform,()=>++count===1?pending:response(200,data));
   p.get('refresh').click();await new Promise(setImmediate);
   assert.equal(p.calls.length,1);
   assert.equal(p.get('refresh').disabled,true,'refresh must not invalidate an in-flight receipt request');
   assert.equal(p.get('poll').disabled,true,'poll must not replace an in-flight receipt request');
   assert.equal(p.get('next').disabled,true);
   assert.equal(p.get('stop').disabled,false,'stopping polling remains available');
   p.get(control).click();assert.equal(p.calls.length,1);
   release(response(200,data));await p.drain();
   assert.ok(p.get('receiptStatus').textContent.includes('实际接收 1 条'));
   assert.equal(p.get('records').children[0].children[1].children[0].textContent,'received-1\nnow');
   assert.equal(p.get('refresh').disabled,false);assert.equal(p.get('poll').disabled,false);assert.equal(p.get('next').disabled,false);
   p.get('refresh').click();await p.drain();assert.equal(p.calls.length,2,'manual refresh works after pending GET finishes');
  });
 }
 test(platform+' re-enables receipt controls after a failed pending GET',async()=>{
  let reject;const pending=new Promise((_resolve,r)=>reject=r);const p=page(platform,()=>pending);
  p.get('refresh').click();await new Promise(setImmediate);
  assert.equal(p.get('refresh').disabled,true);assert.equal(p.get('poll').disabled,true);
  reject(Error('network unavailable'));await p.drain();
  assert.equal(p.get('refresh').disabled,false);assert.equal(p.get('poll').disabled,false);assert.equal(p.get('next').disabled,true);assert.equal(p.get('stop').disabled,false);
  assert.ok(p.get('receiptStatus').textContent.includes('network unavailable'));
 });
}
