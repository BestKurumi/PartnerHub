'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),test=require('node:test');
function page(respond){
 const elements=new Map(),timers=new Map(),calls=[];let timerId=0;
 class Element{constructor(){this.value='';this.textContent='';this.children=[];this.disabled=false;this.events={};}appendChild(e){this.children.push(e);}replaceChildren(...e){this.children=e;}addEventListener(name,fn){this.events[name]=fn;}set innerHTML(_){throw Error('unsafe rendering');}}
 const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
 const context={document:{getElementById:get,createElement:()=>new Element()},AbortController,Error,JSON,Map,Number,Promise,encodeURIComponent,setTimeout(fn,delay){const id=++timerId;timers.set(id,{fn,delay});return id;},clearTimeout(id){timers.delete(id);},async fetch(url,options){calls.push({url,...options});return respond(url,options);}};
 const html=fs.readFileSync(new URL('../../../web/upstream/index.html','file://'+__filename),'utf8');vm.runInNewContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
 get('token').value='input-only-token';get('client').value='000123';get('type').value='NORMAL';get('count').value='1';get('qps').value='1';
 async function drain(){for(let n=0;n<20;n++){await new Promise(setImmediate);const entry=[...timers].find(([,timer])=>timer.delay===2000);if(entry){timers.delete(entry[0]);entry[1].fn();}else if(n>3)break;}}
 return {get,calls,drain};
}
const id='8a236f08-1bf4-4bb3-80c2-7eb27fe7c63f';
test('lost batch body retains all header IDs and polling never resubmits',async()=>{
 const p=page((url,options)=>options.method==='POST'?{ok:true,headers:{get:()=>id},json:async()=>{throw Error('lost body');}}:{status:404,ok:false,text:async()=>'{"status":"RECORD_NOT_AVAILABLE"}'});
 await p.get('trade-form').events.submit({preventDefault(){}});
 assert.match(p.get('items').children[0].children[0].textContent,/8a236f08/);
 const polling=p.get('poll').events.click();await p.drain();await polling;
 assert.equal(p.calls.filter(call=>call.method==='POST').length,1);
 assert.equal(p.calls.filter(call=>call.url.startsWith('demo/messages/')).length,5);
 assert.match(p.get('items').children[0].children[2].textContent,/不要重提/);
 assert.equal(p.calls[0].headers['X-Demo-Token'],'input-only-token');
});
test('malicious response text renders literally; unsent IDs cause no query',async()=>{
 const p=page(()=>({ok:true,headers:{get:()=>id},json:async()=>({items:[{msgId:id,traceId:'<img src=x onerror=alert(1)>',acceptance:'REJECTED',status:'NOT_SENT',sent:false}]} )}));
 await p.get('trade-form').events.submit({preventDefault(){}});
 await p.get('refresh').events.click();
 assert.match(p.get('items').children[0].children[0].textContent,/<img/);
 assert.equal(p.calls.length,1);
 assert.match(p.get('items').children[0].children[2].textContent,/明确未发送/);
});
test('full page history stops new submissions without discarding verification IDs',async()=>{
 const ids=Array.from({length:100},(_,i)=>'id-'+i);
 const p=page(()=>({ok:true,headers:{get:()=>ids.join(',')},json:async()=>({items:ids.map(msgId=>({msgId,traceId:'demo-'+msgId,acceptance:'UNCONFIRMED',status:'NEEDS_VERIFICATION',sent:true}))})}));
 p.get('count').value='100';
 await p.get('trade-form').events.submit({preventDefault(){}});
 p.get('count').value='1';
 await p.get('trade-form').events.submit({preventDefault(){}});
 assert.equal(p.calls.length,1,'full history must prevent another POST');
 assert.equal(p.get('items').children.length,100);
 assert.match(p.get('notice').textContent,/100/);
 assert.equal(p.get('known-ids').value.split('\n').length,100);
 assert.match(p.get('known-ids').value,/id-99/);
});
