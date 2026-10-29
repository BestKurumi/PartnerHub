'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),test=require('node:test');
function page(respond){const elements=new Map(),timers=new Map(),calls=[];let sequence=0;
 const element=()=>({value:'',textContent:'',disabled:false,children:[],appendChild(child){this.children.push(child)},replaceChildren(...children){this.children=children},set innerHTML(_){throw Error('unsafe HTML')}});
 const get=id=>{if(!elements.has(id))elements.set(id,element());return elements.get(id)};
 const context={document:{getElementById:get,createElement:element},crypto:require('node:crypto').webcrypto,window:{addEventListener(){}},location:{href:'http://localhost:28080/hub/'},URL,URLSearchParams,AbortController,Date,JSON,encodeURIComponent,setTimeout(fn,delay){let id=++sequence;timers.set(id,{fn,delay});return id},clearTimeout(id){timers.delete(id)},fetch:async(url,options)=>{calls.push({url:String(url),...options});return respond(url,options)}};
 const html=fs.readFileSync(new URL('../../web/hub/index.html','file://'+__filename),'utf8');vm.runInNewContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
 get('kind').value='messages';get('detail').value='message';
 return {get,calls,async drain(){for(let i=0;i<30;i++){await new Promise(setImmediate);let timer=[...timers].find(([,v])=>v.delay===3000);if(!timer)break;timers.delete(timer[0]);await timer[1].fn()}}};}
const response=(status,data)=>({status,ok:status===200,json:async()=>data});
test('404 polling stays read-only, bounded and explains absence',async()=>{const p=page(()=>response(404,{code:'RECORD_NOT_AVAILABLE'}));p.get('id').value='uuid';p.get('poll').onclick();await p.drain();assert.equal(p.calls.length,10);assert.ok(p.calls.every(v=>!v.method||v.method==='GET'));assert.ok(p.get('status').textContent.includes('可能尚未同步'));});
test('details render malicious values literally',async()=>{const p=page(()=>response(200,{msgId:'<img src=x onerror=alert(1)>',platformResults:[]}));p.get('id').value='uuid';p.get('form').onsubmit({preventDefault(){}});await p.drain();assert.ok(p.get('result').textContent.includes('<img'));assert.ok(p.calls[0].url.endsWith('/hub/demo/messages/uuid'));});
test('failed DB query is not reported as an absent delivery',async()=>{const p=page(()=>response(503,{code:'QUERY_UNAVAILABLE'}));p.get('form').onsubmit({preventDefault(){}});await p.drain();assert.ok(p.get('status').textContent.includes('查询暂不可用'));assert.equal(p.calls.length,1);});

test('only attention tasks and unresolved routes offer manual actions',async()=>{
 const p=page(()=>response(200,{items:[{msgId:'m1',routeStatus:'NEEDS_ATTENTION',platformResults:[]},{msgId:'m2',routeStatus:'ROUTED',platformResults:[{deliveryId:'d1',currentState:'NEEDS_ATTENTION',platformId:'PLATFORM_A'},{deliveryId:'d2',currentState:'SUCCEEDED'}]},{msgId:'m3',routeStatus:'NO_ROUTE',platformResults:[]}]}));
 p.get('form').onsubmit({preventDefault(){}});await p.drain();assert.equal(p.get('actions').children.length,2);assert.ok(p.get('actions').children[0].textContent.includes('m1'));assert.ok(p.get('actions').children[1].textContent.includes('d1'));assert.equal(p.calls.length,1);
});
test('manual action saves intent with original target and explains pending',async()=>{
 const p=page((url,options)=>options.method==='POST'?response(200,{operationId:JSON.parse(options.body).operationId,state:'PENDING',retryCycle:2}):response(200,{items:[{deliveryId:'fixed-delivery',platformUid:'fixed-uid',currentState:'NEEDS_ATTENTION',platformId:'PLATFORM_A'}]}));
 p.get('kind').value='deliveries';p.get('token').value='secret';p.get('form').onsubmit({preventDefault(){}});await p.drain();p.get('operator').value='操作员';p.get('reason').value='修复配置';await p.get('actions').children[0].onclick();
 const call=p.calls[1],body=JSON.parse(call.body);assert.ok(call.url.endsWith('/hub/demo/deliveries/fixed-delivery/retry'));assert.deepEqual(Object.keys(body).sort(),['operationId','operator','reason']);assert.match(body.operationId,/^[a-f0-9-]{36}$/);assert.equal(call.headers['X-Demo-Token'],'secret');assert.ok(p.get('manual-status').textContent.includes('不代表投递成功'));assert.ok(p.get('operation').textContent.includes(body.operationId));
});
test('uncertain command reuses exact operation and blocks duplicate clicks',async()=>{
 let release;const pending=new Promise(resolve=>{release=resolve});let posts=0;
 const p=page(async(url,options)=>{if(options.method==='POST'){posts++;if(posts===1){await pending;return response(503,{code:'RETRY_UNCONFIRMED'})}return response(200,{state:'PENDING',retryCycle:2})}return response(200,{items:[{deliveryId:'d1',currentState:'NEEDS_ATTENTION'}]})});
 p.get('form').onsubmit({preventDefault(){}});await p.drain();p.get('operator').value='op';p.get('reason').value='repair';const b=p.get('actions').children[0];const first=b.onclick();await b.onclick();assert.equal(posts,1);release();await first;
 const body=p.calls[1].body;p.get('reason').value='changed';await b.onclick();assert.equal(posts,1);await p.get('retry-operation').onclick();assert.equal(posts,2);assert.equal(p.calls[2].body,body);assert.ok(p.get('manual-status').textContent.includes('不代表投递成功'));
});

test('platform configuration displays pause without replacing delivery results',async()=>{
 const p=page(()=>response(200,{scope:'INSTANCE_CONFIG',platforms:[{platformId:'PLATFORM_A',state:'PAUSED'},{platformId:'PLATFORM_B',state:'RUNNING'}]}));p.get('token').value='secret';p.get('result').textContent='saved delivery facts';await p.get('load-platforms').onclick();assert.ok(p.get('platform-status').textContent.includes('PAUSED'));assert.ok(p.get('platform-status').textContent.includes('RUNNING'));assert.equal(p.get('result').textContent,'saved delivery facts');assert.ok(p.calls[0].url.endsWith('/hub/demo/runtime'));assert.equal(p.calls[0].headers['X-Demo-Token'],'secret');assert.ok(!p.calls[0].method||p.calls[0].method==='GET');
});
