(()=>{
 const C=Console;
 const relatedQuery=new URLSearchParams(location.search);for(const [id,key] of [['uid','uid'],['trace','traceId']])if(relatedQuery.has(key))$(id).value=relatedQuery.get(key);
 const protocolFields=["userUid", "securityCode", "securityName", "direction", "execTime", "market", "tradeType", "traceId"];
 const fieldLabels={"userUid": "平台用户 UID", "securityCode": "证券代码", "securityName": "证券名称", "direction": "买卖方向", "execTime": "成交时间", "market": "交易市场", "tradeType": "交易类型", "traceId": "跟踪编号", "extUserId": "平台用户 UID", "stockCode": "证券代码", "stockName": "证券名称", "operateType": "买卖方向", "tradeDateTime": "成交时间", "marketNo": "交易市场", "bizType": "业务类型"};
 const displayValue=value=>value==null?'未提供':value===''?'（空字符串）':typeof value==='object'?JSON.stringify(value):String(value);
 function receiptField(key,value){const field=C.node('div','','receipt-field'),term=C.node('dt');field.dataset.field=key;C.append(term,C.node('strong',fieldLabels[key]||'扩展字段'),C.node('code',key));C.append(field,term,C.node('dd',displayValue(value)));return field;}
 const originalDraw=draw;
 draw=function(items){
  originalDraw(items);const cards=[];
  for(const item of items.slice(0,100)){
   const card=C.node('article','','receipt'),header=C.node('div','','receipt-head');C.append(header,C.node('h3','平台 A · 实际接收通知'),C.badge('实际已接收'));C.append(card,header);
   const meta=C.node('dl','','receipt-meta');for(const [name,value] of [['接收编号',item.receiveId],['平台接收时间',item.receivedAt?C.time(item.receivedAt):null],['平台 UID',item.userUid],['跟踪编号',item.traceId]]){const row=C.node('div');C.append(row,C.node('dt',name),C.node('dd',displayValue(value)));C.append(meta,row);}C.append(card,meta);
   const payload=typeof item.payload==='object'&&item.payload&&!Array.isArray(item.payload)?item.payload:{};
   C.append(card,C.node('h4','通知业务字段 · 协议 8 项','receipt-section-title'));
   const fields=C.node('dl','','receipt-fields');for(const key of [...protocolFields,...Object.keys(payload).filter(key=>!protocolFields.includes(key))])C.append(fields,receiptField(key,payload[key]));C.append(card,fields);
   const raw=C.node('details');C.append(raw,C.node('summary','原始通知报文'),C.node('pre',JSON.stringify(item.payload??null,null,2)));C.append(card,raw);cards.push(card);
  }
  if(!cards.length)cards.push(C.node('div','当前过滤条件下暂无实收，不表示未发送。','empty'));$('receipt-cards').replaceChildren(...cards);
 };
 const updateFacts=()=>{const text=$('bindingResult').textContent;if(!text)return;try{const data=JSON.parse(text);$('binding-facts').replaceChildren(C.kv(Object.entries(data).filter(([k,v])=>typeof v!=='object').map(([k,v])=>[k,v])));}catch(_){}};
 new MutationObserver(updateFacts).observe($('bindingResult'),{childList:true,characterData:true,subtree:true});
 $('load-bindings').onclick=async()=>{const client=$('binding-client').value||$('clientId').value;if(!client.trim()){$('binding-query-status').textContent='请填写客户号。';return;}const b=$('load-bindings');b.disabled=true;try{const data=await C.request('/hub/demo/bindings?'+new URLSearchParams({limit:'20',platformId:'PLATFORM_A',clientId:client}));$('current-bindings').replaceChildren(C.table(['客户号','平台 UID','权威状态','版本','更新时间'],(data.items||[]).map(v=>[v.clientId,v.platformUid,C.badge(v.status),v.revision,C.time(v.updatedAt)])));$('binding-query-status').textContent='中台权威关系 · 不用发布成功代替生效确认。';}catch(e){$('binding-query-status').textContent=e.message;}finally{b.disabled=false;}};
})();
