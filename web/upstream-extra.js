(()=>{
 const C=Console;
 const acceptanceLabels={COMPLETED:['首轮完成','good'],ACCEPTED:['已保存接管','warning'],UNCONFIRMED:['结果未确认','warning'],REJECTED:['提交被拒绝','bad']};
 const statusLabels={SUCCEEDED:'首轮投递成功',NO_ROUTE:'无投递目标',RECOVERY_PENDING:'恢复中',NEEDS_ATTENTION:'待人工处理',NEEDS_VERIFICATION:'待核查',NOT_SENT:'明确未发送',PRE_SEND_REJECTED:'发送前被拒绝'};
 function submissionFacts(entry){
  const box=C.node('div','','submission-facts'),known=acceptanceLabels[entry.acceptance];
  const label=entry.sent===false?'明确未发送':known?.[0]||'未知确认状态';
  C.append(box,C.node('strong',label,'badge submission-state '+(entry.sent===false?'warning':known?.[1]||'warning')));
  C.append(box,C.node('span',statusLabels[entry.status]||entry.status||'状态未记录','submission-outcome'));
  if(entry.httpStatus)C.append(box,C.node('small','HTTP '+entry.httpStatus,'submission-http'));
  if(!known)C.append(box,C.node('small','原确认值：'+(entry.acceptance||'未提供')));
  const detail=C.node('details'),summary=C.node('summary','原始提交详情');
  C.append(detail,summary,C.node('pre',JSON.stringify({acceptance:entry.acceptance,status:entry.status,httpStatus:entry.httpStatus,sent:entry.sent,response:entry.response},null,2)));C.append(box,detail);return box;
 }
 render=function(){
  const rows=[];for(const entry of entries.values()){
   const row=C.node('tr');
   for(const value of [entry.msgId,entry.traceId,submissionFacts(entry),entry.record,C.link('查看中台链路 ↗','/hub/?msgId='+encodeURIComponent(entry.msgId)+'#chain')])C.append(row,C.append(C.node('td'),value instanceof Node?value:C.node('span',value,'record-identifier')));
   rows.push(row);
  }
  el('items').replaceChildren(...rows);el('known-ids').value=[...entries.keys()].join('\n');
  el('result-summary').textContent='当前页面保留 '+entries.size+' / 100 条核查记录。刷新或关闭前请保存清单。';
 };
 const originalSubmit=submit;
 // The original handler owns the single POST and header-ID preservation.
 el('trade-form').removeEventListener('submit',submit);
 el('trade-form').addEventListener('submit',async event=>{await originalSubmit(event);if(entries.size)location.hash='results';});
 const originalRefresh=refresh;
 refresh=async function(){await originalRefresh();if(!el('detail').textContent)return;try{const data=JSON.parse(el('detail').textContent);if(data.msgId)el('result-summary').textContent='当前页面保留 '+entries.size+' / 100 条。最近权威记录：'+data.msgId+' · '+(data.status||data.routeStatus||'已持久保存')+'；不等于平台实际接收。';}catch(_){}};
 render();
})();
