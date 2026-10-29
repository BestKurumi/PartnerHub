(()=>{
 const C=Console;
 const originalRender=render;
 render=function(){originalRender();for(const [i,entry] of [...entries.values()].entries()){const row=el('items').children[i],cell=C.node('td');const a=C.link('查看中台链路 ↗','/hub/?msgId='+encodeURIComponent(entry.msgId)+'#chain');C.append(cell,a);row.appendChild(cell);}el('result-summary').textContent='当前页面保留 '+entries.size+' / 100 条核查记录。刷新或关闭前请保存清单。';};
 const originalSubmit=submit;
 // The original handler owns the single POST and header-ID preservation.
 el('trade-form').removeEventListener('submit',submit);
 el('trade-form').addEventListener('submit',async event=>{await originalSubmit(event);if(entries.size)location.hash='results';});
 const originalRefresh=refresh;
 refresh=async function(){await originalRefresh();if(!el('detail').textContent)return;try{const data=JSON.parse(el('detail').textContent);if(data.msgId)el('result-summary').textContent='当前页面保留 '+entries.size+' / 100 条。最近权威记录：'+data.msgId+' · '+(data.status||data.routeStatus||'已持久保存')+'；不等于平台实际接收。';}catch(_){}};
 render();
})();
