(()=>{
 const C=Console,ids=C.byId;
 let metrics=null,infra='instances',infraMode='compare',selectedInstance='',chain=null,chainVersion=0,relatedVersion=0;
 const history=new Map(),attemptPages=new Map();
 const names={ingress_rate:'入口速率',ingress_total:'入口累计',first_rate:'平台首轮速率',first_p99:'首轮 P99（秒）',ingress_p99:'入口 P99（秒）',route_rate:'路由速率',archive_pending:'待归档条数',archive_bytes:'待归档字节',archive_oldest:'最老归档年龄（秒）',recovery_tasks:'恢复任务',recovery_oldest:'最老恢复年龄（秒）',binding_lag:'绑定消费 lag',receipts:'外部累计实收',b_lag:'平台 B 消费 lag',b_paused:'平台 B 暂停状态',targets_up:'采集目标 UP',observation_up:'业务采样成功',observation_age:'业务采样年龄（秒）',cpu:'CPU 消耗（核）',rss:'进程 RSS（字节）',heap:'Go 堆（字节）',goroutines:'Goroutine',gc_pause:'最近 GC 最大暂停（秒）',db_open:'数据库打开连接',db_in_use:'数据库使用中连接',db_max:'数据库池配置上限',resource_up:'资源采样成功（1/0）',resource_age:'资源采样年龄（秒）',mysql_connections:'MySQL 当前连接（个）',mysql_threads_running:'MySQL 活跃线程（个）',mysql_queries:'MySQL 累计查询（次）',mysql_slow_queries:'MySQL 累计慢查询（次）',mysql_buffer_pool_bytes:'InnoDB 数据占用（字节）',redis_clients:'Redis 客户端连接（个）',redis_blocked:'Redis 阻塞客户端（个）',redis_evicted:'Redis 累计驱逐（键）',redis_ops:'Redis 瞬时操作（次/秒）',redis_used:'Redis 已用内存（字节）',redis_max:'Redis maxmemory 配置（字节）',redis_durable:'Redis 持久与驱逐配置就绪（1/0）',kafka_brokers:'Kafka Broker（个）',kafka_partitions:'Kafka 分区（个）',kafka_leaderless:'Kafka 无 Leader 分区（个）',kafka_under_replicated:'Kafka 副本不足分区（个）'};
 const fmt=value=>typeof value==='number'&&Number.isFinite(value)?new Intl.NumberFormat('zh-CN',{maximumFractionDigits:4}).format(value):'无有效数值';
 // Display translations stay local to the hub; API values remain unchanged.
 const states={SUCCEEDED:'投递成功',SUCCESS:'成功',RECOVERY_PENDING:'恢复中',NEEDS_ATTENTION:'待人工处理',NO_ROUTE:'无投递目标',ROUTED:'已确定目标',PENDING:'待处理',RETRY_WAIT:'等待重试',IN_FLIGHT:'投递中',BOUND:'已绑定',UNBOUND:'已解绑',APPLIED:'已应用',IGNORED:'已忽略',NO_CHANGE:'关系未变化',UID_MISMATCH:'UID 不匹配',CONFLICT:'冲突',STALE:'旧事件已忽略',COMPLETED:'首轮完成',ACCEPTED:'已接管',PERSISTED:'已持久保存',REJECTED:'已拒绝',UNCONFIRMED:'结果未确认',DEPENDENCY_UNAVAILABLE:'依赖不可用',HANDOFF_UNCONFIRMED:'接管未确认',INTERNAL_ERROR:'内部异常',RETRYABLE_FAILURE:'可重试失败',PERMANENT_FAILURE:'永久失败',UNKNOWN:'结果未确认',RUNNING:'运行中',PAUSED:'已暂停',PUBLISHED:'已发布，待确认',ERROR:'错误',WARN:'警告',INFO:'信息',ok:'采样正常',stale:'样本已过期',unavailable:'采样不可用',no_samples:'暂无有效样本'};
 const platforms={PLATFORM_A:'平台 A',PLATFORM_B:'平台 B'};
 const platform=value=>platforms[value]||value||'平台未记录';
 function status(value){const e=C.badge(value);e.textContent=value==null||value===''?'未记录':states[value]||'未知状态（'+value+'）';e.title=String(value??'未记录');e.dataset.status=String(value??'');if(['PERMANENT_FAILURE','REJECTED','INTERNAL_ERROR'].includes(value))e.classList.add('bad');if(['RETRYABLE_FAILURE','UNKNOWN','UNCONFIRMED','UNBOUND','PENDING','IN_FLIGHT'].includes(value))e.classList.add('warning');return e;}
 function line(label,value){const row=C.node('div','','fact-line');C.append(row,C.node('span',label,'fact-label'),value instanceof Node?value:C.node('span',value??'未记录'));return row;}
 function technical(title,entries){const box=C.node('details','','technical-details');C.append(box,C.node('summary',title));for(const [key,value] of entries)C.append(box,line(key,value));return box;}
 const jobs={hub:'中台服务','mock-upstream':'上游交易台','mock-platform1':'平台 A 接收端','mock-platform2':'平台 B 接收端'};
 function instanceName(value){return /^hub-([0-9]+)(?::[0-9]+)?$/.test(value)?'中台实例 '+value.match(/^hub-([0-9]+)/)[1]:value;}
 function scope(labels={},key=''){const box=C.node('div','','metric-scope');const resource=labels.kind||(/^mysql_/.test(key)?'mysql':/^redis_/.test(key)?'redis':/^kafka_/.test(key)?'kafka':'');if(resource)C.append(box,C.node('strong',({mysql:'MySQL',redis:'Redis',kafka:'Kafka'})[resource]||resource));if(labels.job)C.append(box,C.node('div',jobs[labels.job]||labels.job));if(labels.instance){const n=C.node('div',instanceName(labels.instance));n.title=labels.instance;C.append(box,n);}else C.append(box,C.node('small','跨实例聚合'));return box;}
 function dimensions(labels={}){const box=C.node('div','','metric-dimensions');const titles={kind:'资源 / 采样对象',pool:'连接池',platform:'平台',status:'状态',outcome:'结果',topic:'主题',quantile:'分位数',le:'桶上界',code:'HTTP 状态码'};const entries=Object.entries(labels).filter(([k])=>!['instance','job','__name__'].includes(k));for(const [key,value] of entries){const label=key==='status'&&/^\d{3}$/.test(value)?'HTTP 状态码':titles[key]||key;const text=key==='kind'?({mysql:'MySQL',redis:'Redis',kafka:'Kafka',archive:'归档',recovery:'恢复任务',binding_lag:'绑定消费'})[value]||value:key==='platform'?platform(value):['status','outcome'].includes(key)?states[value]||value:value;const fact=line(label,text);fact.lastElementChild.classList.add('resource-dimension-value');C.append(box,fact);}if(!entries.length)C.append(box,C.node('small','未按业务维度细分'));if(Object.keys(labels).length)C.append(box,technical('原始标签',Object.entries(labels)));return box;}
 function emptyMetric(m){if(m.reason==='no_samples')return /p99$/.test(m.key)?'暂无有效耗时样本；当前窗口无法计算 P99。':'暂无有效样本；可能尚未产生数据或抓取次数不足。';return ({query_failed:'采样查询失败；请检查采集服务。',not_sampled:'等待首次后台采样。',expired:'采样结果已过期，请刷新核查。'})[m.reason]||(m.status==='unavailable'?'采样不可用':'暂无样本');}
 function metricStatus(m){const badge=status(m.reason==='no_samples'&&m.status==='unavailable'?'no_samples':m.status);if(m.reason==='query_failed'&&m.samples?.length){const box=C.node('div');C.append(box,badge,C.node('small','最近采样查询失败，保留旧值。'));return box;}return badge;}
 function metricCard(metric){
  const card=C.node('article','','metric'),head=C.node('div','','metric-head'),body=C.node('div','','metric-samples'),foot=C.node('div','','metric-foot');
  C.append(head,C.node('h3',names[metric.key]||metric.key));body.tabIndex=0;body.setAttribute('role','region');body.setAttribute('aria-label',(names[metric.key]||metric.key)+'样本列表');
  if(!metric.samples?.length)C.append(body,C.node('div',emptyMetric(metric),'empty'));
  for(const sample of metric.samples||[]){const item=C.node('div','','metric-sample');C.append(item,C.node('div',fmt(sample.value),'metric-value'),scope(sample.labels,metric.key),dimensions(sample.labels),C.node('small','采样 '+C.time(sample.sampledAt)));C.append(body,item);}
  C.append(foot,metricStatus(metric),C.node('small','来源 '+(metric.source||'未提供')));C.append(card,head,body,foot);return card;
 }
 function metricTable(items){const rows=[];for(const m of items){if(!m.samples?.length)rows.push([names[m.key]||m.key,scope({},m.key),'—',emptyMetric(m),metricStatus(m),m.source||'未提供','未记录']);else for(const sample of m.samples)rows.push([names[m.key]||m.key,scope(sample.labels,m.key),dimensions(sample.labels),fmt(sample.value),metricStatus(m),m.source||'未提供',C.time(sample.sampledAt)]);}return C.table(['指标','实例 / 采集对象','业务维度','值','采样状态','来源','采样时间'],rows,'当前类别没有采样项。缺失不代表零。');}
 function renderMetrics(){if(!metrics)return;const selected=['ingress_rate','first_p99','archive_pending','recovery_tasks'];ids('metric-overview').replaceChildren(...selected.map(key=>metricCard(metrics.metrics.find(m=>m.key===key)||{key,status:'unavailable',samples:[]})));ids('metric-business').replaceChildren(metricTable(metrics.metrics.filter(m=>['ingress_total','route_rate','first_rate','ingress_p99','archive_bytes','archive_oldest','recovery_tasks','recovery_oldest','binding_lag'].includes(m.key))));ids('metric-health').replaceChildren(metricTable(metrics.metrics.filter(m=>['targets_up','observation_up','observation_age','resource_up','resource_age'].includes(m.key))));renderInfra();renderTrend();renderBusinessCharts();}
 function infraItems(){const items=[];for(const metric of metrics.metrics){if(['resource_up','resource_age'].includes(metric.key)){const samples=(metric.samples||[]).filter(sample=>infra==='instances'||sample.labels?.kind===infra);if(samples.length||!metric.samples?.length)items.push({...metric,samples});continue;}const accept=infra==='mysql'?/^(mysql_|db_)/.test(metric.key):infra==='redis'?/^(redis_|archive_)/.test(metric.key):infra==='kafka'?/^(kafka_|binding_lag$|b_lag$|b_paused$)/.test(metric.key):/^(cpu$|rss$|heap$|goroutines$|gc_pause$|db_|targets_up$|observation_)/.test(metric.key);if(accept)items.push(metric);}return items;}
 const instanceKey=labels=>JSON.stringify([labels.job||'',labels.instance]);
 function metricGroup(key){return /^(cpu|goroutines|targets_up|observation_|resource_)/.test(key)?'进程与采样':/^(rss|heap|gc_pause)/.test(key)?'内存与 GC':/^db_/.test(key)?'数据库连接池':/^archive_/.test(key)?'归档积压':/lag|paused/.test(key)?'消费与任务':'中间件运行指标';}
 function sampleCell(m,sample){const box=C.node('div','','resource-value');if(!sample){C.append(box,C.node('span','未采集该实例','muted'));return box;}C.append(box,C.node('strong',fmt(sample.value)),metricStatus(m),C.node('small','采样 '+C.time(sample.sampledAt)),technical('采样详情',[['来源',m.source||'未提供'],...Object.entries(sample.labels||{})]));return box;}
 function resourceTable(headers,rows,empty){const table=C.table(headers.map(h=>h instanceof Node?'':h),rows,empty);table.querySelectorAll('th').forEach((th,i)=>{if(headers[i] instanceof Node)th.replaceChildren(headers[i]);});return table;}
 function renderInfra(){
  if(!metrics)return;const items=infraItems(),targets=new Map(),rows=new Map(),shared=[];
  // Include every available instance in the snapshot so selection survives category changes.
  for(const m of metrics.metrics)for(const sample of m.samples||[])if(sample.labels?.instance)targets.set(instanceKey(sample.labels),sample.labels);
  const activeTargets=new Map();
  for(const m of items){if(!m.samples?.length){shared.push([names[m.key]||m.key,C.node('span',emptyMetric(m),'muted'),metricStatus(m)]);continue;}
   for(const sample of m.samples){const labels=sample.labels||{};if(!labels.instance){shared.push([names[m.key]||m.key,dimensions(labels),sampleCell(m,sample)]);continue;}
    const target=instanceKey(labels);activeTargets.set(target,labels);
    const dims=Object.fromEntries(Object.entries(labels).filter(([key])=>!['instance','job','__name__'].includes(key)).sort(([a],[b])=>a.localeCompare(b)));
    const rowKey=JSON.stringify([m.key,dims]);let row=rows.get(rowKey);if(!row){row={metric:m,dims,cells:new Map()};rows.set(rowKey,row);}const peers=row.cells.get(target)||[];peers.push(sample);row.cells.set(target,peers);
   }
  }
  const sorted=[...targets].sort(([a],[b])=>a.localeCompare(b));
  if(!selectedInstance&&sorted.length)selectedInstance=sorted[0][0];
  const select=ids('infra-instance');select.replaceChildren();for(const [key,labels] of sorted){const option=C.node('option',instanceName(labels.instance)+(labels.job?' · '+(jobs[labels.job]||labels.job):''));option.value=key;select.appendChild(option);}
  if(selectedInstance&&!targets.has(selectedInstance)){const old=JSON.parse(selectedInstance),option=C.node('option',instanceName(old[1])+' · 当前快照缺失');option.value=selectedInstance;select.appendChild(option);}select.value=selectedInstance;select.disabled=!selectedInstance;
  ids('infra-instance-label').hidden=infraMode!=='detail';document.querySelectorAll('[data-infra-mode]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.infraMode===infraMode)));
  const root=ids('infra-metrics');root.replaceChildren();
  const section=C.node('section','','resource-section');section.id=infraMode==='compare'?'infra-comparison':'infra-detail';C.append(section,C.node('h3',infraMode==='compare'?'实例对比':'单实例全部指标'),C.node('p','采样状态、时间与来源随数值展示；未采集不代表零。','muted'));
  if(infraMode==='detail'&&selectedInstance&&!targets.has(selectedInstance))C.append(section,C.node('div','所选实例在当前快照中缺失，请刷新或选择其他实例。','note warning'));
  const columns=sorted.filter(([key,labels])=>infra==='instances'||labels.job==='hub'||/^hub-[0-9]+(?::[0-9]+)?$/.test(labels.instance)||activeTargets.has(key));
  const headers=infraMode==='compare'?['指标 / 分组','业务维度',...columns.map(([key,labels])=>{const b=C.button(instanceName(labels.instance),()=>{selectedInstance=key;infraMode='detail';renderInfra();});b.dataset.instanceLink=key;b.title=(jobs[labels.job]||labels.job||'采集对象')+' · '+labels.instance;return C.append(C.node('div'),b,C.node('small','数值 / 采样状态')); })]:['指标 / 分组','业务维度','数值与采样状态'];
  const data=[];for(const row of [...rows.values()].sort((a,b)=>metricGroup(a.metric.key).localeCompare(metricGroup(b.metric.key),'zh-CN'))){const label=C.node('div');C.append(label,C.node('small',metricGroup(row.metric.key),'resource-group-label'),C.node('strong',names[row.metric.key]||row.metric.key,'resource-metric-title'));const cells=infraMode==='compare'?columns.map(([key])=>key):[selectedInstance];data.push([label,dimensions(row.dims),...cells.map(key=>{const group=C.node('div');for(const sample of row.cells.get(key)||[null])group.appendChild(sampleCell(row.metric,sample));return group;})]);}
  C.append(section,resourceTable(headers,data,infraMode==='detail'?'该类别没有可归属所选实例的指标；请查看下方共享资源。':'该类别没有实例级指标；请查看下方共享资源。'));root.appendChild(section);
  const sharedBox=C.node('section','','resource-section');sharedBox.id='infra-shared';C.append(sharedBox,C.node('h3','共享资源与未归属指标'),C.node('p','已聚合指标仅展示一次，不拆分到各实例、不跨实例相加。无样本的指标暂无法归属。','muted'),C.table(['指标','业务维度 / 缺失说明','值与采样状态'],shared,'当前类别没有共享或未归属指标。'));root.appendChild(sharedBox);
 }
 document.querySelectorAll('[data-infra-mode]').forEach(b=>b.onclick=()=>{infraMode=b.dataset.infraMode;renderInfra();});
 ids('infra-instance').onchange=()=>{selectedInstance=ids('infra-instance').value;renderInfra();};
 const chartColors=['#3976e9','#17a98c','#efab3b','#e56d74','#9274d5','#487d91','#be773a','#64748b'];
 function chartState(m){const box=C.node('div','','chart-state');C.append(box,metricStatus(m));if(m.status==='stale')C.append(box,C.node('strong','历史数据已过期，当前不能当作实时值。','chart-warning'));return box;}
 function chartMetric(key){return metrics.metrics.find(m=>m.key===key)||{key,status:'unavailable',samples:[]};}
 function seriesLabel(labels={}){return Object.entries(labels).map(([key,value])=>key==='platform'?platform(value):key==='status'&&/^\d{3}$/.test(value)?'HTTP '+value:states[value]||value).join(' · ')||'中台汇总';}
 function svgNode(tag,attrs={},text){const e=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,String(v));if(text!=null)e.textContent=text;return e;}
 function historyChart(tr,title,unit){
  const box=C.node('div','','history-chart');C.append(box,chartState({...tr,samples:tr.series?.some(s=>s.points.some(p=>p.value!=null))?[{}]:[]}));
  const all=(tr.series||[]).flatMap(s=>s.points||[]),finite=all.filter(p=>p.value!=null&&Number.isFinite(p.value));
  if(!finite.length){C.append(box,C.node('div',tr.reason==='no_samples'?'暂无有效历史样本；当前窗口没有可绘制数据。':emptyMetric({...tr,key:tr.key}),'empty'));return box;}
  const start=Date.parse(tr.start),end=Date.parse(tr.end);if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start){C.append(box,C.node('div','历史时间范围未确认，不能绘图。','empty'));return box;}
  const width=760,height=220,left=62,right=20,top=18,bottom=42,plotWidth=width-left-right,plotHeight=height-top-bottom,max=Math.max(...finite.map(p=>p.value),1);
  const svg=svgNode('svg',{viewBox:`0 0 ${width} ${height}`,role:'img','aria-label':title+'历史折线，单位'+unit,class:'history-svg'});
  for(const ratio of [0,.5,1]){const y=top+plotHeight*(1-ratio);svg.append(svgNode('line',{x1:left,y1:y,x2:width-right,y2:y,stroke:'#e7edf5'}),svgNode('text',{x:left-10,y:y+4,'text-anchor':'end',class:'axis-label'},fmt(max*ratio)));}
  svg.append(svgNode('text',{x:left,y:height-12,class:'axis-label'},C.time(tr.start)),svgNode('text',{x:width-right,y:height-12,'text-anchor':'end',class:'axis-label'},C.time(tr.end)));
  const legend=C.node('div','','chart-legend');
  (tr.series||[]).forEach((series,i)=>{const color=chartColors[i%chartColors.length],label=seriesLabel(series.labels),item=C.node('span',label);item.style.setProperty('--series-color',color);legend.appendChild(item);
   let segment=[],previous=null;const flush=()=>{if(!segment.length)return;const poly=svgNode('polyline',{points:segment.map(p=>p.x+','+p.y).join(' '),fill:'none',stroke:color,'stroke-width':2.3});poly.appendChild(svgNode('title',{},label));svg.appendChild(poly);for(const point of segment){const dot=svgNode('circle',{cx:point.x,cy:point.y,r:2.5,fill:color});dot.appendChild(svgNode('title',{},label+' · '+C.time(point.time)+' · '+fmt(point.value)+' '+unit));svg.appendChild(dot);}segment=[];};
   for(const p of series.points||[]){const time=Date.parse(p.time);if(p.value==null||!Number.isFinite(p.value)||!Number.isFinite(time)||time<start||time>end){flush();previous=null;continue;}if(previous!=null&&tr.step&&time-previous>tr.step*1500)flush();segment.push({x:left+(time-start)/(end-start)*plotWidth,y:top+plotHeight*(1-p.value/max),time:p.time,value:p.value});previous=time;}flush();
  });
  C.append(box,C.node('small','纵轴：'+unit+' · 横轴：实际采样时间'),svg,legend,C.node('small','范围 '+C.time(tr.start)+' — '+C.time(tr.end)+'；步长 '+tr.step+' 秒；空样本保留断点。'));
  return box;
 }
 function chartPanel(key,title,description){const panel=C.node('article','','business-chart');panel.dataset.chart=key;C.append(panel,C.node('h3',title),C.node('p',description,'muted'));return panel;}
 function barChart(m,unit){const box=C.node('div','','chart-bars'),samples=(m.samples||[]).filter(s=>Number.isFinite(s.value)&&s.value>=0);C.append(box,chartState(m));if(!samples.length){C.append(box,C.node('div',emptyMetric(m),'empty'));return box;}const max=Math.max(...samples.map(s=>s.value),1);for(const [i,sample] of samples.entries()){const row=C.node('div','','chart-bar-row'),head=C.node('div','','chart-bar-head'),track=C.node('div','','chart-bar-track'),fill=C.node('div','','chart-bar-fill');C.append(head,C.node('span',seriesLabel(sample.labels)),C.node('strong',fmt(sample.value)+' '+unit));fill.style.width=(sample.value/max*100)+'%';fill.style.background=chartColors[i%chartColors.length];track.appendChild(fill);C.append(row,head,track);box.appendChild(row);}return box;}
 function distributionChart(m){const box=C.node('div','','distribution');C.append(box,chartState(m));const samples=(m.samples||[]).filter(s=>Number.isFinite(s.value)&&s.value>=0);if(!samples.length){C.append(box,C.node('div',emptyMetric(m),'empty'));return box;}const grouped=new Map();for(const sample of samples){const label=states[sample.labels?.outcome]||sample.labels?.outcome||'结果未记录';grouped.set(label,(grouped.get(label)||0)+sample.value);}const total=[...grouped.values()].reduce((a,b)=>a+b,0);if(!total){C.append(box,C.node('div','当前窗口无首轮投递流量，暂无结果占比。','empty'));return box;}const svg=svgNode('svg',{viewBox:'0 0 180 180',role:'img','aria-label':'首轮结果速率占比',class:'donut-chart'}),legend=C.node('div','','donut-legend');let offset=0;for(const [[label,value],i] of [...grouped].map((entry,i)=>[entry,i])){if(!value)continue;const percent=value/total*100,color=chartColors[i%chartColors.length],circle=svgNode('circle',{cx:90,cy:90,r:64,fill:'none',stroke:color,'stroke-width':22,pathLength:100,'stroke-dasharray':`${percent} ${100-percent}`,'stroke-dashoffset':-offset,transform:'rotate(-90 90 90)'});circle.appendChild(svgNode('title',{},label+' '+fmt(percent)+'%'));svg.appendChild(circle);const item=C.node('div',label+' · '+fmt(percent)+'% ('+fmt(value)+' 次/秒)');item.style.setProperty('--series-color',color);legend.appendChild(item);offset+=percent;}svg.appendChild(svgNode('text',{x:90,y:88,'text-anchor':'middle',class:'donut-total'},fmt(total)),svgNode('text',{x:90,y:110,'text-anchor':'middle',class:'axis-label'},'次 / 秒'));C.append(box,svg,legend);return box;}
 function renderBusinessCharts(){
  const root=ids('business-charts');if(!root||!metrics)return;root.replaceChildren();
  const rate=chartPanel('first-rate','平台首轮投递速率','中台观察到的首轮结果，按平台与结果区分；单位为次/秒。');rate.appendChild(barChart(chartMetric('first_rate'),'次/秒'));
  const distribution=chartPanel('first-distribution','首轮结果分布','以所选窗口平均速率计算占比；跨平台目标尝试口径，不是消息成功率或窗口累计条数。');distribution.appendChild(distributionChart(chartMetric('first_rate')));
  const p99=chartPanel('first-p99','首轮 P99 历史','从接入到首轮投递结果；平台 B 截止 Broker 确认，不代表消费完成。');const trend=metrics.trends?.find(tr=>tr.key==='first_p99');p99.appendChild(trend?historyChart(trend,'首轮 P99','秒'):C.node('div','暂无有效历史采样；请刷新快照。','empty'));
  const recovery=chartPanel('recovery','恢复任务分布','当前快照，按平台与状态展示任务条数；共享责任不跨实例累加。');recovery.appendChild(barChart(chartMetric('recovery_tasks'),'条'));
  const backlog=chartPanel('backlog','归档与绑定积压','各项量纲独立展示，绑定消费为中台自身订阅的绑定事件。');for(const key of ['archive_pending','archive_bytes','archive_oldest','binding_lag']){const m=chartMetric(key),item=C.node('div','','backlog-stat');C.append(item,C.node('span',names[key]),C.node('strong',m.samples?.length?m.samples.map(s=>fmt(s.value)).join(' / '):emptyMetric(m)),metricStatus(m));backlog.appendChild(item);}
  const firstTrend=chartPanel('first-rate-history','平台首轮速率历史','实际历史采样，按平台与结果分线，缺失点不补零。');const rateTrend=metrics.trends?.find(tr=>tr.key==='first_rate');firstTrend.appendChild(rateTrend?historyChart(rateTrend,'平台首轮速率','次 / 秒'):C.node('div','暂无有效历史采样；请刷新快照。','empty'));
  C.append(root,rate,distribution,p99,recovery,backlog,firstTrend);
 }
 function renderTrend(){const trend=metrics?.trends?.find(tr=>tr.key==='ingress_rate');if(trend){ids('metric-trend').replaceChildren(historyChart(trend,'入口速率','请求 / 秒'));return;}const rows=history.get(ids('metric-window').value)||[];if(rows.length<2){ids('metric-trend').replaceChildren(C.node('div','暂无趋势；至少等待两次不同时间的有效入口速率采样。','empty'));return;}const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 700 170');svg.setAttribute('class','chart');svg.setAttribute('role','img');svg.setAttribute('aria-label','当前页面采样的入口速率趋势');const max=Math.max(...rows.map(x=>x.value),1),poly=document.createElementNS('http://www.w3.org/2000/svg','polyline');poly.setAttribute('fill','none');poly.setAttribute('stroke','currentColor');poly.setAttribute('stroke-width','2');poly.setAttribute('points',rows.map((x,i)=>(12+i/(rows.length-1)*676)+','+(150-x.value/max*130)).join(' '));svg.appendChild(poly);ids('metric-trend').replaceChildren(svg,C.node('small',C.time(rows[0].time)+' — '+C.time(rows.at(-1).time)+' · '+rows.length+' 次有效快照'));}
 async function loadMetrics(){const buttons=[ids('refresh-metrics'),ids('refresh-infra')];if(buttons.some(b=>b.disabled))return;for(const b of buttons)b.disabled=true;try{const [data,config]=await Promise.allSettled([C.request('demo/ops/metrics?window='+ids('metric-window').value),C.request('demo/ops/config')]);if(config.status==='fulfilled'){const safe=C.safeUrl(config.value.grafanaUrl);if(safe){ids('grafana-link').href=safe;ids('grafana-link').removeAttribute('aria-disabled');ids('grafana-status').textContent='独立看板 · 新标签页';}else{ids('grafana-link').removeAttribute('href');ids('grafana-link').setAttribute('aria-disabled','true');ids('grafana-status').textContent='Grafana 未配置或地址不安全';}}else ids('grafana-status').textContent='Grafana 配置查询失败';if(data.status==='rejected')throw data.reason;if(!Array.isArray(data.value.metrics))throw Error('指标响应格式未确认');metrics={...data.value,metrics:data.value.metrics.filter(m=>!['receipts','b_lag','b_paused'].includes(m.key)).map(m=>({...m,samples:(m.samples||[]).filter(sample=>!sample.labels?.job||sample.labels.job==='hub')}))};const sample=metrics.metrics.find(m=>m.key==='ingress_rate'&&m.status==='ok');if(sample?.samples?.length){const valid=sample.samples.filter(s=>Number.isFinite(s.value)),time=valid.map(s=>s.sampledAt).sort().at(-1);if(time){const h=history.get(metrics.window)||[];if(h.at(-1)?.time!==time)h.push({time,value:valid.reduce((sum,s)=>sum+s.value,0)});history.set(metrics.window,h.slice(-120));}}const message='快照 '+C.time(metrics.sampledAt)+' · 窗口 '+metrics.window+'；每项采样状态单独显示；采样正常不代表业务投递成功。';ids('metrics-status').textContent=message;ids('infra-status').textContent=message;renderMetrics();}catch(e){ids('metrics-status').textContent='刷新失败：'+e.message+'；上次快照保留'+(metrics?'（原窗口 '+metrics.window+'）':'')+'，当前不能当作实时值。';ids('infra-status').textContent=ids('metrics-status').textContent;if(metrics){metrics={...metrics,metrics:metrics.metrics.map(m=>({...m,status:'stale',reason:'query_failed'})),trends:metrics.trends?.map(tr=>({...tr,status:'stale',reason:'query_failed'}))};renderMetrics();}}finally{for(const b of buttons)b.disabled=false;}}
 ids('refresh-metrics').onclick=loadMetrics;ids('refresh-infra').onclick=loadMetrics;ids('metric-window').onchange=loadMetrics;
 document.querySelectorAll('[data-infra]').forEach(b=>b.onclick=()=>{infra=b.dataset.infra;document.querySelectorAll('[data-infra]').forEach(x=>x.setAttribute('aria-selected',String(x===b)));renderInfra();});
 function platformResults(message){
  const box=C.node('div','','platform-results');C.append(box,line('路由',status(message.routeStatus)));
  const tasks=message.platformId?[message]:message.platformResults||[];
  if(!tasks.length){C.append(box,C.node('small',message.routeStatus==='NO_ROUTE'?'本条不外发':'当前记录未提供平台明细'));return box;}
  const counts=new Map();for(const task of tasks){const key=task.currentState||'';counts.set(key,(counts.get(key)||0)+1);}
  const result=C.node('div','','delivery-summary');C.append(result,C.node('span','投递','fact-label'));
  for(const [state,count] of counts){const badge=status(state);badge.appendChild(document.createTextNode(' × '+count));C.append(result,badge);}C.append(box,result);
  const details=C.node('details','','platform-details');C.append(details,C.node('summary','详情'));
  for(const task of tasks){const item=C.node('div','','platform-result');C.append(item,C.node('strong',platform(task.platformId)),status(task.currentState));if(task.platformUid)C.append(item,line('固定 UID',task.platformUid));if(task.platformId==='PLATFORM_B'&&task.currentState==='SUCCEEDED')C.append(item,C.node('small','Broker 已确认；消费另查'));C.append(details,item);}
  C.append(box,details);return box;
 }
 function messageRows(items){return items.map(v=>[C.link(v.msgId||'无消息标识','?msgId='+encodeURIComponent(v.msgId||'')+'#chain'),v.traceId||'未记录',v.clientId||'—',status(v.status||v.currentState||v.reason),platformResults(v),C.time(v.receivedAt||v.createdAt),C.button('查看链路',()=>openChain(v.msgId))]);}
 function renderQuery(data){const target=ids('message-table');if(!data){target.replaceChildren(C.node('div','查询未返回可用记录，请查看上方状态。','empty'));return;}const items=Array.isArray(data.items)?data.items:[data];if(items[0]?.eventId&&!items[0]?.msgId)target.replaceChildren(C.table(['事件标识','结果','原因','处理时间'],items.map(v=>[v.eventId,status(v.result||v.reason),v.reason||'—',C.time(v.processedAt||v.createdAt)])));else if(items[0]?.attemptNo)target.replaceChildren(attemptTable(items));else target.replaceChildren(C.table(['msgId','traceId','客户','当前状态','路由与平台投递','接入时间','操作'],messageRows(items)));ids('manual-panel').hidden=!actionButtons.length&&!pendingIntent;}
 function renderRuntime(data){ids('runtime-facts').replaceChildren(data?C.table(['平台','本实例配置状态'],(data.platforms||[]).map(v=>[platform(v.platformId),status(v.state)])):C.node('div','配置查询不可用','empty'));}
 window.ConsoleHub={renderQuery,renderRuntime};
 let traceVersion=0,traceCursor='',traceValue='',tracePage=0,selectedTraceKey='';
 function clearChain(){
  chainVersion++;relatedVersion++;chain=null;attemptPages.clear();
  const chart=ids('chain-waterfall');chart.replaceChildren();ids('chain-detail').replaceChildren(chart);ids('attempts-detail').replaceChildren();selectedTraceKey='';
  ids('related-receipts').replaceChildren(C.node('div','选择固定平台 UID 查询相关实收。','empty'));
  renderActions(null);ids('manual-panel').hidden=!pendingIntent;
 }
 function resetChainSearch(){
  traceVersion++;traceCursor='';traceValue='';tracePage=0;clearChain();
  ids('chain-trace-results').hidden=true;ids('chain-matches').replaceChildren();
  ids('chain-trace-next').disabled=true;ids('load-chain').disabled=false;
  const isTrace=ids('chain-kind').value==='traceId';
  ids('chain-query-label').textContent=isTrace?'完整 Trace ID':'完整 msgId';
  ids('chain-id').placeholder=isTrace?'精确匹配；多条消息分别选择':'输入完整消息标识';
  ids('chain-status').textContent='输入标识后查询；不会重新提交或投递。';
 }
 function openChain(id,fromTrace=false){
  if(!id)return;
  if(!fromTrace){ids('chain-kind').value='msgId';resetChainSearch();ids('chain-id').value=id;}
  location.hash='chain';loadChain(id);
 }
 async function lookupTrace(more=false){
  const trace=more?traceValue:ids('chain-id').value.trim();if(!trace)return;
  const version=++traceVersion,cursor=more?traceCursor:'';clearChain();
  if(!more){traceValue=trace;traceCursor='';tracePage=0;ids('chain-matches').replaceChildren();}
  ids('chain-trace-results').hidden=false;ids('chain-trace-next').disabled=true;ids('load-chain').disabled=true;
  for(const button of ids('chain-matches').querySelectorAll('button'))button.disabled=true;
  ids('chain-status').textContent='正在按 Trace ID 查找消息…';ids('chain-match-status').textContent='正在读取匹配记录…';
  try{
   const q=new URLSearchParams({traceId:trace,limit:'20'});if(cursor)q.set('cursor',cursor);
   const data=await C.request('demo/messages?'+q);if(version!==traceVersion)return;
   traceCursor=data.nextCursor||'';tracePage=more?tracePage+1:1;
   ids('chain-matches').replaceChildren(C.table(['消息标识','当前状态','接入时间','操作'],(data.items||[]).map(item=>[
    item.msgId,status(item.status),C.time(item.receivedAt),C.button('查看耗时链路',()=>openChain(item.msgId,true))
   ]),'暂无匹配记录。'));
   ids('chain-match-status').textContent=(data.items||[]).length?'第 '+tracePage+' 页 · 本页 '+data.items.length+' 条；Trace ID 可能对应多条消息，请选择具体消息。':'暂无匹配记录；不能证明未接收或未发送，请核对标识和同步状态。';
   ids('chain-status').textContent='Trace ID 查找完成；选择具体消息后显示耗时链路。';
   const url=new URL(location.href);url.searchParams.delete('msgId');url.searchParams.set('traceId',trace);url.hash='chain';historyReplace(url);
  }catch(e){if(version!==traceVersion)return;ids('chain-status').textContent='Trace ID 查询失败：'+e.message;
   ids('chain-match-status').textContent=(more?'翻页失败；保留原本页匹配记录，可重试同一页。':'查询失败；尚未取得匹配记录，不能判断消息不存在。');
  }finally{if(version===traceVersion){ids('load-chain').disabled=false;ids('chain-trace-next').disabled=!traceCursor;for(const button of ids('chain-matches').querySelectorAll('button'))button.disabled=false;}}
 }
 function stage(title,value,sub){const n=C.node('div','','flow-node');C.append(n,C.node('strong',title),value instanceof Node?value:C.node('div',value),C.node('small',sub));return n;}
 async function loadChain(id=ids('chain-id').value.trim()){if(!id){ids('chain-status').textContent='请填写完整 msgId。';return;}clearChain();const version=chainVersion;ids('load-chain').disabled=true;ids('chain-status').textContent='正在读取权威持久状态…';try{const data=await C.request('demo/messages/'+encodeURIComponent(id));if(version!==chainVersion)return;chain=data;drawChain(data);ids('chain-status').textContent='已取得权威持久记录。查询不会重新提交或投递。';const url=new URL(location.href);url.searchParams.set('msgId',id);url.hash='chain';historyReplace(url);await Promise.all((data.platformResults||[]).map(task=>loadAttempts(task,false,version)));}catch(e){if(version===chainVersion){chain=null;ids('chain-status').textContent=e.message+'；保留原标识核查，不自动重交。';ids('chain-detail').prepend(C.node('div','未取得持久记录，不能据此判断外部未收到。','note warning'));}}finally{if(version===chainVersion)ids('load-chain').disabled=false;}}
 function historyReplace(url){window.history.replaceState(null,'',url);}
 function drawChain(data){let snapshot=data.routeSnapshot;if(typeof snapshot==='string'){try{snapshot=JSON.parse(snapshot);}catch(_){snapshot={};}}snapshot=snapshot||{};const root=C.node('section','','panel'),body=C.node('div','','panel-body');C.append(body,C.node('h2',data.msgId),C.kv([['traceId',data.traceId],['客户号',data.clientId],['首轮事实',status(data.firstStatus)],['当前状态',status(data.status)],['路由状态',status(data.routeStatus)]]));C.append(body,ids('chain-waterfall'));const flow=C.node('div','','flow');C.append(flow,stage('入口接收',C.time(data.receivedAt),'仅已记录接入时间'),stage('绑定路由',C.time(snapshot.resolvedAt),'按快照固定目标'),stage('数据库保存',C.time(data.storedAt),'数据库插入时间，非精确 commit'));for(const task of data.platformResults||[])C.append(flow,stage(platform(task.platformId),status(task.currentState),task.platformId==='PLATFORM_B'?'成功仅代表 Broker 已确认；消费入库另查':'外部接收独立核查'));C.append(body,flow);const timeline=C.node('div','','timeline');for(const [title,value,field] of [['入口收到',data.receivedAt,'receivedAt'],['路由快照解析',snapshot.resolvedAt,'resolvedAt'],['数据库插入',data.storedAt,'storedAt']]){const event=C.node('div','','event'),head=C.node('div','','timeline-heading');C.append(head,C.node('strong',title),C.node('time',C.time(value)));C.append(event,head,technical('技术详情',[[field,value||'未记录']]));if(field==='storedAt')C.append(event,C.node('small','数据库插入时间，非精确事务提交时间。'));C.append(timeline,event);}C.append(body,timeline,C.node('div','记录范围：校验完成、Redis 确认、HTTP 响应完成未记录逐消息时间，不生成推算时间。','note timeline-note'));if(data.routeStatus==='NO_ROUTE')C.append(body,C.node('div','无目标：本条成交不外发、不补推；新绑定仅影响新成交。','note warning'));
 const tasks=data.platformResults||[];C.append(body,C.table(['平台','固定 UID','首轮结果','当前状态','核查'],tasks.map(task=>[platform(task.platformId),task.platformUid,status(task.firstOutcome),status(task.currentState),C.button('查询相关实收',()=>related(task,data))])));for(const button of body.querySelectorAll('button'))button.dataset.related='true';C.append(body,C.node('h3','人工操作历史'),C.table(['operationId','对象','操作人','原因','周期 / 保存状态','记录时间'],(data.manualOperations||[]).map(op=>[op.operationId,op.targetId,op.operator,op.reason,C.append(C.node('div'),C.node('div','第 '+op.retryCycle+' 周期'),status(op.state)),C.time(op.createdAt)])));if(data.manualOperationsTruncated)C.append(body,C.node('div','人工历史达到接口范围上限；当前展示已截断。','note warning'));C.append(root,body);ids('chain-detail').replaceChildren(root);renderWaterfall();renderActions(data);ids('manual-panel').hidden=!actionButtons.length&&!pendingIntent;}
 function attemptSummary(a){const raw=a.summary;if(!raw)return C.node('span','未记录摘要','muted');const box=C.node('div','','attempt-summary');let title='',facts=[];
  const match=raw.match(/^http_status=(\d{3});\s*outcome=(SUCCESS|RETRYABLE_FAILURE|PERMANENT_FAILURE|UNKNOWN)$/);
  if(match&&match[2]===a.outcome&&(a.sendStarted===true||a.sendStarted===1)){const conclusions={SUCCESS:'接收端确认成功',RETRYABLE_FAILURE:'接收端返回可重试失败',PERMANENT_FAILURE:'接收端返回永久失败',UNKNOWN:'接收端结果未确认'};if(match[2]!=='SUCCESS'||/^2/.test(match[1])){title=conclusions[match[2]];facts=[['HTTP 状态码',match[1]]];}}
  else if(raw==='broker write confirmed'&&a.outcome==='SUCCESS'&&(a.sendStarted===true||a.sendStarted===1)){title='Broker 已确认写入';facts=[['后续核查','消费实收另查']];}
  if(title){C.append(box,C.node('strong',title));for(const [k,v] of facts)box.appendChild(line(k,v));C.append(box,technical('查看原始摘要',[['原文',raw]]));}else C.append(box,C.node('small','原始结果摘要'),C.node('p',raw));return box;
 }
 function attemptTable(items){return C.table(['周期 / 尝试','开始','结束','是否开始外发','结果','摘要'],items.map(a=>[String(a.cycleNo)+' / '+a.attemptNo,C.time(a.startedAt),C.time(a.finishedAt),a.sendStarted===true||a.sendStarted===1?'是':a.sendStarted===false||a.sendStarted===0?'否':'未记录',status(a.outcome),attemptSummary(a)]));}
 // This is a view of recorded message facts, not inferred distributed spans.
 function traceTime(value){
  if(typeof value!=='string'||!value.trim())return null;
  const d=new Date(value);return Number.isFinite(d.getTime())&&d.getUTCFullYear()>1?d.getTime():null;
 }
 const traceDuration=value=>new Intl.NumberFormat('zh-CN',{maximumFractionDigits:3}).format(value)+' ms';
 function traceRows(){
  let snapshot=chain.routeSnapshot;if(typeof snapshot==='string'){try{snapshot=JSON.parse(snapshot);}catch(_){snapshot=null;}}
  const rows=[['入口收到',chain.receivedAt,'入口接收事件，不推算校验耗时'],['路由快照解析',snapshot?.resolvedAt,'路由完成事件，不推算查询耗时'],['数据库插入',chain.storedAt,'数据库插入时间，非精确事务提交时间']].map(([label,time,note],i)=>({key:'event-'+i,kind:'event',label,start:traceTime(time),end:traceTime(time),rawTime:time,note}));
  for(const task of chain.platformResults||[]){
   const items=[...(attemptPages.get(task.deliveryId)?.items||[])].sort((a,b)=>Number(a.attemptNo)-Number(b.attemptNo));let previous=null;
   for(const attempt of items){
    const start=traceTime(attempt.startedAt),end=traceTime(attempt.finishedAt),valid=start!==null&&end!==null&&end>=start;
    if(valid&&previous&&Number.isInteger(Number(attempt.attemptNo))&&Number(attempt.attemptNo)===Number(previous.attempt.attemptNo)+1&&start>previous.end){
     rows.push({key:task.deliveryId+'-gap-'+attempt.attemptNo,kind:'gap',label:platform(task.platformId)+' · 尝试间隔',start:previous.end,end:start,task,note:'两次相邻尝试之间的时间差；原因未记录，不能据此判定排队或退避耗时。'});
    }
    rows.push({key:task.deliveryId+'-attempt-'+attempt.attemptNo,kind:'attempt',label:platform(task.platformId)+' · 周期 '+(attempt.cycleNo??'未记录')+' / 尝试 '+(attempt.attemptNo??'未记录'),start:valid?start:null,end:valid?end:null,task,attempt,note:valid?'尝试执行区间；不等同于纯网络耗时。':'时间缺失或异常，未绘制耗时区间。'});
    previous=valid?{attempt,end}:null;
   }
  }
  return rows;
 }
 function traceDetails(row){
  const box=ids('trace-selection');box.replaceChildren(C.node('strong',row.label));
  if(row.kind==='attempt'){
   const a=row.attempt,sent=a.sendStarted===true||a.sendStarted===1?'已开始外发':a.sendStarted===false||a.sendStarted===0?'未开始外发':'外发状态未记录';
   C.append(box,C.kv([['平台',platform(row.task.platformId)],['固定 UID',row.task.platformUid],['投递标识',row.task.deliveryId],['周期 / 尝试',(a.cycleNo??'未记录')+' / '+(a.attemptNo??'未记录')],['开始',C.time(a.startedAt)],['结束',C.time(a.finishedAt)],['尝试耗时',row.start===null?'时间缺失或异常':traceDuration(row.end-row.start)],['是否外发',sent],['结果',status(a.outcome)]]),attemptSummary(a));
   if(row.task.platformId==='PLATFORM_B')C.append(box,C.node('p','Broker 确认与平台消费入库分别核查；此区间不包含平台内部消费过程。','muted'));
  }else C.append(box,C.kv(row.kind==='event'?[['事件时间',C.time(row.rawTime)]]:[['开始',C.time(new Date(row.start).toISOString())],['结束',C.time(new Date(row.end).toISOString())],['间隔',traceDuration(row.end-row.start)]]));
  C.append(box,C.node('p',row.note,'muted'));
 }
 function renderWaterfall(){
  const target=ids('chain-waterfall');if(!chain||!target)return;
  const card=C.node('section','','trace-card'),head=C.node('div','','trace-heading');
  C.append(head,C.node('div','TRACE / 单消息耗时','eyebrow'),C.node('h2','耗时链路'),C.node('p','同一横轴展示已记录事件与投递尝试；点击条目查看详情。等待与处理分开核查。','muted'));
  C.append(card,head);
  const rows=traceRows(),valid=rows.filter(r=>r.start!==null),times=valid.flatMap(r=>[r.start,r.end]);
  const start=times.length?Math.min(...times):null,end=times.length?Math.max(...times):null,span=start===null?0:end-start;
  const attempts=rows.filter(r=>r.kind==='attempt'),timed=attempts.filter(r=>r.start!==null),longest=timed.length?Math.max(...timed.map(r=>r.end-r.start)):null;
  const summary=C.node('div','','trace-stats');
  for(const [label,value] of [['已记录时间跨度',start===null?'暂无有效时间':traceDuration(span)],['已加载尝试',String(attempts.length)+' 次'],['最长已加载尝试',longest===null?'暂无有效时间':traceDuration(longest)]])C.append(summary,C.append(C.node('div'),C.node('small',label),C.node('strong',value)));
  C.append(card,summary);
  const ranges=C.node('div','','trace-ranges');
  for(const task of chain.platformResults||[]){
   const s=attemptPages.get(task.deliveryId),n=s?.items.length||0;
   const message=!s?'等待读取':s.busy?'正在读取尝试…':s.error?'查询失败；保留已加载 '+n+' 条，不能视为完整历史。':s.cursor?(n>=100?'已到100条展示上限；仍有后续历史。':'已加载 '+n+' 条；仍有后续历史。'):'已读完接口分页，共 '+n+' 条；未记录的内部阶段不在图中。';
   const row=C.node('div','','trace-range');C.append(row,C.node('strong',platform(task.platformId)),C.node('span',message,s?.error?'trace-error':'muted'));
   if(s&&!s.busy&&n<100&&(s.error||s.cursor)){
    const version=chainVersion,b=C.button(s.error?'重试读取 '+platform(task.platformId):'继续加载 '+platform(task.platformId)+' 尝试',()=>loadAttempts(task,!!s.cursor,version));b.dataset.traceMore=task.deliveryId;C.append(row,b);
   }
   C.append(ranges,row);
  }
  if(!(chain.platformResults||[]).length)C.append(ranges,C.node('div',chain.routeStatus==='NO_ROUTE'?'无投递目标：本条消息不外发。':'没有平台投递明细；不能推断已发生外发。','note'));
  C.append(card,ranges);
  const scroller=C.node('div','','trace-scroll');scroller.tabIndex=0;scroller.setAttribute('aria-label','耗时链路时间轴，可横向滚动');
  const plot=C.node('div','','trace-plot'),axis=C.node('div','','trace-axis');
  C.append(axis,C.node('span','步骤 / 事件'));const ticks=C.node('div','','trace-ticks');
  for(const fraction of [0,.25,.5,.75,1]){const tick=C.node('span',start===null?'—':'+'+traceDuration(span*fraction));tick.style.left=(fraction*100)+'%';C.append(ticks,tick);}
  C.append(axis,ticks,C.node('span','耗时 / 状态'));C.append(plot,axis);
  for(const row of rows){
   const el=C.node('div','','trace-row');el.dataset.kind=row.kind;
   if(row.task)el.dataset.deliveryId=row.task.deliveryId;if(row.attempt)el.dataset.attemptNo=row.attempt.attemptNo;
   const button=C.button(row.label,()=>{selectedTraceKey=row.key;for(const peer of plot.querySelectorAll('.trace-row button'))peer.setAttribute('aria-pressed',String(peer===button));traceDetails(row);});button.setAttribute('aria-pressed',String(row.key===selectedTraceKey));
   const lane=C.node('div','','trace-lane');lane.setAttribute('aria-hidden','true');lane.onclick=()=>button.click();lane.style.cursor='pointer';
   if(row.start!==null){const mark=C.node('span','','trace-mark trace-'+row.kind);mark.style.left=(span?(row.start-start)/span*100:0)+'%';mark.style.width=(span?(row.end-row.start)/span*100:0)+'%';if(row.attempt)mark.dataset.outcome=row.attempt.outcome||'';C.append(lane,mark);}
   const info=C.node('div','','trace-row-value');
   if(row.kind==='event')C.append(info,C.node('span',row.start===null?'时间缺失或异常':'事件点'));
   else if(row.kind==='gap')C.append(info,C.node('strong',traceDuration(row.end-row.start)),C.node('small','原因未记录'));
   else C.append(info,C.node('strong',row.start===null?'时间缺失或异常':traceDuration(row.end-row.start)),status(row.attempt.outcome));
   C.append(el,button,lane,info);C.append(plot,el);
  }
  C.append(scroller,plot);C.append(card,scroller);
  if(start===null)C.append(card,C.node('p','暂无有效时间，不能绘制耗时区间。','note warning'));
  else C.append(card,C.node('p',C.time(new Date(start).toISOString())+' → '+C.time(new Date(end).toISOString())+'；相对时间以最早有效记录为起点。','trace-time-range'));
  const detail=C.node('div','','trace-selection');detail.id='trace-selection';detail.setAttribute('role','region');detail.setAttribute('aria-label','所选链路条目详情');C.append(card,detail);
  C.append(card,C.node('p','跨度包含尝试间隔，不等于程序持续执行时间。校验、Redis确认、响应完成及外部平台内部过程未记录；不同实例时钟偏差可能影响先后顺序。','trace-footnote'));
  target.replaceChildren(card);
  const selected=rows.find(r=>r.key===selectedTraceKey);
  if(selected)traceDetails(selected);else detail.textContent='选择事件点或投递条目，查看起止时间、结果和详细说明。';
 }
 async function loadAttempts(task,more,version){
  if(version!==chainVersion)return;
  let state=attemptPages.get(task.deliveryId);
  if(!state){
   const panel=C.node('section','','panel'),body=C.node('div','','panel-body');
   C.append(body,C.node('h2',platform(task.platformId)+' · 投递尝试'),C.node('small',task.deliveryId));
   C.append(panel,body);ids('attempts-detail').appendChild(panel);
   state={items:[],cursor:'',body,busy:false,error:''};attemptPages.set(task.deliveryId,state);
  }
  if(state.busy)return;
  state.busy=true;state.error='';renderWaterfall();
  const old=state.body.querySelector('button');if(old)old.disabled=true;
  try{
   const q=new URLSearchParams({limit:'20'});if(more&&state.cursor)q.set('cursor',state.cursor);
   const data=await C.request('demo/deliveries/'+encodeURIComponent(task.deliveryId)+'/attempts?'+q);
   if(version!==chainVersion)return;
   state.items=(more?state.items:[]).concat(data.items||[]).slice(0,100);state.cursor=data.nextCursor||'';
   state.body.replaceChildren(C.node('h2',platform(task.platformId)+' · 投递尝试'),C.node('small',task.deliveryId),attemptTable(state.items),C.node('small','当前 '+state.items.length+' 条；每页 20 条，页面最多展示 100 条。有下一页不代表已展示全部历史。'));
   if(state.cursor&&state.items.length<100){
    const b=C.button('下一页尝试',()=>loadAttempts(task,true,version));b.dataset.attemptNext=task.deliveryId;state.body.appendChild(b);
   }else if(state.cursor)state.body.appendChild(C.node('div','页面已到 100 条范围上限；历史仍有后续，当前展示已截断。','note warning'));
  }catch(e){
   if(version!==chainVersion)return;
   state.error=e.message;state.body.appendChild(C.node('div','尝试查询失败：'+e.message+'；不等于没有外发。','note error'));if(old)old.disabled=false;
  }finally{state.busy=false;if(version===chainVersion)renderWaterfall();}
 }

 async function related(task,data){const version=++relatedVersion;const target=ids('related-receipts');target.replaceChildren(C.node('div','正在查询 '+platform(task.platformId)+' 固定 UID 的相关实际接收…','empty'));try{const prefix=task.platformId==='PLATFORM_A'?'/mock/p1/':task.platformId==='PLATFORM_B'?'/mock/p2/':null;if(!prefix||!task.platformUid||!data.traceId)throw Error('缺少平台、固定 UID 或 traceId，无法限定相关实收');const q=new URLSearchParams({uid:task.platformUid,traceId:data.traceId,limit:'20'}),result=await C.request(prefix+'demo/received?'+q);if(version!==relatedVersion)return;target.replaceChildren(C.node('div','相关实收：固定 UID + traceId 不能唯一匹配本条成交或某次尝试。'+(task.platformId==='PLATFORM_B'?' Broker ACK 和独立消费落库分别核查。':''),'note warning'),C.table(['receiveId','固定 UID','traceId','平台接收时间'],(result.items||[]).map(r=>[r.receiveId,r.userUid,r.traceId,C.time(r.receivedAt)]),'当前范围暂无相关实收；不能证明未发送。'));if(result.nextCursor)target.appendChild(C.link('在平台消息中心继续核查 ↗',prefix+'?uid='+encodeURIComponent(task.platformUid)+'&traceId='+encodeURIComponent(data.traceId)+'#receipts'));}catch(e){if(version!==relatedVersion)return;target.replaceChildren(C.node('div','相关实收查询失败：'+e.message+'；不等于平台没有收到。','note error'));}}
 ids('chain-form').onsubmit=e=>{e.preventDefault();if(ids('chain-kind').value==='traceId')lookupTrace();else loadChain();};
 ids('chain-kind').onchange=resetChainSearch;ids('chain-id').oninput=resetChainSearch;
 ids('chain-trace-next').onclick=()=>lookupTrace(true);
 const eventNames={ingress_invalid:'成交校验拒绝',VALIDATION_REJECTED:'成交校验拒绝',ingress_body_too_large:'请求体超限',ingress_rate_limited:'入口限流',ingress_in_flight_limited:'入口并发超限',ingress_unconfirmed:'成交接管未确认',ingress_first_abnormal:'首轮投递异常',ingress_panic:'成交入口异常',recovery_result:'恢复投递异常',runtime_warning:'运行警告',runtime_error:'运行异常',binding_fetch_unavailable:'绑定事件读取失败',binding_commit_unconfirmed:'绑定提交未确认',binding_cache_invalidation_failed:'绑定缓存失效失败',platform2_fetch_unavailable:'平台 B 消息读取失败',platform2_commit_unconfirmed:'平台 B 提交未确认',hub_stopped:'中台进程停止'};
 function diagnosticRow(v){
  const owner=C.node('div','','diagnostic-owner');C.append(owner,C.node('strong',instanceName(v.instance||'实例未记录')),technical('技术详情',[['实例原名',v.instance],['启动标识 bootId',v.bootId],['事件代码',v.kind]]));
  const event=C.node('div','','diagnostic-event');C.append(event,C.node('strong',eventNames[v.kind]||v.kind||'事件类型未记录'),C.node('p',v.message||'未提供摘要'));const tags=C.node('div','','diagnostic-tags');if(v.platform)C.append(tags,C.node('span',platform(v.platform),'badge'));if(v.status)C.append(tags,line('状态',status(v.status)));if(v.outcome)C.append(tags,line('结果',status(v.outcome)));C.append(event,tags);
  const refs=C.node('div','','diagnostic-refs');for(const [key,label] of [['requestId','请求标识'],['msgId','消息标识'],['traceId','跟踪标识'],['deliveryId','投递标识']])if(v[key])C.append(refs,line(label,key==='msgId'?C.link(v[key],'?msgId='+encodeURIComponent(v[key])+'#chain'):v[key]));if(!refs.childNodes.length)C.append(refs,C.node('small','未记录关联标识'));
  return [C.time(v.time),status(v.level),owner,event,refs];
 }
 ids('diagnostic-form').onsubmit=async e=>{e.preventDefault();const b=ids('query-logs');if(b.disabled)return;b.disabled=true;const q=new URLSearchParams({limit:ids('log-limit').value});for(const [id,key] of [['log-level','level'],['log-instance','instance'],['log-q','q']])if(ids(id).value)q.set(key,ids(id).value);try{const data=await C.request('demo/ops/diagnostics?'+q);const rows=(data.items||[]).map(diagnosticRow);ids('diagnostics').replaceChildren(C.table(['时间','级别','所属实例','事件与摘要','关联信息'],rows,'查询范围内暂无匹配诊断；不代表所有实例没有异常。'));const retention=C.node('div');retention.appendChild(C.node('p','最近诊断每实例最多 '+(data.retention?.capacityPerInstance||1000)+' 条；重启清空，覆盖后不可恢复。业务持久历史仍在消息记录中。'));for(const instance of data.instances||[]){const item=C.node('div','','diagnostic-retention-item');C.append(item,C.node('p',instanceName(instance.instance)+'（'+instance.instance+'） · '+(instance.status==='ok'?'可查询':'不可达')+' · 已保留 '+(instance.retained??'未知')+' / '+(instance.capacity??'未知')+' · 已覆盖 '+(instance.dropped??'未知')+' · 最早 '+C.time(instance.oldestAt)),technical('启动标识 bootId',[['bootId',instance.bootId||'未知']]));retention.appendChild(item);}ids('diagnostic-retention').replaceChildren(retention);ids('diagnostic-status').textContent='返回 '+rows.length+' 条最近诊断；拒绝 / 未确认不等于已接管。实例可达性与保留范围如下。';}catch(e){ids('diagnostic-status').textContent='诊断查询失败：'+e.message+'；保留旧展示，不将失败解释成无日志。';}finally{b.disabled=false;}};
 const attention={deliveries:{label:'投递',phase:'idle',hasData:false,cursor:'',base:'deliveries?currentState=NEEDS_ATTENTION&limit=20',target:'attention-deliveries',button:'attention-delivery-next'},routes:{label:'路由',phase:'idle',hasData:false,cursor:'',base:'messages?routeStatus=NEEDS_ATTENTION&limit=20',target:'attention-routes',button:'attention-route-next'}};
 function attentionStatus(){const summaries=[];for(const s of Object.values(attention)){let text;if(s.phase==='loading')text=s.label+'查询中'+(s.hasData?'；上次结果尚未刷新':'；尚未取得结果');else if(s.phase==='error')text=s.label+'查询失败：'+s.error+'；'+(s.hasData?'旧结果已过期，保留供核查':'未取得该分支结果')+'；不代表无待处理。';else if(s.phase==='ok')text=s.label+'查询成功；当前页 '+s.count+' 条'+(s.cursor?'，还有下一页。':'。');else text=s.label+'等待查询。';ids(s.target+'-status').textContent=text;ids(s.target).dataset.stale=String(s.hasData&&s.phase!=='ok');ids(s.button).disabled=s.phase==='loading'||!s.cursor;summaries.push(text);}ids('attention-status').textContent=summaries.join('\n');ids('load-attention').disabled=Object.values(attention).some(s=>s.phase==='loading');}
 async function attentionPage(key,more){const s=attention[key];if(s.phase==='loading')return;s.phase='loading';attentionStatus();try{const data=await C.request('demo/'+s.base+(more&&s.cursor?'&cursor='+encodeURIComponent(s.cursor):''));let items=data.items||[];if(key==='routes')items=items.filter(v=>v.routeStatus==='NEEDS_ATTENTION');const rows=items.map(v=>{const b=C.button('选择原任务恢复',()=>{renderActions(v);ids('manual-panel').hidden=false;ids('manual-panel').scrollIntoView({behavior:'smooth'});});return [v.deliveryId||v.msgId,v.msgId,v.platformId?platform(v.platformId):'路由解析',status(v.currentState||v.routeStatus),b];});ids(s.target).replaceChildren(C.table(['原任务 / 消息标识','msgId','平台','状态','操作'],rows,key==='routes'?'本页没有路由待人工；仍可继续下一页查看。':'本页没有投递待人工。'));s.cursor=data.nextCursor||'';s.hasData=true;s.count=items.length;s.phase='ok';s.error='';}catch(e){s.phase='error';s.error=e.message;}finally{attentionStatus();}}
 ids('load-attention').onclick=async()=>{if(ids('load-attention').disabled)return;await Promise.all([attentionPage('deliveries',false),attentionPage('routes',false)]);};ids('attention-delivery-next').onclick=()=>attentionPage('deliveries',true);ids('attention-route-next').onclick=()=>attentionPage('routes',true);
 attentionStatus();
 let bindingCursor='',bindingBase='';async function bindings(more=false){ids('hub-load-bindings').disabled=true;ids('hub-binding-next').disabled=true;try{if(!more){const q=new URLSearchParams({limit:'20'});for(const [id,key] of [['hub-binding-client','clientId'],['hub-binding-platform','platformId'],['hub-binding-uid','platformUid']])if(ids(id).value)q.set(key,ids(id).value);bindingBase='demo/bindings?'+q;}const data=await C.request(bindingBase+(more&&bindingCursor?'&cursor='+encodeURIComponent(bindingCursor):''));bindingCursor=data.nextCursor||'';ids('hub-bindings').replaceChildren(C.table(['客户号','平台','固定 UID','当前状态','版本','更新时间'],(data.items||[]).map(v=>[v.clientId,platform(v.platformId),v.platformUid,status(v.status),v.revision,C.time(v.updatedAt)])));ids('hub-binding-status').textContent='已取得权威绑定关系。新绑定不改写旧消息路由。';}catch(e){ids('hub-binding-status').textContent=e.message;}finally{ids('hub-load-bindings').disabled=false;ids('hub-binding-next').disabled=!bindingCursor;}}
 ids('hub-binding-form').onsubmit=e=>{e.preventDefault();bindings();};ids('hub-binding-next').onclick=()=>bindings(true);
 const query=new URLSearchParams(location.search);if(query.get('msgId')){ids('chain-id').value=query.get('msgId');if(location.hash==='#chain')ids('chain-status').textContent='已载入 msgId；输入令牌后点击查询完整链路。';}else if(query.get('traceId')){ids('chain-kind').value='traceId';resetChainSearch();ids('chain-id').value=query.get('traceId');ids('chain-status').textContent='已载入 Trace ID；输入令牌后点击查询。';}
 C.onNavigate=view=>{if(!['messages','chain','attention'].includes(view)&&!pendingIntent)ids('manual-panel').hidden=true;};C.navigate();
})();
