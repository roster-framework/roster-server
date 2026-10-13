// Optional finite acceptance against independently running real runtime/resource servers.
// Configure the core using examples/local-stack.json after registering resource `notes`.
import assert from 'node:assert/strict';
const base=process.env.CORE_URL||'http://127.0.0.1:7350';
async function command(name,input={}) {const res=await fetch(`${base}/api/commands/${name}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(input)});const value=await res.json();assert.equal(res.status,200,`${name}: ${JSON.stringify(value)}`);return value;}
async function until(read,predicate,label) {const deadline=Date.now()+20000;while(Date.now()<deadline){const value=await read();if(predicate(value))return value;await new Promise(r=>setTimeout(r,50));}throw Error(`${label} timed out`);}
const call=(id,node,operation,input)=>({id,target:{namespace:'nodes',id:node},operation,input});
const nodes=await command('nodes.list');assert.equal(nodes.length,3);assert.ok(nodes.every(n=>!('binding'in n)));
await command('calls.submit',call('design-start','designer','run',{request:'Design the small feature'}));
await until(()=>command('calls.list',{node_id:'developer'}),v=>v.some(s=>s.value.lifecycle.state==='succeeded'),'event subscription');
const documentCall=await command('calls.submit',call('publish-notes','notes','publish',{arguments:{source_kind:'upload',title:'Notes',body:'Resource event integration',change_id:'verify-publish',expected_version:0}}));assert.equal(documentCall.value.lifecycle.state,'queued');
await until(()=>command('calls.get',{id:'publish-notes'}),v=>v.value.lifecycle.state==='succeeded','resource publish');
await command('calls.submit',call('read-notes','notes','read',{arguments:{version:0}}));
const read=await until(()=>command('calls.get',{id:'read-notes'}),v=>v.value.lifecycle.state==='succeeded','resource read');assert.equal(read.value.result.output.body,'Resource event integration');
await command('calls.submit',call('update-notes','notes','publish',{arguments:{source_kind:'upload',title:'Notes',body:'Resource event integration',change_id:'verify-update',expected_version:1}}));
await until(()=>command('calls.get',{id:'update-notes'}),v=>v.value.lifecycle.state==='succeeded','resource update');
await until(()=>command('logs.query',{source:{namespace:'nodes',id:'notes'}}),v=>v.entries.some(e=>e.event.name==='resource.updated'),'resource event ingress');
await until(()=>command('calls.list',{node_id:'developer'}),v=>v.filter(s=>s.value.lifecycle.state==='succeeded').length>=2,'resource subscription');
const ref=id=>({namespace:'nodes',id});const reader={namespace:'participants',id:'danny'};
const receiver=id=>({ref:ref(id),receiver:{target:ref(id),operation:'run',input:{values:{},pointers:{request:'/text'}}}});
for (const [id,participants,recipients] of [ ['one-to-one',[{ref:reader},receiver('developer')],[ref('developer')]], ['meeting',[{ref:reader},receiver('designer'),receiver('developer')],[ref('designer'),ref('developer')]] ]) {
  await command('conversations.create',{id,title:id,participants,closed:false});
  const message=await command('conversations.send',{id,message_id:`${id}-message`,author:reader,recipients,text:'Discuss the implementation plan'});assert.equal(message.call_ids.length,recipients.length);
  const view=await until(()=>command('conversations.export',{id}),v=>v.messages.filter(m=>m.reply_to===`${id}-message`).length===recipients.length,`${id} replies`);assert.equal(view.conversation.value.participants.length,participants.length);
}
const logs=await command('logs.query',{limit:500});assert.ok(logs.entries.some(e=>e.event.name==='log'));assert.ok(logs.entries.some(e=>e.event.name==='message'));
console.log(JSON.stringify({status:'passed',checks:['real runtime HTTP','real resource HTTP','result event → subscription → call','resource change → subscription → call','shared logs','one-to-one replies','multiparty meeting export']}));
