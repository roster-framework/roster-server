import {readFile} from 'node:fs/promises';
import {retentionRequest} from './log-retention.mjs';
const [command,arg,file]=process.argv.slice(2);
const origin=process.env.ROSTER_LOG_URL||'http://127.0.0.1:7341';
const u=new URL(origin);
if(u.protocol!=='https:'&&!(u.protocol==='http:'&&['127.0.0.1','localhost','[::1]'].includes(u.hostname)))throw Error('Remote logging requires HTTPS');
let path,method='POST',input,body,read=false;
switch(command){
 case 'storage':if(process.argv.length!==4||!['status','reclaim'].includes(arg))throw Error('storage status | storage reclaim');path=arg==='status'?'/v1/storage':'/v1/storage/reclaim';method=arg==='status'?'GET':'POST';if(method==='POST')body='{}';break;
 case 'append':path='/v1/records';input=arg;break;
 case 'compact':path='/v1/compact';input=arg;break;
 case 'features':if(!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/.test(arg||''))throw Error('Stream ID required');path='/v1/streams/'+arg+'/features';method='PUT';input=file;break;
 case 'agents':path='/v1/agents';if(arg==='list')method='GET';else if(arg==='save')input=file;else throw Error('agents list | agents save FILE');break;
 case 'query':path='/v1/invoke';read=true;input=arg;break;
 case 'retention':({path,method,body}=await retentionRequest(process.argv.slice(3)));break;
 default:throw Error('append FILE | compact FILE | features ID FILE | agents list | agents save FILE | query FILE | retention ... | storage status | storage reclaim');
}
if(method!=='GET'&&!input&&!body)throw Error('JSON file required');
const token=read?process.env.ROSTER_MEMORY_TOKEN:process.env.ROSTER_MEMORY_WRITE_TOKEN;
if(!token)throw Error('Logging service credential required');
const response=await fetch(new URL(path,u),{method,headers:{Authorization:'Bearer '+token,'Content-Type':'application/json','X-Roster-Service-Protocol':read?'roster-memory/1':'roster-session-ingest/1'},body:body??(input?await readFile(input,'utf8'):undefined),redirect:'error',signal:AbortSignal.timeout(60000)});
if(!response.ok)throw Error('Logging response '+response.status);
console.log(JSON.stringify(await response.json(),null,2));
