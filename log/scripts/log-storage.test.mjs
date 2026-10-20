import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const cli=fileURLToPath(new URL('./logs.mjs',import.meta.url));
test('storage CLI forwards native write authority, strict empty action and fixed response failures',async()=>{
 const seen=[];let status=200;const server=createServer((req,res)=>{let body='';req.on('data',part=>body+=part);req.on('end',()=>{seen.push({path:req.url,method:req.method,protocol:req.headers['x-roster-service-protocol'],authorization:req.headers.authorization,body});res.writeHead(status,{'Content-Type':'application/json'});res.end(JSON.stringify(status===200?{ok:true}:{private:'provider body'}));});});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const invoke=args=>new Promise((resolve,reject)=>{const child=spawn(process.execPath,[cli,'storage',...args],{windowsHide:true,env:{...process.env,ROSTER_LOG_URL:'http://127.0.0.1:'+server.address().port,ROSTER_MEMORY_TOKEN:'read-fixture',ROSTER_MEMORY_WRITE_TOKEN:'write-fixture'},stdio:['ignore','pipe','pipe']});let output='',errors='';child.stdout.on('data',part=>output+=part);child.stderr.on('data',part=>errors+=part);child.on('error',reject);child.on('close',code=>resolve({code,output,errors}));});
 try {
  assert.equal((await invoke(['status'])).code,0);assert.equal((await invoke(['reclaim'])).code,0);
  assert.deepEqual(seen,[{path:'/v1/storage',method:'GET',protocol:'roster-session-ingest/1',authorization:'Bearer write-fixture',body:''},{path:'/v1/storage/reclaim',method:'POST',protocol:'roster-session-ingest/1',authorization:'Bearer write-fixture',body:'{}'}]);
  const before=seen.length;for(const args of [[],['invalid'],['reclaim','extra'],['status','extra']])assert.notEqual((await invoke(args)).code,0);assert.equal(seen.length,before);
  status=503;const failed=await invoke(['reclaim']);assert.notEqual(failed.code,0);assert.ok(!failed.errors.includes('provider body')&&!failed.errors.includes('write-fixture')&&!failed.output.includes('private'));
 }finally{await new Promise(resolve=>server.close(resolve));}
});
