import assert from 'node:assert/strict';
import {test} from 'node:test';
import {retentionRequest} from './log-retention.mjs';

test('retention CLI delegates fixed native requests and preserves the policy CAS', async () => {
  assert.deepEqual(await retentionRequest(['status','release']), {path:'/v1/streams/release/retention',method:'GET'});
  assert.deepEqual(await retentionRequest(['prune','release','3']), {path:'/v1/streams/release/retention/prune',method:'POST',body:'{"version":3}'});
  assert.deepEqual(await retentionRequest(['set','release','policy.json'], async () => '{"version":3,"retain_hours":24}'), {path:'/v1/streams/release/retention',method:'PUT',body:'{"version":3,"retain_hours":24}'});
  assert.deepEqual(await retentionRequest(['audit','release','10','2']), {path:'/v1/streams/release/retention/audit?after=10&limit=2',method:'GET'});
  for (const args of [['status','../key'],['prune','release'],['prune','release','-1'],['prune','release','1','extra'],['audit','release','0','201'],['status','release','extra']]) await assert.rejects(retentionRequest(args));
  for (const policy of [{version:0,retain_hours:23},{version:0,retain_hours:24,now:'spoof'}, {version:-1,retain_hours:24},null,[]]) await assert.rejects(retentionRequest(['set','release','file'],async () => JSON.stringify(policy)));
});
