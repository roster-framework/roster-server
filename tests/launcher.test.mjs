import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parseStrict } from '../scripts/launch.mjs';

const launcher = fileURLToPath(new URL('../scripts/launch.mjs', import.meta.url));

test('launcher rejects duplicate JSON keys instead of accepting last-write-wins', () => {
  assert.throws(() => parseStrict('{"schema":"roster-launch/1","schema":"other"}'));
});

test('launcher rejects command forms outside the single config entrypoint', () => {
  const result = spawnSync(process.execPath, [launcher, '--config', 'a.json', '--extra'], { encoding: 'utf8' });
  assert.equal(result.status, 2);
  const lines = result.stdout.trim().split(/\r?\n/u);
  assert.equal(lines.length, 1);
  const output = JSON.parse(lines[0]);
  assert.deepEqual(output, {
    schema: 'roster-launch-result/1', phase: 'failed', component: 'preflight', code: 2,
    children: {}, unresolved: [],
  });
  assert.equal(result.stderr, '');
});
