// The CLI only drives the native common use cases; it never opens the server database.
import {readFile} from 'node:fs/promises';

export async function retentionRequest(args, load = file => readFile(file, 'utf8')) {
  const [command, id, value, limit, ...extra] = args;
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/.test(id || '') || extra.length) throw Error('Stream ID required');
  const base = `/v1/streams/${id}/retention`;
  switch (command) {
    case 'status':
      if (value !== undefined || limit !== undefined) break;
      return {path: base, method: 'GET'};
    case 'set': {
      if (!value || limit !== undefined) break;
      const policy = JSON.parse(await load(value));
      if (!policy || Array.isArray(policy) || Object.keys(policy).some(key => !['version', 'retain_hours'].includes(key)) ||
          !Number.isSafeInteger(policy.version) || policy.version < 0 ||
          (policy.retain_hours !== null && (!Number.isInteger(policy.retain_hours) || policy.retain_hours < 24 || policy.retain_hours > 87600))) break;
      return {path: base, method: 'PUT', body: JSON.stringify(policy)};
    }
    case 'prune':
      if (limit !== undefined || !/^(0|[1-9][0-9]*)$/.test(value || '') || !Number.isSafeInteger(Number(value))) break;
      return {path: `${base}/prune`, method: 'POST', body: JSON.stringify({version: Number(value)})};
    case 'audit': {
      const after = value ?? '0', size = limit ?? '100';
      if (!/^(0|[1-9][0-9]*)$/.test(after) || !Number.isSafeInteger(Number(after)) || !/^[1-9][0-9]*$/.test(size) || Number(size) > 200) break;
      return {path: `${base}/audit?after=${after}&limit=${size}`, method: 'GET'};
    }
  }
  throw Error('retention status ID | set ID FILE | prune ID VERSION | audit ID [AFTER] [LIMIT]');
}
