import {createAgentTemplates} from '/runtime/presets/codex/agent.mjs';

export function createAgents(options = {}) {
  const {role, model, project, mode, executable, timeout_seconds: timeoutSeconds} = options;
  const expectedProject = role === 'planner' ? '/planning' : '/project';
  const expectedMode = role === 'reviewer' ? 'read-only' : 'workspace-write';
  if (!['planner', 'implementer', 'reviewer'].includes(role) ||
      typeof model !== 'string' || !model.trim() ||
      project !== expectedProject || mode !== expectedMode ||
      executable !== '/fixture/cli' ||
      !Number.isInteger(timeoutSeconds) || timeoutSeconds < 30 || timeoutSeconds > 3600) {
    throw new Error('Invalid bridge fixture options');
  }
  const template = createAgentTemplates({executable})[0];
  return [template.create({
    id: role,
    name: role,
    parameters: {model, project, mode, timeout_seconds: timeoutSeconds},
  })];
}
