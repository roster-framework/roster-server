// Deterministic integration fixture for the EXISTING Node runtime. No model API.
export function createAgents() {
  return ['designer', 'developer'].map(id => ({
    spec: { id, name: id, description: 'Node core integration fixture', statuses: ['complete'], events: id === 'designer' ? ['complete'] : [], skills: [], rules: [], capabilities: [] },
    async execute(invocation, context) {
      context.log(`${id}: ${invocation.context.request}`);
      return {status:'complete', summary:`${id} finished`, output:{request:invocation.context.request}, events:id==='designer'?[{type:'complete',data:{summary:'Ready for implementation'}}]:[], artifacts:[], metrics:{}};
    },
  }));
}
