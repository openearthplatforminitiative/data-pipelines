local hostMatch = std.extVar('hostMatch');

{
  apiVersion: 'traefik.io/v1alpha1',
  kind: 'IngressRoute',
  metadata: {
    name: 'dagster-webserver',
    namespace: 'dagster',
  },
  spec: {
    entryPoints: ['websecure'],
    routes: [{
      kind: 'Rule',
      match: 'Host(`' + hostMatch + '`) && PathPrefix(`/dagster`)',
      services: [{
        kind: 'Service',
        name: 'dagster-dagster-webserver',
        port: 80,
      }],
      middlewares: [{
        name: 'dagster-oauth2-proxy@kubernetescrd',
      }],
    }],
  },
}
