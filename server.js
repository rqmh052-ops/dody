const { PeerServer } = require('peer');

const port = process.env.PORT || 9000;

PeerServer({
  port,
  path: '/',
  proxied: true,
  allow_discovery: false
});

console.log('Peer server running on port ' + port);
