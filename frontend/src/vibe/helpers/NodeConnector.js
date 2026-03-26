// Real-time SSE API (Node). Override with REACT_APP_NODE_API_URL.

const APIConnection = {
  endpoint:
    process.env.REACT_APP_NODE_API_URL ||
    'https://lsp-backend-node.herokuapp.com/',
};
export default APIConnection;
