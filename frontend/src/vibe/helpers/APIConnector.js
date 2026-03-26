// Persistent / historical API (Flask). Override with REACT_APP_FLASK_API_URL in dev/prod.

const APIConnection = {
  endpoint:
    process.env.REACT_APP_FLASK_API_URL ||
    'http://lsp-backend-flask.herokuapp.com/',
};
export default APIConnection;
