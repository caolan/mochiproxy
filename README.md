Example use:

```erlang
%% proxy /api/* to couchdb on http://localhost:5984/*
mochiproxy:handle_req("/api", "http://localhost:5984", Req).
```
