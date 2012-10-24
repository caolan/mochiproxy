Example use:

```erlang
%% proxy /api/* to http://localhost:5984/*
mochiproxy:handle_req("/api", "http://localhost:5984", Req).
```
