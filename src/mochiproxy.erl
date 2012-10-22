% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.
%
% This code is based on the CouchDB proxy module: couch_httpd_proxy.erl, also
% available under this license: http://couchdb.org


-module(mochiproxy).

-export([handle_req/3]).

-include_lib("ibrowse/include/ibrowse.hrl").

-define(TIMEOUT, infinity).
-define(PKT_SIZE, 4096).


handle_req(From, To, Req) ->
    Method = get_method(Req),
    Url = get_url(From, To, Req),
    Version = get_version(Req),
    Headers = get_headers(Req),
    Body = get_body(Req),
    Options = [
        {http_vsn, Version},
        {headers_as_is, true},
        {response_format, binary},
        {stream_to, {self(), once}}
    ],
    case ibrowse:send_req(Url, Headers, Method, Body, Options, ?TIMEOUT) of
        {ibrowse_req_id, ReqId} ->
            stream_response(Req, To, ReqId);
        {error, Reason} ->
            throw({error, Reason})
    end.


get_method(Req) ->
    case Req:get(method) of
        Method when is_atom(Method) ->
            list_to_atom(string:to_lower(atom_to_list(Method)));
        Method when is_list(Method) ->
            list_to_atom(string:to_lower(Method));
        Method when is_binary(Method) ->
            list_to_atom(string:to_lower(binary_to_list(Method)))
    end.

split_url(Url) ->
    [X || X <- re:split(Url,"\/",[{return,list}]), X /= []].

strip_prefix([H|T1], [H|T2]) ->
    strip_prefix(T1, T2);
strip_prefix([], Rest) ->
    {ok, Rest};
strip_prefix(Pre, _) ->
    {error, "URL does not match prefix: /" ++ string:join(Pre, "/")}.

strip_url_prefix(Pre, Path) ->
    Pre2 = split_url(Pre),
    Path2 = split_url(Path),
    {ok, Result} = strip_prefix(Pre2, Path2),
    "/" ++ string:join(Result, "/").

get_url(From, To, Req) when is_binary(To) ->
    get_url(From, binary_to_list(To), Req);
get_url(From, To, Req) ->
    BaseUrl = case mochiweb_util:partition(To, "/") of
        {[], "/", _} -> absolute_uri(Req, To);
        _ -> To
    end,
    Path = re:split(Req:get(path),"\/",[{return,list}]),
    ProxyPrefix = "/" ++ hd(Path),
    RequestedPath = strip_url_prefix(From, Req:get(raw_path)),
    case mochiweb_util:partition(RequestedPath, ProxyPrefix) of
        {[], ProxyPrefix, []} ->
            BaseUrl;
        {[], ProxyPrefix, [$/ | DestPath]} ->
            remove_trailing_slash(BaseUrl) ++ "/" ++ DestPath;
        {[], ProxyPrefix, DestPath} ->
            remove_trailing_slash(BaseUrl) ++ "/" ++ DestPath;
        _Else ->
            throw({invalid_url_path, {ProxyPrefix, RequestedPath}})
    end.

get_version(Req) ->
    Req:get(version).


get_headers(Req) ->
    to_ibrowse_headers(mochiweb_headers:to_list(Req:get(headers)), []).

to_ibrowse_headers([], Acc) ->
    lists:reverse(Acc);
to_ibrowse_headers([{K, V} | Rest], Acc) when is_atom(K) ->
    to_ibrowse_headers([{atom_to_list(K), V} | Rest], Acc);
to_ibrowse_headers([{K, V} | Rest], Acc) when is_list(K) ->
    case string:to_lower(K) of
        "content-length" ->
            to_ibrowse_headers(Rest, [{content_length, V} | Acc]);
        % This appears to make ibrowse too smart.
        %"transfer-encoding" ->
        %    to_ibrowse_headers(Rest, [{transfer_encoding, V} | Acc]);
        _ ->
            to_ibrowse_headers(Rest, [{K, V} | Acc])
    end.

get_body(Req) ->
    case Req:get(method) of
        'GET' ->
            fun() -> eof end;
        'HEAD' ->
            fun() -> eof end;
        'DELETE' ->
            fun() -> eof end;
        _ ->
            case Req:get(body_length) of
                undefined ->
                    <<>>;
                {unknown_transfer_encoding, Unknown} ->
                    exit({unknown_transfer_encoding, Unknown});
                chunked ->
                    {fun stream_chunked_body/1, {init, Req, 0}};
                0 ->
                    <<>>;
                Length when is_integer(Length) andalso Length > 0 ->
                    {fun stream_length_body/1, {init, Req, Length}};
                Length ->
                    exit({invalid_body_length, Length})
            end
    end.


remove_trailing_slash(Url) ->
    rem_slash(lists:reverse(Url)).

rem_slash([]) ->
    [];
rem_slash([$\s | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\t | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\r | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\n | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$/ | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash(RevUrl) ->
    lists:reverse(RevUrl).


stream_chunked_body({init, MReq, 0}) ->
    % First chunk, do expect-continue dance.
    init_body_stream(MReq),
    stream_chunked_body({stream, MReq, 0, [], ?PKT_SIZE});
stream_chunked_body({stream, MReq, 0, Buf, BRem}) ->
    % Finished a chunk, get next length. If next length
    % is 0, its time to try and read trailers.
    {CRem, Data} = read_chunk_length(MReq),
    case CRem of
        0 ->
            BodyData = lists:reverse(Buf, Data),
            {ok, BodyData, {trailers, MReq, [], ?PKT_SIZE}};
        _ ->
            stream_chunked_body(
                {stream, MReq, CRem, [Data | Buf], BRem-size(Data)}
            )
    end;
stream_chunked_body({stream, MReq, CRem, Buf, BRem}) when BRem =< 0 ->
    % Time to empty our buffers to the upstream socket.
    BodyData = lists:reverse(Buf),
    {ok, BodyData, {stream, MReq, CRem, [], ?PKT_SIZE}};
stream_chunked_body({stream, MReq, CRem, Buf, BRem}) ->
    % Buffer some more data from the client.
    Length = lists:min([CRem, BRem]),
    NewState = case MReq:recv(Length, ?TIMEOUT) of
        {ok, Data} when size(Data) == CRem ->
            case MReq:recv(2, ?TIMEOUT) of
                {ok, <<"\r\n">>} ->
                    {stream, MReq, 0, [<<"\r\n">>, Data | Buf], BRem-Length-2};
                _ ->
                    exit(normal)
            end;
        {ok, Data} ->
            {stream, MReq, CRem-Length, [Data | Buf], BRem-Length};
        _ ->
            exit(normal)
    end,
    stream_chunked_body(NewState);
stream_chunked_body({trailers, MReq, Buf, BRem}) when BRem =< 0 ->
    % Empty our buffers and send data upstream.
    BodyData = lists:reverse(Buf),
    {ok, BodyData, {trailers, MReq, [], ?PKT_SIZE}};
stream_chunked_body({trailers, MReq, Buf, BRem}) ->
    % Read another trailer into the buffer or stop on an
    % empty line.
    Socket = MReq:get(socket),
    mochiweb_socket:setopts(Socket, [{packet, line}]),
    case MReq:recv(0, ?TIMEOUT) of
        {ok, <<"\r\n">>} ->
            mochiweb_socket:setopts(Socket, [{packet, raw}]),
            BodyData = lists:reverse(Buf, <<"\r\n">>),
            {ok, BodyData, eof};
        {ok, Footer} ->
            mochiweb_socket:setopts(Socket, [{packet, raw}]),
            NewState = {trailers, MReq, [Footer | Buf], BRem-size(Footer)},
            stream_chunked_body(NewState);
        _ ->
            exit(normal)
    end;
stream_chunked_body(eof) ->
    % Tell ibrowse we're done sending data.
    eof.


stream_length_body({init, Req, Length}) ->
    % Do the expect-continue dance
    init_body_stream(Req),
    stream_length_body({stream, Req, Length});
stream_length_body({stream, _Req, 0}) ->
    % Finished streaming.
    eof;
stream_length_body({stream, Req, Length}) ->
    BufLen = lists:min([Length, ?PKT_SIZE]),
    case Req:recv(BufLen) of
        <<>> -> eof;
        Bin -> {ok, Bin, {stream, Req, Length-BufLen}}
    end.


init_body_stream(Req) ->
    Expect = case Req:get_header_value("expect") of
        undefined ->
            undefined;
        Value when is_list(Value) ->
            string:to_lower(Value)
    end,
    case Expect of
        "100-continue" ->
            Req:start_raw_response({100, gb_trees:empty()});
        _Else ->
            ok
    end.


read_chunk_length(Req) ->
    Socket = Req:get(socket),
    mochiweb_socket:setopts(Socket, [{packet, line}]),
    case Req:recv(0, ?TIMEOUT) of
        {ok, Header} ->
            mochiweb_socket:setopts(Socket, [{packet, raw}]),
            Splitter = fun(C) ->
                C =/= $\r andalso C =/= $\n andalso C =/= $\s
            end,
            {Hex, _Rest} = lists:splitwith(Splitter, binary_to_list(Header)),
            {mochihex:to_int(Hex), Header};
        _ ->
            exit(normal)
    end.


stream_response(Req, ProxyDest, ReqId) ->
    receive
        {ibrowse_async_headers, ReqId, "100", _} ->
            % ibrowse doesn't handle 100 Continue responses which
            % means we have to discard them so the proxy client
            % doesn't get confused.
            ibrowse:stream_next(ReqId),
            stream_response(Req, ProxyDest, ReqId);
        {ibrowse_async_headers, ReqId, Status, Headers} ->
            {Source, Dest} = get_urls(Req, ProxyDest),
            FixedHeaders = fix_headers(Source, Dest, Headers, []),
            case body_length(FixedHeaders) of
                chunked ->
                    {ok, Resp} = start_chunked_response(
                        Req, list_to_integer(Status), FixedHeaders
                    ),
                    ibrowse:stream_next(ReqId),
                    stream_chunked_response(Req, ReqId, Resp),
                    {ok, Resp};
                Length when is_integer(Length) ->
                    {ok, Resp} = start_response_length(
                        Req, list_to_integer(Status), FixedHeaders, Length
                    ),
                    ibrowse:stream_next(ReqId),
                    stream_length_response(Req, ReqId, Resp),
                    {ok, Resp};
                _ ->
                    {ok, Resp} = start_response(
                        Req, list_to_integer(Status), FixedHeaders
                    ),
                    ibrowse:stream_next(ReqId),
                    stream_length_response(Req, ReqId, Resp),
                    % XXX: MochiWeb apparently doesn't look at the
                    % response to see if it must force close the
                    % connection. So we help it out here.
                    erlang:put(mochiweb_request_force_close, true),
                    {ok, Resp}
            end
    end.


stream_chunked_response(Req, ReqId, Resp) ->
    receive
        {ibrowse_async_response, ReqId, {error, Reason}} ->
            throw({error, Reason});
        {ibrowse_async_response, ReqId, Chunk} ->
            send_chunk(Resp, Chunk),
            ibrowse:stream_next(ReqId),
            stream_chunked_response(Req, ReqId, Resp);
        {ibrowse_async_response_end, ReqId} ->
            last_chunk(Resp)
    end.


stream_length_response(Req, ReqId, Resp) ->
    receive
        {ibrowse_async_response, ReqId, {error, Reason}} ->
            throw({error, Reason});
        {ibrowse_async_response, ReqId, Chunk} ->
            send(Resp, Chunk),
            ibrowse:stream_next(ReqId),
            stream_length_response(Req, ReqId, Resp);
        {ibrowse_async_response_end, ReqId} ->
            ok
    end.


get_urls(Req, ProxyDest) ->
    %SourceUrl = absolute_uri(Req, "/" ++ hd(Req:get(path))),
    Path = re:split(Req:get(path),"\/",[{return,list}]),
    SourceUrl = absolute_uri(Req, hd(Path)),
    Source = parse_url(binary_to_list(iolist_to_binary(SourceUrl))),
    case (catch parse_url(ProxyDest)) of
        Dest when is_record(Dest, url) ->
            {Source, Dest};
        _ ->
            DestUrl = absolute_uri(Req, ProxyDest),
            {Source, parse_url(DestUrl)}
    end.


fix_headers(_, _, [], Acc) ->
    lists:reverse(Acc);
fix_headers(Source, Dest, [{K, V} | Rest], Acc) ->
    Fixed = case string:to_lower(K) of
        "location" -> rewrite_location(Source, Dest, V);
        "content-location" -> rewrite_location(Source, Dest, V);
        "uri" -> rewrite_location(Source, Dest, V);
        "destination" -> rewrite_location(Source, Dest, V);
        "set-cookie" -> rewrite_cookie(Source, Dest, V);
        _ -> V
    end,
    fix_headers(Source, Dest, Rest, [{K, Fixed} | Acc]).


rewrite_location(Source, #url{host=Host, port=Port, protocol=Proto}, Url) ->
    case (catch parse_url(Url)) of
        #url{host=Host, port=Port, protocol=Proto} = Location ->
            DestLoc = #url{
                protocol=Source#url.protocol,
                host=Source#url.host,
                port=Source#url.port,
                path=join_url_path(Source#url.path, Location#url.path)
            },
            url_to_url(DestLoc);
        #url{} ->
            Url;
        _ ->
            url_to_url(Source#url{path=join_url_path(Source#url.path, Url)})
    end.


rewrite_cookie(_Source, _Dest, Cookie) ->
    Cookie.


parse_url(Url) when is_binary(Url) ->
    ibrowse_lib:parse_url(binary_to_list(Url));
parse_url(Url) when is_list(Url) ->
    ibrowse_lib:parse_url(binary_to_list(iolist_to_binary(Url))).


join_url_path(Src, Dst) ->
    Src2 = case lists:reverse(Src) of
        "/" ++ RestSrc -> lists:reverse(RestSrc);
        _ -> Src
    end,
    Dst2 = case Dst of
        "/" ++ RestDst -> RestDst;
        _ -> Dst
    end,
    Src2 ++ "/" ++ Dst2.


url_to_url(#url{host=Host, port=Port, path=Path, protocol=Proto} = Url) ->
    LPort = case {Proto, Port} of
        {http, 80} -> "";
        {https, 443} -> "";
        _ -> ":" ++ integer_to_list(Port)
    end,
    LPath = case Path of
        "/" ++ _RestPath -> Path;
        _ -> "/" ++ Path
    end,
    HostPart = case Url#url.host_type of
        ipv6_address ->
            "[" ++ Host ++ "]";
        _ ->
            Host
    end,
    atom_to_list(Proto) ++ "://" ++ HostPart ++ LPort ++ LPath.


body_length(Headers) ->
    case is_chunked(Headers) of
        true -> chunked;
        _ -> content_length(Headers)
    end.


is_chunked([]) ->
    false;
is_chunked([{K, V} | Rest]) ->
    case string:to_lower(K) of
        "transfer-encoding" ->
            string:to_lower(V) == "chunked";
        _ ->
            is_chunked(Rest)
    end.

content_length([]) ->
    undefined;
content_length([{K, V} | Rest]) ->
    case string:to_lower(K) of
        "content-length" ->
            list_to_integer(V);
        _ ->
            content_length(Rest)
    end.

absolute_uri(Req, Path) ->
    Host = host_for_request(Req),
    XSsl = "X-Forwarded-Ssl",
    Scheme = case Req:get_header_value(XSsl) of
        "on" -> "https";
        _ ->
            XProto = "X-Forwarded-Proto",
            case Req:get_header_value(XProto) of
                %% Restrict to "https" and "http" schemes only
                "https" -> "https";
                _ -> case Req:get(scheme) of
                        https -> "https";
                        http -> "http"
                    end
            end
    end,
    Scheme ++ "://" ++ Host ++ Path.

host_for_request(Req) ->
    XHost = "X-Forwarded-Host",
    case Req:get_header_value(XHost) of
        undefined ->
            case Req:get_header_value("Host") of
                undefined ->
                    {ok, {Address, Port}} = case Req:get(socket) of
                        {ssl, SslSocket} -> ssl:sockname(SslSocket);
                        Socket -> inet:sockname(Socket)
                    end,
                    inet_parse:ntoa(Address) ++ ":" ++ integer_to_list(Port);
                Value1 ->
                    Value1
            end;
        Value -> Value
    end.

start_chunked_response(Req, Code, Headers) ->
    Headers2 = http_1_0_keep_alive(Req, Headers),
    Resp = Req:respond({Code, Headers2, chunked}),
    case Req:get(method) of
    'HEAD' -> throw({http_head_abort, Resp});
    _ -> ok
    end,
    {ok, Resp}.

http_1_0_keep_alive(Req, Headers) ->
    KeepOpen = Req:should_close() == false,
    IsHttp10 = Req:get(version) == {1, 0},
    NoRespHeader = no_resp_conn_header(Headers),
    case KeepOpen andalso IsHttp10 andalso NoRespHeader of
        true -> [{"Connection", "Keep-Alive"} | Headers];
        false -> Headers
    end.

start_response_length(Req, Code, Headers, Length) ->
    Resp = Req:start_response_length({Code, Headers, Length}),
    case Req:get(method) of
    'HEAD' -> throw({http_head_abort, Resp});
    _ -> ok
    end,
    {ok, Resp}.

start_response(Req, Code, Headers) ->
    Resp = Req:start_response({Code, Headers}),
    case Req:get(method) of
        'HEAD' -> throw({http_head_abort, Resp});
        _ -> ok
    end,
    {ok, Resp}.

send_chunk(Resp, Data) ->
    case iolist_size(Data) of
    0 -> ok; % do nothing
    _ -> Resp:write_chunk(Data)
    end,
    {ok, Resp}.

last_chunk(Resp) ->
    Resp:write_chunk([]),
    {ok, Resp}.

send(Resp, Data) ->
    Resp:send(Data),
    {ok, Resp}.

no_resp_conn_header([]) ->
    true;
no_resp_conn_header([{Hdr, _}|Rest]) ->
    case string:to_lower(Hdr) of
        "connection" -> false;
        _ -> no_resp_conn_header(Rest)
    end.
