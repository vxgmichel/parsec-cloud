# import os
# import zmq
# import zmq.auth
# from zmq.auth.thread import ThreadAuthenticator
# import attr

# from parsec.backend.message import InMemoryMessageComponent
# from parsec.exceptions import ParsecError


# @attr.s(slots=True)
# class RequestContext:
#     userid = attr.ib()
#     msg = attr.ib()
#     frames = attr.ib()


# def _build_request(frames):
#     if len(frames) < 3:
#         return None
#     id_frame, _, msg_frame, *_ = frames
#     try:
#         body = ejson_loads(msg_frame.bytes.decode())
#     except:
#         # Invalid msg
#         return None
#     return RequestContext(
#         id_frame.bytes,
#         msg,
#         frames
#     )


# def _build_response(id, rep):
#     if isinstance(rep, (list, tuple)):
#         repmsg, *frames = rep
#     else:
#         repmsg = rep
#         frames = ()
#     return (id, b'', ejson_dumps(repmsg).encode(), *exframes)


# def run_backend(addr, server_keys, public_keys_dir):
#     cmd_addr = addr
#     # cmd_addr = 'tcp://%s:%s/cmds' % (host, port)
#     # event_addr = 'tcp://%s:%s/events' % (host, port)

#     context = zmq.Context()

#     # Auth is required between core and backend
#     auth = ThreadAuthenticator(context)
#     auth.start()
#     # auth.allow('127.0.0.1')
#     # Tell authenticator to use the certificate in a directory
#     auth.configure_curve(domain='*', location=public_keys_dir)
#     # server_public, server_secret = zmq.auth.load_certificate(SERVER_SECRET)
#     server_public, server_secret = server_keys

#     cmds_router = context.socket(zmq.ROUTER)
#     cmds_router.curve_secretkey = server_secret
#     cmds_router.curve_publickey = server_public
#     cmds_router.curve_server = True  # must come before bind
#     cmds_router.bind(cmd_addr)

#     # events_dealer = context.socket(zmq.DEALER)
#     # events_dealer.curve_secretkey = server_secret
#     # events_dealer.curve_publickey = server_public
#     # events_dealer.curve_server = True # must come before bind
#     # events_dealer.bind(cmd_addr)

#     msgapi = InMemoryMessageComponent()
#     api_cmds_router = {
#         **msgapi.get_dispatcher()
#     }
#     while True:
#         id, _, frames = cmds_router.recv_multipart()
#         print('REQ: ', id, frames)
#         req = _build_request(frames)
#         if not req:
#             cmds_router.send_multipart((id, b'', b'{"status": "bad_msg"}'))
#             continue
#         try:
#             cmd = api_cmds_router[req.msg['cmd']]
#         except KeyError:
#             cmds_router.send_multipart(_build_response(id, {"status": "unknown_cmd"}))
#             continue
#         else:
#             try:
#                 rep = cmd(req)
#             except ParsecError as exc:
#                 rep = exc.to_dict()
#         print('REP: ', id, rep)
#         cmds_router.send_multipart(_build_response(id, rep))
