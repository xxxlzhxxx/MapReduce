============================= test session starts ==============================
platform linux -- Python 3.8.10, pytest-7.2.1, pluggy-1.0.0 -- /home/ziheng/p4/env/bin/python3
cachedir: .pytest_cache
rootdir: /home/ziheng/p4
plugins: mock-3.10.0
collecting ... collected 1 item

tests/test_manager_00.py::test_shutdown 
-------------------------------- live log call ---------------------------------
INFO     mapreduce.manager.__main__:__main__.py:25 Starting manager host=localhost port=6000 pwd=/home/ziheng/p4
FAILED

=================================== FAILURES ===================================
________________________________ test_shutdown _________________________________

__wrapped_mock_method__ = <function NonCallableMock.assert_has_calls at 0x7f296ce08ca0>
args = (<MagicMock name='socket' id='139815895609936'>, [call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>), call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()])
kwargs = {'any_order': True}, __tracebackhide__ = True
msg = "'socket' does not contain all of (call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)...any_order': True}\n  Right contains 1 more item:\n  {'any_order': True}\n  Full diff:\n  - {'any_order': True}\n  + {}"
__mock_self = <MagicMock name='socket' id='139815895609936'>
actual_args = (<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>)
actual_kwargs = {}
introspection = "\nArgs:\nassert (<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>) == ([call(<AddressFamily.AF_INET: 2>, <Socke...any_order': True}\n  Right contains 1 more item:\n  {'any_order': True}\n  Full diff:\n  - {'any_order': True}\n  + {}"
@py_assert2 = ([call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>), call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()],)
@py_assert1 = False
@py_format4 = "(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>) == ([call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREA...().setsockopt(1, 2, 1),\n~-   call().__enter__().bind(('localhost', 6000)),\n~-   call().__enter__().listen()],,\n~  )"

    def assert_wrapper(
        __wrapped_mock_method__: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> None:
        __tracebackhide__ = True
        try:
>           __wrapped_mock_method__(*args, **kwargs)

env/lib/python3.8/site-packages/pytest_mock/plugin.py:444: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 

self = <MagicMock name='socket' id='139815895609936'>
calls = [call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>), call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()]
any_order = True

    def assert_has_calls(self, calls, any_order=False):
        """assert the mock has been called with the specified calls.
        The `mock_calls` list is checked for the calls.
    
        If `any_order` is False (the default) then the calls must be
        sequential. There can be extra calls before or after the
        specified calls.
    
        If `any_order` is True then the calls can be in any order, but
        they must all appear in `mock_calls`."""
        expected = [self._call_matcher(c) for c in calls]
        cause = next((e for e in expected if isinstance(e, Exception)), None)
        all_calls = _CallList(self._call_matcher(c) for c in self.mock_calls)
        if not any_order:
            if expected not in all_calls:
                if cause is None:
                    problem = 'Calls not found.'
                else:
                    problem = ('Error processing expected calls.\n'
                               'Errors: {}').format(
                                   [e if isinstance(e, Exception) else None
                                    for e in expected])
                raise AssertionError(
                    f'{problem}\n'
                    f'Expected: {_CallList(calls)}'
                    f'{self._calls_repr(prefix="Actual").rstrip(".")}'
                ) from cause
            return
    
        all_calls = list(all_calls)
    
        not_found = []
        for kall in expected:
            try:
                all_calls.remove(kall)
            except ValueError:
                not_found.append(kall)
        if not_found:
>           raise AssertionError(
                '%r does not contain all of %r in its call list, '
                'found %r instead' % (self._mock_name or 'mock',
                                      tuple(not_found), all_calls)
            ) from cause
E           AssertionError: 'socket' does not contain all of (call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()) in its call list, found [call().setsockopt(1, 2, 1), call().__enter__(), call().bind(('localhost', 6000)), call().listen(), call().settimeout(1), call().accept(), call().accept().__iter__(), call().__exit__(<class 'ValueError'>, ValueError('not enough values to unpack (expected 2, got 0)'), <traceback object at 0x7f296c3c7400>), call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>), call().setsockopt(1, 2, 1)] instead

/usr/lib/python3.8/unittest/mock.py:966: AssertionError

During handling of the above exception, another exception occurred:

mocker = <pytest_mock.plugin.MockerFixture object at 0x7f296cc5b490>

    def test_shutdown(mocker):
        """Verify Manager shuts down.
    
        Note: 'mocker' is a fixture function provided the the pytest-mock package.
        This fixture lets us override a library function with a temporary fake
        function that returns a hardcoded value while testing.
    
        See https://github.com/pytest-dev/pytest-mock/ for more info.
        """
        # Mock the socket library socket class
        mock_socket = mocker.patch("socket.socket")
    
        # accept() returns a mock client socket
        mock_clientsocket = mocker.MagicMock()
        mock_accept = mock_socket.return_value.__enter__.return_value.accept
        mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))
    
        # TCP recv() returns a sequence of hardcoded values
        mock_recv = mock_clientsocket.recv
        mock_recv.side_effect = [
            json.dumps({"message_type": "shutdown"}).encode("utf-8"),
            None
        ]
    
        # UDP recv() returns heartbeat messages
        mock_udp_recv = mock_socket.return_value.__enter__.return_value.recv
        mock_udp_recv.side_effect = utils.worker_heartbeat_generator(3001)
    
        # Run student Manager code.  When student Manager calls recv(), it will
        # receive the faked responses configured above.  When the student code
        # calls sys.exit(0), it triggers a SystemExit exception, which we'll catch.
        # The Manager may have several threads, so we make sure that they have all
        # been stopped.
        try:
            mapreduce.manager.Manager("localhost", 6000)
            assert threading.active_count() == 1, "Failed to shutdown threads"
        except SystemExit as error:
            assert error.code == 0
    
        # Verify that the student code called the correct socket functions with
        # the correct arguments.
        #
        # NOTE: to see a list of all calls
        # >>> print(mock_socket.mock_calls)
>       mock_socket.assert_has_calls([
            # TCP socket server configuration.  This is the socket the Manager uses
            # to receive JSON formatted commands from mapreduce-submit.
            mocker.call(socket.AF_INET, socket.SOCK_STREAM),
            mocker.call().__enter__().setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1,
            ),
            mocker.call().__enter__().bind(("localhost", 6000)),
            mocker.call().__enter__().listen(),
        ], any_order=True)
E       AssertionError: 'socket' does not contain all of (call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()) in its call list, found [call().setsockopt(1, 2, 1), call().__enter__(), call().bind(('localhost', 6000)), call().listen(), call().settimeout(1), call().accept(), call().accept().__iter__(), call().__exit__(<class 'ValueError'>, ValueError('not enough values to unpack (expected 2, got 0)'), <traceback object at 0x7f296c3c7400>), call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>), call().setsockopt(1, 2, 1)] instead
E       
E       pytest introspection follows:
E       
E       Args:
E       assert (<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_DGRAM: 2>) == ([call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>), call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()],)
E         At index 0 diff: <AddressFamily.AF_INET: 2> != [call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>), call().__enter__().setsockopt(1, 2, 1), call().__enter__().bind(('localhost', 6000)), call().__enter__().listen()]
E         Left contains one more item: <SocketKind.SOCK_DGRAM: 2>
E         Full diff:
E           (
E         +  <AddressFamily.AF_INET: 2>,
E         +  <SocketKind.SOCK_DGRAM: 2>,
E         -  [call(<AddressFamily.AF_INET: 2>, <SocketKind.SOCK_STREAM: 1>),
E         -   call().__enter__().setsockopt(1, 2, 1),
E         -   call().__enter__().bind(('localhost', 6000)),
E         -   call().__enter__().listen()],,
E           )
E       Kwargs:
E       assert {} == {'any_order': True}
E         Right contains 1 more item:
E         {'any_order': True}
E         Full diff:
E         - {'any_order': True}
E         + {}

tests/test_manager_00.py:54: AssertionError
------------------------------ Captured log call -------------------------------
INFO     mapreduce.manager.__main__:__main__.py:25 Starting manager host=localhost port=6000 pwd=/home/ziheng/p4
=============================== warnings summary ===============================
tests/test_manager_00.py::test_shutdown
  /home/ziheng/p4/env/lib/python3.8/site-packages/_pytest/threadexception.py:73: PytestUnhandledThreadExceptionWarning: Exception in thread Thread-1
  
  Traceback (most recent call last):
    File "/usr/lib/python3.8/threading.py", line 932, in _bootstrap_inner
      self.run()
    File "/usr/lib/python3.8/threading.py", line 870, in run
      self._target(*self._args, **self._kwargs)
    File "/home/ziheng/p4/mapreduce/manager/__main__.py", line 83, in tcp_server
      conn, addr = self.tcp_socket.accept()
  ValueError: not enough values to unpack (expected 2, got 0)
  
    warnings.warn(pytest.PytestUnhandledThreadExceptionWarning(msg))

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_manager_00.py::test_shutdown - AssertionError: 'socket' doe...
!!!!!!!!!!!!!!!!!!!!!!!!!! stopping after 1 failures !!!!!!!!!!!!!!!!!!!!!!!!!!!
========================= 1 failed, 1 warning in 0.07s =========================
