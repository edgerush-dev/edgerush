"""An idle connection's answer is read whole, however it arrives, so the next one starts clean."""

import socket
import threading
import unittest

from idle import SWITCHED, answer, h2_answer, h2_frame, h2_request, opened


def served(*pieces):
    """The client's end of a connection whose other end writes `pieces` and closes."""
    client, server = socket.socketpair()

    def write():
        for piece in pieces:
            server.sendall(piece)
        server.close()

    threading.Thread(target=write).start()
    return client


class Answer(unittest.TestCase):
    def test_a_head_and_its_body_are_read_whole(self):
        connection = served(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhel", b"lo", b"HTTP/1.1 204 ")
        self.assertEqual(answer(connection), b"HTTP/1.1 200 OK\r\ncontent-length: 5")
        connection.close()

    def test_a_head_in_pieces_is_read_whole(self):
        connection = served(b"HTTP/1.1 2", b"00 OK\r\nConten", b"t-Length: 0\r\n\r", b"\n")
        self.assertTrue(answer(connection).startswith(b"HTTP/1.1 200 OK"))
        connection.close()

    def test_an_answer_cut_short_is_an_error(self):
        for pieces in [(b"HTTP/1.1 200 OK\r\n",), (b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\n\r\nabc",)]:
            connection = served(*pieces)
            with self.assertRaises(ConnectionError):
                answer(connection)
            connection.close()


def served_h2(*pieces):
    """The same for HTTP/2: the other end writes `pieces`, then reads what the client sends
    until the client closes. Returns the client's end and a function that, once the client
    has closed, waits for the other end to finish and returns what the client sent."""
    client, server = socket.socketpair()
    sent = []

    def write():
        for piece in pieces:
            server.sendall(piece)
        while chunk := server.recv(4096):
            sent.append(chunk)
        server.close()

    thread = threading.Thread(target=write)
    thread.start()

    def received():
        thread.join(timeout=10)
        return b"".join(sent)

    return client, received


SETTINGS = h2_frame(0x4, 0, 0, b"")
HEAD = h2_frame(0x1, 0x4, 1, b"\x88")


class H2Answer(unittest.TestCase):
    def test_the_request_is_a_preface_settings_and_one_ended_stream(self):
        request = h2_request("bench.example.com")
        self.assertTrue(request.startswith(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
        settings, headers = request[24:33], request[33:]
        self.assertEqual(settings, SETTINGS)
        self.assertEqual((headers[3], headers[4], headers[5:9]), (0x1, 0x5, b"\x00\x00\x00\x01"))
        self.assertIn(b"\x00\x0a:authority\x11bench.example.com", headers)

    def test_stream_one_is_read_to_its_end_in_pieces_and_settings_are_acknowledged(self):
        data = h2_frame(0x0, 0x1, 1, b"ok")
        wire = SETTINGS + HEAD + data
        connection, received = served_h2(wire[:5], wire[5:14], wire[14:])
        self.assertEqual(h2_answer(connection), b"\x88")
        connection.close()
        self.assertEqual(received(), h2_frame(0x4, 0x1, 0, b""))

    def test_a_reset_or_a_close_before_the_end_is_an_error(self):
        reset, _ = served_h2(h2_frame(0x3, 0, 1, b"\x00\x00\x00\x08"))
        # This one's other end closes once it has written.
        closed = served(HEAD)
        for connection in [reset, closed]:
            with self.assertRaises(ConnectionError):
                h2_answer(connection)
            connection.close()


class WebSocket(unittest.TestCase):
    def test_a_websocket_is_held_once_switched_and_anything_else_is_an_error(self):
        listener = socket.create_server(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        answers = [
            b"HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\r\n",
            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n",
        ]

        def serve():
            for answered in answers:
                connection, _ = listener.accept()
                connection.recv(4096)
                connection.sendall(answered)
                connection.close()

        threading.Thread(target=serve).start()
        switched = opened("127.0.0.1", port, b"GET /ws HTTP/1.1\r\n\r\n", expect=SWITCHED)
        switched.close()
        with self.assertRaises(ConnectionError):
            opened("127.0.0.1", port, b"GET /ws HTTP/1.1\r\n\r\n", expect=SWITCHED)
        listener.close()


if __name__ == "__main__":
    unittest.main()
