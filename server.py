import asyncio
import socket
import logging


_logger = logging.getLogger()


class AsyncTcpServer(object):
    """ A Simple Asynchronous TCP Socket Server """

    def __init__(self, host='localhost', port=9527):
        self.host = host
        self.port = port
        self.running = False
        self.listen_socket = self._create_listen_socket()
        self.loop = asyncio.get_event_loop()
        self._connected_sockets = []

    def _create_listen_socket(self):
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        new_socket.setblocking(False)

        return new_socket

    def start(self):
        _logger.debug("Preparing AsyncTcpServer...")
        self.running = True
        self.listen_socket.bind((self.host, self.port))
        self.listen_socket.listen()

        self.loop.create_task(self.receive_connection())
        self.loop.create_task(self.heartbeat())
        _logger.debug("Running AsyncTcpServer...")
        self.loop.run_forever() # Blocks here
        _logger.debug("Closing AsyncTcpServer...")
        self.loop.close()

    async def receive_message(self, client_socket):
        """ Receives messages and closes connection when closing """
        while self.running:
            message = await self.loop.sock_recv(client_socket, 1024)
            if not message:
                # Receiving null data means remote connection closing
                _logger.debug("Closing connection...")
                self._connected_sockets.remove(client_socket)
                break
            _logger.debug(f"Receiving new message {message}...")
        client_socket.close()

    async def receive_connection(self):
        """ Accepts new connections and starts listening and sending tasks """
        while self.running:
            client_socket, address = await self.loop.sock_accept(self.listen_socket)
            self._connected_sockets.append(client_socket)
            _logger.debug("Receiving new connection...")
            self.loop.create_task(self.receive_message(client_socket))

    async def heartbeat(self):
        while self.running:
            await asyncio.sleep(5)
            _logger.debug("Sending heartbeat ping")
            for connected_socket in self._connected_sockets:
                await self.loop.sock_sendall(connected_socket, b"PING")


if __name__ == '__main__':
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(logging.StreamHandler())
    server = AsyncTcpServer()
    server.start()