import asyncio
import socket
import logging


_logger = logging.getLogger()


class AsyncTcpClient(object):
    """ A Simple Asynchronous TCP Socket Client """

    def __init__(self, host='localhost', port=9527):
        self.host = host
        self.port = port
        self.running = False
        self.target_socket = self._create_target_socket()
        self.loop = asyncio.get_event_loop()
        self.out_queue = asyncio.Queue()

    def _create_target_socket(self):
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        new_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        return new_socket

    def start(self):
        _logger.debug("Preparing AsyncTcpClient...")
        self.running = True

        self.target_socket.connect((self.host, self.port))
        self.target_socket.setblocking(False)

        self.loop.create_task(self.receive_messages())
        self.loop.create_task(self.send_messages())
        _logger.debug("Running AsyncTcpClient...")
        self.loop.run_forever() # Blocks here
        _logger.debug("Closing AsyncTcpClient...")
        self.loop.close()

    async def receive_messages(self):
        """ Receives messages and closes connection when closing """
        while self.running:
            message = await self.loop.sock_recv(self.target_socket, 1024)
            if not message:
                # Receiving null data means remote connection closing
                break

        self.target_socket.close()

    async def send_messages(self):
        """ Sends messages to server """
        while self.running:
            message = await self.out_queue.get()
            if message:
                await self.loop.sock_sendall(self.target_socket, message)


if __name__ == '__main__':
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(logging.StreamHandler())
    client = AsyncTcpClient()
    client.out_queue.put_nowait(b"Test Message!")
    client.start()
