import asyncio
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Pipes raw binary data bidirectionally between client and target server."""
    try:
        while not reader.at_eof():
            data = await reader.read(8192)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()
        await writer.wait_closed()

async def handle_client(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
    try:
        request_line = await client_reader.readline()
        if not request_line:
            client_writer.close()
            return

        parts = request_line.decode('utf-8', errors='ignore').split()
        if len(parts) < 2:
            client_writer.close()
            return

        method, target = parts[0].upper(), parts[1]
        logging.info(f"{method} {target}")

        # 1. HTTPS Tunneling (CONNECT method)
        if method == 'CONNECT':
            host, port = target.split(':') if ':' in target else (target, 443)
            port = int(port)

            try:
                server_reader, server_writer = await asyncio.open_connection(host, port)
            except Exception as e:
                logging.error(f"Connection failed to {host}:{port} -> {e}")
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await client_writer.drain()
                client_writer.close()
                return

            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await client_writer.drain()

            await asyncio.gather(
                pipe(client_reader, server_writer),
                pipe(server_reader, client_writer),
                return_exceptions=True
            )

        # 2. Standard HTTP Forward Proxy
        else:
            headers = []
            host = None
            port = 80

            while True:
                line = await client_reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break
                headers.append(line)
                line_str = line.decode('utf-8', errors='ignore')
                if line_str.lower().startswith('host:'):
                    host_val = line_str.split(':', 1)[1].strip()
                    if ':' in host_val:
                        host, port_str = host_val.split(':')
                        port = int(port_str)
                    else:
                        host = host_val

            if not host:
                client_writer.close()
                return

            try:
                server_reader, server_writer = await asyncio.open_connection(host, port)
            except Exception as e:
                logging.error(f"Connection failed to {host}:{port} -> {e}")
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await client_writer.drain()
                client_writer.close()
                return

            server_writer.write(request_line)
            for h in headers:
                server_writer.write(h)
            server_writer.write(b"\r\n")
            await server_writer.drain()

            await asyncio.gather(
                pipe(client_reader, server_writer),
                pipe(server_reader, client_writer),
                return_exceptions=True
            )

    except Exception as e:
        logging.error(f"Handler error: {e}")
    finally:
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass

async def main():
    # Bind to 0.0.0.0 and use Render's dynamic PORT environment variable
    host = '0.0.0.0'
    port = int(os.environ.get('PORT', 8888))
    
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f"Proxy listening on {host}:{port}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProxy stopped.")
