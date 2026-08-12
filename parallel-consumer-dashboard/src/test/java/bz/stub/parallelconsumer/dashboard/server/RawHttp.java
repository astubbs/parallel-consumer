package bz.stub.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A deliberately dumb HTTP/1.1 client built on a raw socket.
 * <p>
 * Every well-behaved HTTP client in the JDK and in Vert.x <em>protects</em> you from sending the requests these tests
 * have to send. {@code HttpURLConnection} silently refuses to set {@code Host} unless a system property is set.
 * Vert.x's client derives {@code Host} from the address it connected to. Both are correct behaviour and both make it
 * impossible to test a {@code Host} allowlist, because the interesting request is precisely the one where the
 * {@code Host} header disagrees with the address dialled - which is exactly what a DNS rebinding attack produces.
 * <p>
 * So: sockets and string formatting. It also makes the server-sent-events assertions straightforward, since reading
 * events incrementally off a chunked stream is easier here than through a client that wants to buffer a response.
 */
final class RawHttp {

    private static final int DEFAULT_TIMEOUT_MILLIS = 10_000;

    /**
     * The address dialled. The dashboard binds loopback in these tests; the {@code Host} header is what varies, and
     * varying it independently of the address is the whole point of this class.
     */
    private static final String CONNECT_HOST = "127.0.0.1";

    private RawHttp() {
    }

    static Response get(int port, String path) throws IOException {
        return request("GET", port, path, new LinkedHashMap<>());
    }

    /**
     * One request, one connection, closed afterwards - so the response body always ends at EOF and there is no
     * connection reuse to reason about.
     */
    static Response request(String method, int port, String path, Map<String, String> headers) throws IOException {
        try (Socket socket = connect(port)) {
            Map<String, String> effective = withDefaults(headers, CONNECT_HOST, port);
            effective.put("Connection", "close");
            write(socket.getOutputStream(), method, path, effective);
            return readResponse(socket.getInputStream());
        }
    }

    /**
     * One request whose header lines are written <em>exactly</em> as given - no defaults added, not even {@code Host}.
     * <p>
     * The {@link Map}-based form structurally cannot send a header twice, which is precisely the request RFC 7230
     * s5.4 is about and precisely the request an ambiguous-authority check has to be tested against.
     */
    static Response requestWithRawHeaders(String method, int port, String path, List<String> headerLines)
            throws IOException {
        try (Socket socket = connect(port)) {
            StringBuilder request = new StringBuilder();
            request.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");
            for (String line : headerLines) {
                request.append(line).append("\r\n");
            }
            request.append("Connection: close\r\n\r\n");
            OutputStream out = socket.getOutputStream();
            out.write(request.toString().getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
            return readResponse(socket.getInputStream());
        }
    }

    /**
     * Opens a server-sent-events stream and leaves it open for the caller to read from.
     */
    static SseStream openSse(int port, String path) throws IOException {
        Socket socket = connect(port);
        Map<String, String> headers = withDefaults(new LinkedHashMap<>(), CONNECT_HOST, port);
        headers.put("Accept", "text/event-stream");
        write(socket.getOutputStream(), "GET", path, headers);
        return new SseStream(socket);
    }

    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(CONNECT_HOST, port), DEFAULT_TIMEOUT_MILLIS);
        socket.setSoTimeout(DEFAULT_TIMEOUT_MILLIS);
        return socket;
    }

    private static Map<String, String> withDefaults(Map<String, String> headers, String connectHost, int port) {
        Map<String, String> effective = new LinkedHashMap<>();
        effective.put("Host", connectHost + ":" + port);
        // no Accept-Encoding, so the server never compresses and the body is readable as-is
        effective.putAll(headers);
        return effective;
    }

    private static void write(OutputStream out, String method, String path, Map<String, String> headers)
            throws IOException {
        StringBuilder request = new StringBuilder();
        request.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");
        for (Map.Entry<String, String> header : headers.entrySet()) {
            if (header.getValue() == null) {
                continue;
            }
            request.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
        }
        request.append("\r\n");
        out.write(request.toString().getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    private static Response readResponse(InputStream in) throws IOException {
        String statusLine = readLine(in);
        if (statusLine == null) {
            throw new IOException("The server closed the connection without sending a status line");
        }
        String[] parts = statusLine.split(" ", 3);
        int statusCode = Integer.parseInt(parts[1]);

        Map<String, List<String>> headers = new LinkedHashMap<>();
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon < 0) {
                continue;
            }
            String name = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            String value = line.substring(colon + 1).trim();
            headers.computeIfAbsent(name, key -> new ArrayList<>()).add(value);
        }

        InputStream body = isChunked(headers) ? new ChunkedInputStream(in) : in;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = body.read(chunk)) >= 0) {
            buffer.write(chunk, 0, read);
        }
        return new Response(statusCode, headers, new String(buffer.toByteArray(), StandardCharsets.UTF_8));
    }

    private static boolean isChunked(Map<String, List<String>> headers) {
        List<String> encoding = headers.get("transfer-encoding");
        return encoding != null && encoding.toString().toLowerCase(Locale.ROOT).contains("chunked");
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) >= 0) {
            if (b == '\n') {
                byte[] bytes = line.toByteArray();
                int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r' ? bytes.length - 1 : bytes.length;
                return new String(bytes, 0, length, StandardCharsets.ISO_8859_1);
            }
            line.write(b);
        }
        return line.size() == 0 ? null : line.toString("ISO-8859-1");
    }

    /**
     * One HTTP response, with headers lower-cased so a test never has to guess at capitalisation.
     */
    static final class Response {

        final int statusCode;

        final Map<String, List<String>> headers;

        final String body;

        Response(int statusCode, Map<String, List<String>> headers, String body) {
            this.statusCode = statusCode;
            this.headers = headers;
            this.body = body;
        }

        String header(String name) {
            List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
            return values == null || values.isEmpty() ? null : values.get(0);
        }

        boolean hasHeader(String name) {
            return headers.containsKey(name.toLowerCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return statusCode + " " + headers + " " + body;
        }
    }

    /**
     * An open server-sent-events connection. Reads one event at a time, blocking up to the socket timeout.
     */
    static final class SseStream implements AutoCloseable {

        private final Socket socket;

        private final InputStream body;

        final Response head;

        SseStream(Socket socket) throws IOException {
            this.socket = socket;
            InputStream in = socket.getInputStream();
            // read only the status line and headers here; the body is the stream itself and must not be drained
            String statusLine = readLine(in);
            int statusCode = Integer.parseInt(statusLine.split(" ", 3)[1]);
            Map<String, List<String>> headers = new LinkedHashMap<>();
            String line;
            while ((line = readLine(in)) != null && !line.isEmpty()) {
                int colon = line.indexOf(':');
                if (colon < 0) {
                    continue;
                }
                headers.computeIfAbsent(line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
                        key -> new ArrayList<>()).add(line.substring(colon + 1).trim());
            }
            this.head = new Response(statusCode, headers, "");
            this.body = isChunked(headers) ? new ChunkedInputStream(in) : in;
        }

        /**
         * The next event, or null at end of stream. Comment lines (SSE keep-alives) are skipped.
         */
        SseEvent readEvent() throws IOException {
            String id = null;
            String event = null;
            StringBuilder data = new StringBuilder();
            boolean sawField = false;
            String line;
            while ((line = readLine(body)) != null) {
                if (line.isEmpty()) {
                    if (sawField) {
                        return new SseEvent(id, event, data.toString());
                    }
                    continue;
                }
                if (line.startsWith(":")) {
                    // a comment - the keep-alive - carries no event
                    continue;
                }
                int colon = line.indexOf(':');
                String field = colon < 0 ? line : line.substring(0, colon);
                String value = colon < 0 ? "" : line.substring(colon + 1).trim();
                sawField = true;
                if ("id".equals(field)) {
                    id = value;
                } else if ("event".equals(field)) {
                    event = value;
                } else if ("data".equals(field)) {
                    data.append(value);
                }
                // "retry" is a client directive, not part of an event
                if ("retry".equals(field)) {
                    sawField = false;
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /**
     * One server-sent event.
     */
    static final class SseEvent {

        final String id;

        final String event;

        final String data;

        SseEvent(String id, String event, String data) {
            this.id = id;
            this.event = event;
            this.data = data;
        }

        @Override
        public String toString() {
            return "id=" + id + " event=" + event + " data=" + data;
        }
    }

    /**
     * Decodes HTTP chunked transfer encoding. Needed because the event stream and any handler using a chunked
     * response are framed, and a test reading raw bytes would otherwise see the chunk sizes as content.
     */
    private static final class ChunkedInputStream extends InputStream {

        private final InputStream in;

        private int remainingInChunk;

        private boolean finished;

        private ChunkedInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            if (finished) {
                return -1;
            }
            if (remainingInChunk == 0) {
                String sizeLine = readLine(in);
                // the CRLF that terminates the previous chunk shows up as an empty line
                while (sizeLine != null && sizeLine.isEmpty()) {
                    sizeLine = readLine(in);
                }
                if (sizeLine == null) {
                    finished = true;
                    return -1;
                }
                int semicolon = sizeLine.indexOf(';');
                String size = (semicolon < 0 ? sizeLine : sizeLine.substring(0, semicolon)).trim();
                remainingInChunk = Integer.parseInt(size, 16);
                if (remainingInChunk == 0) {
                    finished = true;
                    return -1;
                }
            }
            int b = in.read();
            if (b < 0) {
                finished = true;
                return -1;
            }
            remainingInChunk--;
            return b;
        }
    }
}
