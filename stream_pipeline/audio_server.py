import asyncio
import io
import threading
import uuid
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import numpy as np
from pyogg import OpusEncoder, OggOpusWriter

from ogg import OggS_Page

from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
import pyogg

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger("webrtc_app")


class WebRTCConnection:
    """
    Encapsulates a single WebRTC connection.
    """
    def __init__(self, stop_connection: Callable[[], None]) -> None:
        self.id: str = f"Conn({uuid.uuid4()})"
        self.pc: RTCPeerConnection = RTCPeerConnection()
        self._stop_connection: Callable[[], None] = stop_connection

        self.on_webrtc_stream_callback: Optional[Callable[[bytes], None]] = None
        self.running: bool = False

        logger.info(f"{self.id} Created WebRTCConnection.")

        @self.pc.on("track")
        def on_track(track: MediaStreamTrack) -> None:
            logger.info(f"{self.id} Track received: {track.kind}")
            # For audio tracks we want to both record and invoke the callback.
            if track.kind == "audio":
                if self.on_webrtc_stream_callback:
                    # spawn a task to process frames from track
                    asyncio.create_task(self._consume_audio(track))
                else:
                    # If no callback was registered, use the original behavior.
                    self.recorder.addTrack(track)

                @track.on("ended")
                async def on_ended() -> None:
                    logger.info(f"{self.id} Audio track ended.")
            else:
                # (For video or other track types you can add your own handling here.)
                pass


            @track.on("ended")
            async def on_ended() -> None:
                logger.info(f"{self.id} Audio track ended.")

        @self.pc.on("connectionstatechange")
        def on_connectionstatechange() -> None:
            logger.info(f"{self.id} Connection state changed to {self.pc.connectionState}.")
            if self.pc.connectionState in ["failed", "closed"]:
                logger.warning(f"{self.id} Connection state is {self.pc.connectionState}. Initiating stop().")
                asyncio.create_task(self.stop())

    def set_on_webrtc_stream(self, callback: Callable[[bytes], None]) -> None:
        """
        Set a callback to be called each time audio data (as bytes) is received.
        (For example, the callback might write the bytes to an OGG opus file.)
        """
        self.on_webrtc_stream_callback = callback
        logger.info(f"{self.id} on_webrtc_stream callback set.")

    async def _consume_audio(self, track):
        """Continuously receives audio from the WebRTC track, encodes it to Ogg Opus, and sends it to the callback."""
        
        # Initialize Opus Encoder
        encoder = pyogg.OpusBufferedEncoder()
        encoder.set_application("audio")
        encoder.set_sampling_frequency(48000)
        encoder.set_channels(2)
        encoder.set_frame_size(20)  # 20ms frames

        # Create one buffer and one OggOpusWriter for the entire connection
        buffer = io.BytesIO()
        writer = pyogg.OggOpusWriter(buffer, encoder)

        self.running = True
        while self.running:
            try:
                frame = await track.recv()  # Receive an AudioFrame
            except Exception as e:
                continue

            # Convert the frame to PCM and write it to the writer
            pcm_array = frame.to_ndarray().astype(np.int16)  # Ensure int16 format
            pcm_bytes = pcm_array.tobytes()
            writer.write(memoryview(bytearray(pcm_bytes)))  # Write PCM data to encoder

            # Instead of closing the writer every time (which resets the state),
            # we simply check if the buffer has data ready to be sent.
            # If your writer accumulates pages, you can send the buffer's current value.
            latest_pages = buffer.getvalue()
            if latest_pages:
                self.on_webrtc_stream_callback(latest_pages)  # Send latest pages to callback

                # Reset the buffer for new data
                buffer.seek(0)
                buffer.truncate()
        
        # When finished streaming, close the writer
        writer.close()



    def get_status(self) -> str:
        state: str = self.pc.connectionState
        logger.debug(f"{self.id} get_status() called, returning '{state}'.")
        if state == "new":
            return "new"
        elif state == "connecting":
            return "connecting"
        elif state == "connected":
            return "connected"
        elif state == "disconnected":
            return "disconnected"
        elif state == "failed":
            return "failed"
        elif state == "closed":
            return "closed"
        else:
            return "unknown"

    async def set_remote_description(self, offer: RTCSessionDescription) -> None:
        logger.info(f"{self.id} Setting remote description.")
        await self.pc.setRemoteDescription(offer)

    async def create_and_set_answer(self) -> RTCSessionDescription:
        logger.info(f"{self.id} Creating answer.")
        answer: RTCSessionDescription = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)
        logger.info(f"{self.id} Local description set.")
        return self.pc.localDescription

    async def stop(self) -> None:
        """Clean up the connection by stopping the recorder and closing the RTCPeerConnection."""
        self.running = False
        try:
            logger.info(f"{self.id} Closing peer connection.")
            await self.pc.close()
        except Exception as e:
            logger.error(f"{self.id} Error closing peer connection: {e}")
        logger.info(f"{self.id} Cleaning up connection.")

        if self.pc.connectionState == "closed":
            return
        self._stop_connection()


class WSConnection:
    """
    Encapsulates a single WebSocket connection for signalling.
    """
    def __init__(self, webrtc_connection: WebRTCConnection, stop_connection: Callable[[], None]) -> None:
        self.websocket: Optional[WebSocket] = None
        self.webrtc_connection: WebRTCConnection = webrtc_connection
        self.running: bool = False
        self._stop_connection: Callable[[], None] = stop_connection
        self._lock: asyncio.Lock = asyncio.Lock()
        logger.info(f"{self.webrtc_connection.id} WSConnection created.")

    async def set_ws(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.websocket = websocket
            logger.info(f"{self.webrtc_connection.id} WebSocket set.")

    def get_status(self) -> str:
        if not self.websocket:
            return "disconnected"
        # The client_state values correspond to:
        # 0: CONNECTING, 1: OPEN, 2: CLOSING, 3: CLOSED
        if self.websocket.client_state == 0:
            return "connecting"
        elif self.websocket.client_state == 1:
            return "connected"
        elif self.websocket.client_state == 2:
            return "disconnecting"
        elif self.websocket.client_state == 3:
            return "disconnected"
        else:
            return "unknown"

    async def run(self) -> None:
        if not self.websocket:
            raise Exception("No websocket: You first need to set the websocket connection with set_ws(WebSocket)")
        
        logger.info(f"{self.webrtc_connection.id} WSConnection run loop started.")
        try:
            self.running = True
            # Handle further signalling messages.
            while self.running:
                message: Dict[str, Any] = await self.websocket.receive_json()
                logger.debug(f"{self.webrtc_connection.id} Received WS message: {message}")
                action: Optional[str] = message.get("action")
                if action == "offer":
                    sdp: Optional[str] = message.get("sdp")
                    type_: Optional[str] = message.get("type")
                    if not sdp or not type_:
                        error_msg = "Missing SDP parameters in offer"
                        logger.error(f"{self.webrtc_connection.id} {error_msg}")
                        await self.websocket.send_json({"error": error_msg})
                        continue
                    offer_desc = RTCSessionDescription(sdp=sdp, type=type_)
                    await self.webrtc_connection.set_remote_description(offer_desc)
                    answer_desc = await self.webrtc_connection.create_and_set_answer()
                    logger.info(f"{self.webrtc_connection.id} Sending answer over WebSocket.")
                    await self.websocket.send_json({
                        "action": "answer",
                        "sdp": answer_desc.sdp,
                        "type": answer_desc.type
                    })
                elif action == "ping":
                    logger.debug(f"{self.webrtc_connection.id} Received ping, sending pong.")
                    await self.websocket.send_json({"action": "pong"})
                else:
                    error_msg = "Unknown action"
                    logger.error(f"{self.webrtc_connection.id} {error_msg}: {action}")
                    await self.websocket.send_json({"error": error_msg})
        except WebSocketDisconnect:
            logger.info(f"{self.webrtc_connection.id} WebSocket disconnected.")
        except Exception as e:
            logger.error(f"{self.webrtc_connection.id} WebSocket error: {e}")

    async def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        logger.info(f"{self.webrtc_connection.id} Stopping WSConnection.")
        if self.websocket:
            await self.websocket.close()
        self._stop_connection()


class Connection:
    def __init__(self, remove_connection: Callable[[str], None]) -> None:
        self.id: str = f"Conn({uuid.uuid4()})"
        self.tocken: str = str(uuid.uuid4())
        self.webrtc: WebRTCConnection = WebRTCConnection(self.stop)
        self.ws: WSConnection = WSConnection(self.webrtc, self.stop)
        self._remove_connection: Callable[[str], None] = remove_connection
        self._lock: threading.Lock = threading.Lock()
        logger.info(f"{self.id} Connection instance created with token {self.tocken}.")

    def get_id(self) -> str:
        return self.id
    
    def get_tocken(self) -> str:
        return self.tocken
    
    def validate_tocken(self, tocken: str) -> bool:
        valid: bool = tocken == self.tocken
        logger.debug(f"{self.id} Token validation: {valid}.")
        return valid

    def get_status(self) -> str:
        # Delegate status check to the WebSocket component.
        status: str = self.ws.get_status()
        logger.debug(f"{self.id} get_status() called, returning '{status}'.")
        return status

    def on_webrtc_stream(self, callback: Callable[[bytes], None]) -> None:
        """
        Register a callback that will be called every time audio data (as bytes)
        is received from the WebRTC connection. For example, you might use the callback
        to store the bytes in an OGG opus file.
        """
        self.webrtc.set_on_webrtc_stream(callback)
        logger.info(f"{self.id} on_webrtc_stream callback registered.")

    async def run(self, tocken: str, ws: WebSocket) -> None:
        if not self.validate_tocken(tocken):
            error_msg = "Invalid tocken"
            logger.error(f"{self.id} {error_msg}.")
            raise Exception(error_msg)
        await self.ws.set_ws(ws)
        logger.info(f"{self.id} Starting WSConnection run loop.")
        await self.ws.run()

    def stop(self) -> None:
        logger.info(f"{self.id} Stopping Connection.")
        self._remove_connection(self.id)
        if self.ws.get_status() == "connected":
            asyncio.create_task(self.ws.stop())
        if self.webrtc.get_status() == "connected":
            asyncio.create_task(self.webrtc.stop())


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: List[Connection] = []
        self.timeout_time: int = 10
        self._lock: threading.Lock = threading.Lock()
        logger.info("ConnectionManager initialized.")

    def create_connection(self) -> Tuple[str, str]:
        conn: Connection = Connection(self.remove_connection)
        conn_id: str = conn.get_id()
        conn_tocken: str = conn.get_tocken()
        with self._lock:
            self.connections.append(conn)
            logger.info(f"Connection {conn_id} created and added to manager.")
        self._timeout_checker(conn_id)
        return conn_id, conn_tocken
    
    def _timeout_checker(self, conn_id: str) -> None:
        async def _check() -> None:
            await asyncio.sleep(self.timeout_time)
            con: Optional[Connection] = self.get_connection(conn_id)
            if con is None:
                logger.debug(f"Timeout checker: Connection {conn_id} already removed.")
                return
            if con.get_status() == "connected":
                logger.debug(f"Timeout checker: Connection {conn_id} is connected; no action taken.")
                return
            logger.info(f"Timeout checker: Removing inactive connection {conn_id}.")
            self.remove_connection(conn_id)

        asyncio.create_task(_check())

    def remove_connection(self, conn_id: str) -> None:
        with self._lock:
            self.connections = [conn for conn in self.connections if conn.get_id() != conn_id]
            logger.info(f"Connection {conn_id} removed from manager.")

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        with self._lock:
            for conn in self.connections:
                if conn.get_id() == conn_id:
                    return conn
        logger.debug(f"Connection {conn_id} not found in manager.")
        return None
    
    def stop(self) -> None:
        logger.info("Stopping all connections in ConnectionManager.")
        for conn in self.connections:
            conn.stop()





















con_manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        con_manager.stop()

app = FastAPI(lifespan=lifespan)

def recv_data(data: bytes) -> None:
    page = OggS_Page(data)
    logger.info(f"Received data: {page}")

    # write to file
    with open("out.ogg", "ab") as f:
        f.write(data)


@app.get("/register")
async def register() -> JSONResponse:
    """
    Creates a new WebRTC connection and returns its connection ID and a token.
    The token is valid for 10 seconds; if the client doesn't use it to authenticate
    on the WebSocket within that time, it gets invalidated.
    """
    con_id, tocken = con_manager.create_connection()

    con = con_manager.get_connection(con_id)
    con.on_webrtc_stream(recv_data)

    logger.info(f"Registered new connection: {con_id} with token: {tocken}")
    return JSONResponse({"connection_id": con_id, "token": tocken})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("WebSocket connection accepted.")
    try:
        # Expect an authentication message with the token.
        auth_msg: Dict[str, Any] = await websocket.receive_json()
        logger.debug(f"Authentication message received: {auth_msg}")
        if auth_msg.get("action") != "authenticate" or "connection_id" not in auth_msg or "token" not in auth_msg:
            error_msg = "Invalid authentication message. Need connection_id and token"
            logger.error(error_msg)
            await websocket.send_json({"error": error_msg})
            return
        connection_id: str = auth_msg["connection_id"]
        token: str = auth_msg["token"]

        connection: Optional[Connection] = con_manager.get_connection(connection_id)
        if not connection:
            error_msg = "Invalid connection_id, or connection expired"
            logger.error(error_msg)
            await websocket.send_json({"error": error_msg})
            return
        
        if not connection.validate_tocken(token):
            error_msg = "Invalid token"
            logger.error(f"{connection_id}: {error_msg}")
            await websocket.send_json({"error": error_msg})
            return

        await websocket.send_json({"action": "authenticated", "message": "Token accepted"})
        logger.info(f"{connection_id} authenticated successfully.")
        await connection.run(token, websocket)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected during authentication or run loop.")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    logger.info("Index page requested.")
    # HTML page with both WebRTC and WebSocket (signalling) demo.
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>WebRTC with WS Signalling Demo</title>
    </head>
    <body>
        <h1>WebRTC with WS Signalling</h1>
        <button onclick="start()">Start Audio</button>
        <button onclick="stop()">Stop Audio</button>
        <p id="status"></p>
        <div id="wsLog" style="background: #f0f0f0; padding: 10px; max-height: 150px; overflow-y: auto;"></div>
        <script>
        let pc = null;
        let localStream = null;
        let ws = null;
        let token = null;
        let connectionId = null;
        const statusElem = document.getElementById('status');
        const wsLog = document.getElementById('wsLog');

        async function start() {
            statusElem.textContent = "Registering connection...";
            // First, register and get a token.
            try {
            const regResponse = await fetch('/register');
            const regData = await regResponse.json();
            token = regData.token;
            connectionId = regData.connection_id;
            statusElem.textContent = "Registered. Connection ID: " + connectionId;
            wsLog.innerHTML += "<p>Registered with token: <strong>" + token + "</strong></p>";
            } catch (err) {
            statusElem.textContent = "Registration failed.";
            console.error(err);
            return;
            }

            // Open WebSocket connection for signalling.
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            ws.onopen = function() {
            wsLog.innerHTML += "<p><em>WebSocket connection opened.</em></p>";
            // Send an authentication message with the token.
            ws.send(JSON.stringify({"action": "authenticate", "connection_id": connectionId, "token": token}));
            };

            ws.onmessage = async function(event) {
            console.log("WS message received:", event.data);
            let msg;
            try {
                msg = JSON.parse(event.data);
            } catch (err) {
                console.error("Error parsing WS message", err);
                return;
            }
            if (msg.action === "authenticated") {
                wsLog.innerHTML += "<p>Authenticated on WS.</p>";
                // Once authenticated, start WebRTC.
                await startWebRTC();
            } else if (msg.action === "answer") {
                wsLog.innerHTML += "<p>Received answer.</p>";
                const answer = new RTCSessionDescription({sdp: msg.sdp, type: msg.type});
                await pc.setRemoteDescription(answer);
                statusElem.textContent = "WebRTC connected!";
            } else if (msg.action === "pong") {
                wsLog.innerHTML += "<p>Received pong.</p>";
            } else if (msg.error) {
                wsLog.innerHTML += "<p style='color: red;'>Error: " + msg.error + "</p>";
            }
            };

            ws.onclose = function() {
            wsLog.innerHTML += "<p><em>WebSocket connection closed.</em></p>";
            };
        }

        async function startWebRTC() {
            statusElem.textContent = "Starting WebRTC...";
            pc = new RTCPeerConnection();

            try {
            localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            } catch (err) {
            alert("Could not get audio stream: " + err);
            return;
            }
            localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

            // When ICE gathering is complete, send the offer over the WS connection.
            pc.onicecandidate = async function(event) {
            if (event.candidate === null) {
                wsLog.innerHTML += "<p>Sending offer via WebSocket.</p>";
                ws.send(JSON.stringify({
                "action": "offer",
                "sdp": pc.localDescription.sdp,
                "type": pc.localDescription.type
                }));
            }
            };

            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
        }

        function stop() {
            statusElem.textContent = "Stopping...";
            if (pc) {
            pc.close();
            pc = null;
            }
            if (localStream) {
            localStream.getTracks().forEach(track => track.stop());
            localStream = null;
            }
            if (ws) {
            ws.close();
            ws = null;
            }
            statusElem.textContent = "Stopped.";
        }
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting uvicorn server on 0.0.0.0:8000")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
