import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaRecorder

# Keep track of peer connections globally.
pcs: set[RTCPeerConnection] = set()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # --- Startup code can go here if needed ---
    yield
    # --- Shutdown code: close all peer connections ---
    coros = [pc.close() for pc in pcs]
    await asyncio.gather(*coros)
    pcs.clear()

# Pass the lifespan handler to FastAPI.
app = FastAPI(lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    # A simple HTML page with inline JavaScript to capture audio.
    return """
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Audio Streaming Demo</title>
      </head>
      <body>
        <h1>Send Audio to Server</h1>
        <button onclick="start()">Start</button>
        <button onclick="stop()">Stop</button>
        <p id="status"></p>
        <script>
          let pc = null;
          let localStream = null;
          const statusElem = document.getElementById('status');

          async function start() {
            statusElem.textContent = "Starting…";
            // Create a new RTCPeerConnection.
            pc = new RTCPeerConnection();
            
            // Get the audio stream from the microphone.
            try {
              localStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            } catch (err) {
              alert("Could not get audio stream: " + err);
              return;
            }
            // Add all audio tracks to the peer connection.
            localStream.getTracks().forEach(track => pc.addTrack(track, localStream));

            // When ICE candidates are gathered, send the offer to the server.
            pc.onicecandidate = event => {
              if (event.candidate === null) {
                // ICE gathering complete: send localDescription to the server.
                fetch('/offer', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    sdp: pc.localDescription.sdp,
                    type: pc.localDescription.type
                  })
                })
                .then(response => response.json())
                .then(async answer => {
                  await pc.setRemoteDescription(answer);
                  statusElem.textContent = "Connected!";
                })
                .catch(err => {
                  alert("Error during offer/answer exchange: " + err);
                });
              }
            };

            // Create and set local offer.
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
          }

          function stop() {
            statusElem.textContent = "Stopping…";
            // Close the peer connection (which will stop the recording on the server)
            if (pc) {
              pc.close();
              pc = null;
            }
            // Stop all local media tracks.
            if (localStream) {
              localStream.getTracks().forEach(track => track.stop());
              localStream = null;
            }
            statusElem.textContent = "Stopped.";
          }
        </script>
      </body>
    </html>
    """

@app.post("/offer")
async def offer(request: Request) -> JSONResponse:
    """
    The /offer route expects a JSON object with an SDP offer from the browser.
    It creates a new RTCPeerConnection, attaches a MediaRecorder that saves audio
    to a uniquely named .ogg file, and returns an SDP answer.
    """
    params: Dict[str, Any] = await request.json()
    offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

    # Create a new PeerConnection and add it to the set.
    pc = RTCPeerConnection()
    pcs.add(pc)
    pc_id = f"PC({uuid.uuid4()})"
    print(pc_id, "Created for new offer")

    # Create a MediaRecorder that writes to a uniquely named OGG file.
    recorder = MediaRecorder(f"recording-{uuid.uuid4()}.ogg", format="ogg")

    @pc.on("track")
    def on_track(track: Any) -> None:
        print(pc_id, "Track received:", track.kind)
        if track.kind == "audio":
            # Add the audio track to the recorder.
            recorder.addTrack(track)
        # Optional: log when a track ends.
        @track.on("ended")
        async def on_ended() -> None:
            print(pc_id, "Audio track ended.")

    @pc.on("connectionstatechange")
    async def on_connectionstatechange() -> None:
        print(pc_id, "Connection state is", pc.connectionState)
        # When the connection is closed or fails, stop the recorder.
        if pc.connectionState in ["failed", "closed"]:
            await recorder.stop()
            pcs.discard(pc)

    # Set the remote description from the offer.
    await pc.setRemoteDescription(offer)
    # Start the recorder (it will record as soon as tracks are received).
    await recorder.start()

    # Create an answer, set it as the local description, and return it.
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)
    return JSONResponse({
        "sdp": pc.localDescription.sdp,
        "type": pc.localDescription.type
    })

if __name__ == "__main__":
    import uvicorn
    # Run the app on 0.0.0.0:8000 with auto-reload enabled.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
