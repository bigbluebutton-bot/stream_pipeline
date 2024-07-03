import time
from extract_ogg import split_ogg_data_into_frames, OggSFrame

def speech_to_text(data):
    # Placeholder for the actual speech-to-text function
    print(f"Processing {len(data)} bytes of audio data")

def calculate_frame_duration(current_granule_position, previous_granule_position, sample_rate=48000):
    if previous_granule_position is None:
        return 0.02  # Default value for the first frame
    samples = current_granule_position - previous_granule_position
    duration = samples / sample_rate
    return duration

def simulate_live_audio_stream(file_path, sample_rate=48000):
    with open(file_path, 'rb') as file:
        ogg_bytes = file.read()

    frames = split_ogg_data_into_frames(ogg_bytes)
    audio_data_buffer = []
    previous_granule_position = None
    start_time = time.time()

    for frame_index, frame in enumerate(frames):
        current_granule_position = frame.header['granule_position']
        frame_duration = calculate_frame_duration(current_granule_position, previous_granule_position, sample_rate)
        previous_granule_position = current_granule_position

        audio_data_buffer.append(frame.raw_data)

        # Sleep to simulate real-time audio playback
        time.sleep(frame_duration)

        # Every second, process the last 10 seconds of audio
        if frame_duration > 0 and (frame_index + 1) % int(1 / frame_duration) == 0:
            current_time = time.time()
            elapsed_time = current_time - start_time

            if elapsed_time > 10:
                start_time += 1  # Move the window forward by 1 second

            # Combine the last 10 seconds of audio data
            ten_seconds_of_audio = b''.join(audio_data_buffer[-int(10 / frame_duration):])
            speech_to_text(ten_seconds_of_audio)

if __name__ == "__main__":
    # Path to the Ogg file
    file_path = './audio/bbb.ogg'
    start_time = time.time()
    simulate_live_audio_stream(file_path)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")
