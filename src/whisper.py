from .module_classes import ExecutionModule, ModuleOptions
import whisper
import tempfile
import io
from pydub import AudioSegment
import torch

class WhisperModule(ExecutionModule):
    def __init__(self, model_size='base', language=None, task='transcribe', temperature=0.0, vad=False, beam_size=5, best_of=5, fp16=True, condition_on_previous_text=True, ram_disk_path='/tmp'):
        super().__init__(ModuleOptions())
        self.model_size = model_size
        self.language = language
        self.task = task
        self.temperature = temperature
        self.vad = vad
        self.beam_size = beam_size
        self.best_of = best_of
        self.fp16 = fp16
        self.condition_on_previous_text = condition_on_previous_text
        self.ram_disk_path = ram_disk_path
        self.model = whisper.load_model(self.model_size)

    def execute(self, data):
        # Create temporary file for audio data which will be deleted after use.
        with tempfile.NamedTemporaryFile(prefix='tmp_audio_', suffix='.wav', dir=self.ram_disk_path, delete=True) as temp_file:
            # Convert opus to wav
            opus_data = io.BytesIO(data)
            opus_audio = AudioSegment.from_file(opus_data, format="ogg", frame_rate=48000, channels=2, sample_width=2)
            opus_audio.export(temp_file.name, format="wav")
            
            # Transcribe audio data
            result = self.model.transcribe(temp_file.name, 
                                           fp16=torch.cuda.is_available() and self.fp16, 
                                           task=self.task,
                                           language=self.language,
                                           temperature=self.temperature,
                                           beam_size=self.beam_size,
                                           best_of=self.best_of,
                                           condition_on_previous_text=self.condition_on_previous_text)
            text = result['text'].strip()

        return True, "Text", text

# Example usage:
# module = WhisperModule(model_size='large', language='en', task='transcribe', temperature=0.5, ram_disk_path='/path/to/ramdisk')
# success, label, transcription = module.execute(audio_data)
