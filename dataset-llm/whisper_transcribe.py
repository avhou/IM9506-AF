import whisper
from whisper import Whisper
import ffmpeg
import os
import sys
import sqlite3
from itertools import islice


def extract_audio(input_file, output_file="temp_audio.wav"):
    try:
        # ffmpeg.input(input_file).output(output_file, format="wav", acodec="pcm_s16le", ac=1, ar="16k").run(overwrite_output=True, quiet=True)
        ffmpeg.input(input_file).output(output_file, format="wav", acodec="pcm_s24le", ac=2, ar="48k").run(overwrite_output=True, quiet=True)
        return output_file
    except ffmpeg.Error as e:
        print(f"Error extracting audio: {e}")
        return None


def transcribe_audio(file_path, model: Whisper):
    print("Transcribing...")
    result = model.transcribe(file_path)
    return result["text"]


def load_model(model_size="base") -> Whisper:
    print(f"Loading Whisper model ({model_size})...")
    model = whisper.load_model(model_size)  # Options: tiny, base, small, medium, large
    return model

def transcribe_videos(folder: str, target_db: str):
    model = load_model("large")

    table_name = os.path.basename(os.path.normpath(folder))
    print(f"using table_name {table_name}")

    with sqlite3.connect(target_db) as conn:
        for row in islice(conn.execute(f"""select id from {table_name} where transcription is null;"""), 10):
            video_id = row[0]
            if os.path.exists(os.path.join(folder, f"{video_id}.mp4")):
                print(f"Extracting audio from video {video_id}.mp4 to temp_audio_{video_id}.wav...")
                input_file = extract_audio(os.path.join(folder, f"{video_id}.mp4"), f"temp_audio_{video_id}.wav")
                if not input_file:
                    print(f"Could not extract audio from {video_id}.mp4.")
                    continue
                transcription = transcribe_audio(input_file, model)
                print(f"video {video_id} was transcribed to {transcription}")
                os.remove(input_file)
                conn.execute(f"update {table_name} set transcription = ? where id = ?", (transcription, video_id))



if __name__ == "__main__":
    if len(sys.argv) <= 2:
        raise RuntimeError("usage : whisper_transcribe.py <folder> <tiktok-db.sqlite>")
    transcribe_videos(sys.argv[1], sys.argv[2])
