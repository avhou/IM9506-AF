import whisper
import ffmpeg
import os
import sys

def extract_audio(input_file, output_file="temp_audio.wav"):
    """Extracts audio from a video file using ffmpeg."""
    try:
        # Correct way to call ffmpeg using ffmpeg-python
        ffmpeg.input(input_file).output(output_file, format="wav", acodec="pcm_s16le", ac=1, ar="16k").run(overwrite_output=True, quiet=True)
        return output_file
    except ffmpeg.Error as e:
        print(f"Error extracting audio: {e}")
        return None

def transcribe_audio(file_path, model_size="base"):
    """Transcribes audio using OpenAI Whisper."""
    print(f"Loading Whisper model ({model_size})...")
    model = whisper.load_model(model_size)  # Options: tiny, base, small, medium, large
    print("Transcribing...")
    result = model.transcribe(file_path)
    return result["text"]

def main():
    if len(sys.argv) < 2:
        print("Usage: python transcribe.py <audio_or_video_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    # Check if it's a video file and extract audio
    if input_file.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.flv', '.webm')):
        print("Extracting audio from video...")
        input_file = extract_audio(input_file)
        if not input_file:
            sys.exit(1)

    # Transcribe the audio
    transcription = transcribe_audio(input_file, "large")

    # Output transcription
    print("\n=== Transcription ===\n")
    print(transcription)

    # Save to text file
    with open("transcription.txt", "w", encoding="utf-8") as f:
        f.write(transcription)
    print("\nTranscription saved to 'transcription.txt'.")

if __name__ == "__main__":
    main()
