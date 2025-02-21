import torch
from transformers import MarianTokenizer, MarianMTModel
import re

# Detect and use MPS if available
device = "mps" if torch.backends.mps.is_available() else "cpu"
device = "cpu"
print(f"Using device: {device}")

# Choose the appropriate model based on input language
def get_translation_model(source_lang):
    if source_lang == "fr":
        model_name = "Helsinki-NLP/opus-mt-fr-en"
    elif source_lang == "nl":
        model_name = "Helsinki-NLP/opus-mt-nl-en"
    else:
        raise ValueError("Unsupported language. Use 'fr' for French or 'nl' for Dutch.")

    tokenizer = MarianTokenizer.from_pretrained(model_name)
    model = MarianMTModel.from_pretrained(model_name).to(device)
    return tokenizer, model

# Chunking function to avoid truncation
def chunk_text(text, max_length=250):
    sentences = re.split(r'\.|\?|!', text)
    chunks = []
    current_chunk = ""

    for sentence in sentences:
        sentence = re.sub(r'\s+', ' ', sentence.strip()).strip()

        # If sentence is too long, split it by words
        if len(sentence) > max_length:
            words = sentence.split()
            for i in range(0, len(words), max_length):
                split_sentence = " ".join(words[i:i + max_length]) + "."
                chunks.append(split_sentence.strip())
        else:
            # Normal chunking logic
            if len(current_chunk) + len(sentence) < max_length:
                current_chunk += sentence + " "
            else:
                chunks.append(current_chunk.strip())
                current_chunk = sentence + " "

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


# Token-based chunking function with MPS support
def chunk_text_by_tokens(text, tokenizer, max_tokens=512):
    sentences = re.split(r'\.|\?|!', text)
    chunks = []
    current_chunk = []
    current_length = 0

    for sentence in sentences:
        cleaned_sentence = re.sub(r'\s+', ' ', sentence.strip())
        print(f"found sentence '{cleaned_sentence.strip()}'")
        tokenized_sentence = tokenizer(sentence, return_tensors="pt", add_special_tokens=False).input_ids.to(device)  # Move to MPS
        sentence_length = tokenized_sentence.shape[1]  # Get number of tokens

        if current_length + sentence_length > max_tokens:
            chunks.append(" ".join(current_chunk))  # Store as text, not tensors
            current_chunk = [sentence]
            current_length = sentence_length
        else:
            current_chunk.append(sentence)
            current_length += sentence_length

    if current_chunk:
        chunks.append(" ".join(current_chunk))  # Store remaining sentences

    return chunks

# Translate text chunk by chunk
def translate_text(text, source_lang):
    tokenizer, model = get_translation_model(source_lang)
    text_chunks = chunk_text(text)
    # text_chunks = chunk_text_by_tokens(text, tokenizer)

    translated_chunks = []
    for chunk in text_chunks:
        # print(f"translating chunk: {chunk}")
        inputs = tokenizer(chunk, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
        with torch.no_grad():
            translated_ids = model.generate(**inputs)
        translated_text = tokenizer.batch_decode(translated_ids, skip_special_tokens=True)[0]
        # print(f"chunk was translated to : {translated_text}")
        translated_chunks.append(translated_text)

    return " ".join(translated_chunks)

with open("input_text.txt", "r") as f:
    input_text = f.read()

translated_text = translate_text(input_text, source_lang="nl")
print("Translated Text:", translated_text)
with open("translated_text.txt", "w") as f:
    f.write(translated_text)
