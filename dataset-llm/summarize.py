from transformers import MT5Tokenizer, MT5ForConditionalGeneration
import torch

print("loading model")

# Load the tokenizer and model
model_name = "google/mt5-xl"
tokenizer = MT5Tokenizer.from_pretrained(model_name)
# must use safetensor + torch.float16 for mac m1
model = MT5ForConditionalGeneration.from_pretrained(model_name, use_safetensors=True, torch_dtype=torch.float16)
print("model was loaded")

print("move model to MPS")
model.to("mps")


# Example long text
text = """
Premier De Wever wil stappen zetten naar "Europese defensiemacht"
"Soms gebeurt er op één week een decennium aan geschiedenis. Misschien zitten we wel op zo'n punt in de geschiedenis dat er veel mogelijk wordt", zegt premier De Wever in een opmerkelijk interview met Villa politica. Hij houdt daarbij een pleidooi om "stappen te zetten naar een Europese defensiemacht".
De Wever benadrukt dat die stappen ook gewoon een noodzaak zijn. "We moeten ons voorbereiden op een dreiging van een tiran in het oosten die niet onmiddellijk zal verdwijnen. De militaire capaciteit in Rusland wordt opgebouwd. Ze produceren op twee maanden meer dan wij op één jaar. Tirannieke leiders met grote legers, de geschiedenis leest zich als een handboek, wat dat betreft. Die gaan dat vroeg of laat gebruiken en ons testen."  
De Wever benadrukt nog dat Europa de verdediging niet moet overlaten aan de grenslanden van Rusland. "We doen in Europa al behoorlijk veel samen voor defensie, alleen gebeurt dat altijd in gespreide slagorde. Misschien is het nu tijd voor een stevige Europese pilaar in het Navo-verhaal, met minder aparte wapensystemen en een doordacht aankoopbeleid. Dan zouden we met dezelfde budgetten al ongelooflijk veel meer kunnen doen."
"""

# Preprocess input text for summarization
input_text = "Summarize the following Dutch text, using a maximum of 5 words.  This is the text to summarize : " + text
inputs = tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True)

print(f"move tokens to MPS")
inputs = {k: v.to("mps") for k, v in inputs.items()}

# Generate summary, do not use inputs.input_ids
summary_ids = model.generate(inputs["input_ids"], max_length=50, num_beams=5, length_penalty=2.0, early_stopping=True)
summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True, clean_up_tokenization_spaces=True)

print("Summary:", summary)
