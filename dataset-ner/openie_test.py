from transformers import pipeline

# Load OpenIE from Hugging Face
openie = pipeline("openie", model="allenai/openie5")

# Input text for extraction
text = "Barack Obama was born in Hawaii. He was elected President of the United States."

# Use OpenIE to extract triples
triples = openie(text)

# Display the extracted triples
for triple in triples:
    print(f"Subject: {triple['subject']}, Predicate: {triple['predicate']}, Object: {triple['object']}")
