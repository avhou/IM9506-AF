from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
import spacy

# Load the model and tokenizer for Mistral Instruct or Microsoft Phi4
# For the Mistral Instruct model:
tokenizer = AutoTokenizer.from_pretrained("mistralai/Mistral-7B-Instruct-v0.3")
model = AutoModelForTokenClassification.from_pretrained("mistralai/Mistral-7B-Instruct-v0.3")

# For the Microsoft Phi4 model (you can substitute this one if you prefer Phi4):
# tokenizer = AutoTokenizer.from_pretrained("microsoft/phi4")
# model = AutoModelForTokenClassification.from_pretrained("microsoft/phi4")

# Create the NER pipeline
ner_pipeline = pipeline("ner", model=model, tokenizer=tokenizer)

# Sample text
text = """
Albert Einstein was a theoretical physicist who developed the theory of relativity. He was born in the Kingdom of Württemberg in the German Empire on 14 March 1879.
"""

# Perform Named Entity Recognition (NER)
entities = ner_pipeline(text)

print("Named Entities Extracted:")
for entity in entities:
    # print(f"{entity['word']} - {entity['entity_group']}")
    print(f"{entity}")

# Now, for Knowledge Graph extraction, let's use Spacy for relationship extraction
# You can install Spacy and its English model using `pip install spacy` and `python -m spacy download en_core_web_trf`

nlp = spacy.load("en_core_web_trf")

# Process the text using Spacy
doc = nlp(text)

# Extract relationships and entities for building a simple knowledge graph
print("\nKnowledge Graph Extraction:")
for ent1 in doc.ents:
    for ent2 in doc.ents:
        if ent1 != ent2 and ent1.root.dep_ == 'nsubj' and ent2.root.dep_ == 'attr':
            print(f"Relation: {ent1.text} -> {ent2.text}")
