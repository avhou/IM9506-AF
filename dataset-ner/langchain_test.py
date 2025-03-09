import torch
from transformers import BertTokenizer, BertForTokenClassification
from langchain_core.documents import Document

# Load the model and tokenizer locally
tokenizer = BertTokenizer.from_pretrained("dbmdz/bert-large-cased-finetuned-conll03-english")
model = BertForTokenClassification.from_pretrained("dbmdz/bert-large-cased-finetuned-conll03-english")

# Define a function to perform Named Entity Recognition (NER) with the model
def run_ner(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        outputs = model(**inputs)

    # Extract the labels
    predictions = torch.argmax(outputs.logits, dim=-1)
    labels = [model.config.id2label[prediction.item()] for prediction in predictions[0]]
    tokens = tokenizer.convert_ids_to_tokens(inputs['input_ids'][0])

    # Reconstruct the words by merging subword tokens (e.g., '##e' -> 'e')
    words = []
    current_word = ""
    current_label = None
    for token, label in zip(tokens, labels):
        if token.startswith('##'):
            # Continue the current word
            current_word += token[2:]
        else:
            # Finish the current word and start a new one
            if current_word:
                words.append((current_word, current_label))
            current_word = token
            current_label = label

    # Append the last word
    if current_word:
        words.append((current_word, current_label))

    return words

# Use the run_ner function for NER on input text
text = """
Marie Curie, born in 1867, was a Polish and naturalised-French physicist and chemist who conducted pioneering research on radioactivity.
She was the first woman to win a Nobel Prize, the first person to win a Nobel Prize twice, and the only person to win a Nobel Prize in two scientific fields.
Her husband, Pierre Curie, was a co-winner of her first Nobel Prize, making them the first-ever married couple to win the Nobel Prize and launching the Curie family legacy of five Nobel Prizes.
She was, in 1906, the first woman to become a professor at the University of Paris.
"""

# Running NER
ner_results = run_ner(text)

# Print NER results
for word, label in ner_results:
    print(f"{word}: {label}")

# Now let's manually create a simple knowledge graph from the NER results
# We will extract "Person" and "Organization" entities and create a graph-like structure

nodes = []
relationships = []

# Combine consecutive 'Person' and 'Organization' entities into one
merged_entities = []
current_entity = None
current_entity_type = None

for word, label in ner_results:
    if label.startswith('B-ORG') or label.startswith('I-ORG'):
        # If it's an organization, combine
        if current_entity_type == 'Organization':
            current_entity += " " + word
        else:
            if current_entity:
                merged_entities.append((current_entity, current_entity_type))
            current_entity = word
            current_entity_type = 'Organization'
    elif label.startswith('B-PER') or label.startswith('I-PER'):
        # If it's a person, combine
        if current_entity_type == 'Person':
            current_entity += " " + word
        else:
            if current_entity:
                merged_entities.append((current_entity, current_entity_type))
            current_entity = word
            current_entity_type = 'Person'
    else:
        if current_entity:
            merged_entities.append((current_entity, current_entity_type))
        current_entity = None
        current_entity_type = None

# Append the last entity if needed
if current_entity:
    merged_entities.append((current_entity, current_entity_type))

# Print merged entities
print(f"Entities: {merged_entities}")

# Now we will define nodes and relationships from the merged entities
for entity, entity_type in merged_entities:
    nodes.append({"type": entity_type, "value": entity})

# Example of creating relationships (this part is simplistic, you can define relationships based on context)
for i in range(1, len(nodes)):
    relationships.append({"from": nodes[i-1], "to": nodes[i], "type": "related_to"})

# Example output for nodes and relationships
print(f"Nodes: {nodes}")
print(f"Relationships: {relationships}")
