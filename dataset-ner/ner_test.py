import spacy
from spacy.cli import download


# Function to extract meaningful triples using dependency parsing
def extract_triples(doc):
    triples = []

    for token in doc:
        # Check if token represents a meaningful relation
        if token.dep_ in ("ROOT", "acl", "xcomp", "ccomp"):
            subject = None
            obj = None

            # Find subject (nsubj) and object (dobj, prep, attr)
            for child in token.children:
                if child.dep_ in ("nsubj", "nsubjpass"):  # Subject
                    subject = child.text
                if child.dep_ in ("dobj", "attr", "prep", "pobj"):  # Object
                    obj = child.text

            if subject and obj:
                triples.append((subject, token.lemma_, obj))  # Use lemma_ for predicate normalization

    return triples

def extract_triples2(doc):
    triples = []

    for token in doc:
        if token.dep_ in ("ROOT", "acl", "xcomp", "ccomp"):
            subject = None
            obj = None
            predicate = token.text  # Use original text for predicates

            aux_verbs = []  # Store auxiliary verbs (was, has been, etc.)
            for child in token.children:
                if child.dep_ in ("nsubj", "nsubjpass"):  # Subject
                    subject = child.text
                if child.dep_ in ("dobj", "attr", "prep", "pobj", "agent"):  # Object or object of prep
                    if child.dep_ == "prep":  # Handle prepositional phrases
                        # Extract the object of the preposition (e.g., "Hawaii")
                        for grandchild in child.children:
                            if grandchild.dep_ == "pobj":  # Object of prep
                                obj = grandchild.text
                    else:
                        obj = child.text
                if child.dep_ in ("aux", "auxpass"):  # Auxiliary verbs
                    aux_verbs.append(child.text)

            # If passive voice detected, construct predicate correctly
            if aux_verbs:
                predicate = " ".join(aux_verbs) + " " + token.text  # Use token.text instead of lemma_

            print(f"aux_verbs: {aux_verbs}, predicate: {predicate}")

            # Only add valid triples
            if subject and obj:
                triples.append((subject, predicate, obj))

    return triples

def do_extract():

    model_name = "en_core_web_trf"
    try:
        nlp = spacy.load(model_name)
        print(f"Model '{model_name}' is already installed!")
    except OSError:
        print(f"Model '{model_name}' not found. Downloading now...")
        download(model_name)
        nlp = spacy.load(model_name)
        print("Model downloaded and loaded successfully!")
    text = "Barack Obama was born in Hawaii. He was elected President of the United States."
    doc = nlp(text)
    triples = extract_triples2(doc)
    print("Extracted Triples:", triples)

if __name__ == "__main__":
    do_extract()