import json

from faiss_utils import models_and_params, get_index_and_metadata, BASE_EMBEDDING_MODEL, MULTILINGUAL_EMBEDDING_MODEL
from outlet_faiss_querying import *


KOLOM_NAMEN = ["translated_text", "content"]

def query_index(faiss_folder: str, db_name: str, questions_file: str, nr_of_hits: int = 5):
    queries = []
    with open(questions_file, "r") as f:
        for line in f:
            query = line.strip()
            if query:
                queries.append(query)
    print(f"will execute {len(queries)} queries")
    responses = {}
    for query in queries:
        responses[query] = {}

    for (model_name, dimension, kwargs) in models_and_params():
        model = load_model(model_name, **kwargs)

        total_unique_retrieved_doc_ids = set()
        for column_name in KOLOM_NAMEN:
            (index_path, metadata_path) = get_index_and_metadata(faiss_folder, model_name, column_name, db_name)
            index, chunk_metadata = read_index_and_metadata(index_path, metadata_path)

            for query in queries:
                print(f"querying faiss index {index_path} with query {query}")
                unique_retrieved_doc_ids = query_index_with_model(model, index, chunk_metadata, query, nr_of_hits)
                responses[query].update({f'retrieved_doc_ids_{model_name}_{column_name}': unique_retrieved_doc_ids})
                total_unique_retrieved_doc_ids.update(unique_retrieved_doc_ids)
            # responses.append({'query': 'total', 'retrieved_doc_ids': list(total_unique_retrieved_doc_ids)})

    for query in queries:
        stats = responses[query]
        # berekenen hoeveel intersectie er is tussen de verschillende modellen, op dezelfde kolommen
        for column_name in KOLOM_NAMEN:
            base_stats = stats[f"retrieved_doc_ids_{BASE_EMBEDDING_MODEL}_{column_name}"]
            multilingual_stats = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{column_name}"]
            common_for_column_name = list(set(base_stats).intersection(set(multilingual_stats)))
            stats[f'intersection_{column_name}'] = common_for_column_name
            stats[f'intersection_{column_name}_percentage'] = f"{(float(len(common_for_column_name)) / float(len(base_stats)) if len(base_stats) > 0 else 0) * 100:.2f}"
        # berekenen hoeveel intersectie er is tussen de verschillende kolommen, op het nomic model
        nomic_stats_first = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{KOLOM_NAMEN[0]}"]
        nomic_stats_second = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{KOLOM_NAMEN[1]}"]
        common_for_model = list(set(nomic_stats_first).intersection(set(nomic_stats_second)))
        stats[f'intersection_cross_column_multilingual'] = common_for_model
        stats[f'intersection_cross_column_multilingual_percentage'] = f"{(float(len(common_for_model)) / float(len(nomic_stats_first)) if len(base_stats) > 0 else 0) * 100:.2f}"
    with open(f"{db_name[:-len('.sqlite')]}-{questions_file[:-len('.txt')]}.json", "w") as f:
        json.dump(responses, f)


if __name__ == "__main__":
    if len(sys.argv) <= 3:
        raise RuntimeError("usage : batch_outlet_faiss_querying.py <faiss_folder> <db_name.sqlite> <questions_file> [nr_of_hits]")
    query_index(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else 5)