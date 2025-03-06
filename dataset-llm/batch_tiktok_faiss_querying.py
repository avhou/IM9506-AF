import json
import sys

from faiss_utils import *
from typing import List
import sqlite3


KOLOM_NAMEN = ["translated_text", "transcription"]

def query_folder(faiss_folder: str, db_name: str, nr_of_hits: int = 5):
    with sqlite3.connect(db_name) as conn:
        for file, column_name, value in zip(
                ["queries_disinformation.txt", "queries_irrelevant.txt", "queries_relevant.txt"],
                ["disinformation", "relevant", "relevant"],
                ["y?", "n?", "y?"],
        ):
            print(f"processing query file {file}, marking column {column_name} with value {value}")
            documents_to_mark = query_index(faiss_folder, db_name, file, nr_of_hits)
            print(f"{len(documents_to_mark)} documents could be marked as {value} for column {column_name} if already marked documents were not taken into account")
            number_corresponding_value = conn.execute(f"select count(*) from video_hits where number in ({','.join(map(str, documents_to_mark))}) and {column_name} = '{value[:-1]}';").fetchone()[0]
            number_not_corresponding_value = conn.execute(f"select count(*) from video_hits where number in ({','.join(map(str, documents_to_mark))}) and {column_name} != '{value[:-1]}';").fetchone()[0]
            print(f"update video_hits set {column_name} = coalesce({column_name}, '') || '{value}' where number in ({','.join(map(str, documents_to_mark))}) and (relevant not in ('y', 'n') or relevant is null) and ({column_name} not in ('y', 'n') or {column_name} is null);")
            rows_updated = conn.execute(f"update video_hits set {column_name} = coalesce({column_name}, '') || '{value}' where number in ({','.join(map(str, documents_to_mark))}) and (relevant not in ('y', 'n') or relevant is null) and ({column_name} not in ('y', 'n') or {column_name} is null);").rowcount
            print(f"{rows_updated} documents actually marked as {value} for column {column_name}, {number_corresponding_value} were already marked as {value[:-1]}, {number_not_corresponding_value} were marked as the opposite value")



def query_index(faiss_folder: str, db_name: str, questions_file: str, nr_of_hits: int = 5) -> List[int]:
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

        for column_name in KOLOM_NAMEN:
            (index_path, metadata_path) = get_index_and_metadata(faiss_folder, model_name, column_name, db_name)
            index, chunk_metadata = read_index_and_metadata(index_path, metadata_path)

            for query in queries:
                print(f"querying faiss index {index_path} with query {query}")
                unique_retrieved_doc_ids = query_index_with_model(model, index, chunk_metadata, query, nr_of_hits)
                responses[query].update({f'retrieved_doc_ids_{model_name}_{column_name}': unique_retrieved_doc_ids})
            # responses.append({'query': 'total', 'retrieved_doc_ids': list(total_unique_retrieved_doc_ids)})

    total_unique_retrieved_doc_ids = set()
    for query in queries:
        stats = responses[query]

        # berekenen hoeveel intersectie er is tussen de verschillende kolommen, op het nomic model
        nomic_stats_first = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{KOLOM_NAMEN[0]}"]
        nomic_stats_second = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{KOLOM_NAMEN[1]}"]
        common_for_model = list(set(nomic_stats_first).intersection(set(nomic_stats_second)))
        stats[f'intersection_cross_column_multilingual'] = common_for_model
        stats[f'intersection_cross_column_multilingual_percentage'] = f"{(float(len(common_for_model)) / float(len(nomic_stats_first)) if len(nomic_stats_first) > 0 else 0) * 100:.2f}"

        # berekenen hoeveel intersectie er is tussen de verschillende modellen, op dezelfde kolommen
        for column_name in KOLOM_NAMEN:
            base_stats = stats[f"retrieved_doc_ids_{BASE_EMBEDDING_MODEL}_{column_name}"]
            multilingual_stats = stats[f"retrieved_doc_ids_{MULTILINGUAL_EMBEDDING_MODEL}_{column_name}"]
            common_for_column_name = list(set(base_stats).intersection(set(multilingual_stats)))
            stats[f'intersection_{column_name}'] = common_for_column_name
            stats[f'intersection_{column_name}_percentage'] = f"{(float(len(common_for_column_name)) / float(len(base_stats)) if len(base_stats) > 0 else 0) * 100:.2f}"

        # we willen de intersectie tussen de verschillende modellen op column translated_text overhouden
        total_unique_retrieved_doc_ids.update(stats[f'intersection_translated_text'])


    responses.update({'total': {'retrieved_doc_ids': list(total_unique_retrieved_doc_ids)}})
    with open(f"{db_name[:-len('.sqlite')]}-{questions_file[:-len('.txt')]}.json", "w") as f:
        json.dump(responses, f)

    return list(total_unique_retrieved_doc_ids)


if __name__ == "__main__":
    if len(sys.argv) <= 2:
        raise RuntimeError("usage : batch_tiktok_faiss_querying.py <faiss_folder> <db_name.sqlite> [nr_of_hits]")
    query_folder(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 5)