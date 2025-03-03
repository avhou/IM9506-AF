# read the content of "relevant-content.json" as an array of dictionaries
# read the content of "irrelevant-content.json" as an array of dictionaries
# take the element that has the value 'total' for key 'query' from both arrays
# take the property of field 'retrieved_doc_ids' from both elements
# calculate the intersection of the two arrays, and express as a percentage of the total number of elements in the first array
import json

def calculate_intersection_percentage(relevant_file: str, irrelevant_file: str) -> float:
    # Read the content of "relevant-content.json" as an array of dictionaries
    with open(relevant_file, 'r') as file:
        relevant_content = json.load(file)

    # Read the content of "irrelevant-content.json" as an array of dictionaries
    with open(irrelevant_file, 'r') as file:
        irrelevant_content = json.load(file)

    # Take the element that has the value 'total' for key 'query' from both arrays
    relevant_total = next(item for item in relevant_content if item['query'] == 'total')
    irrelevant_total = next(item for item in irrelevant_content if item['query'] == 'total')

    # Take the property of field 'retrieved_doc_ids' from both elements
    relevant_doc_ids = set(relevant_total['retrieved_doc_ids'])
    irrelevant_doc_ids = set(irrelevant_total['retrieved_doc_ids'])

    # Calculate the intersection of the two arrays
    intersection = relevant_doc_ids.intersection(irrelevant_doc_ids)

    # Express as a percentage of the total number of elements in the first array
    percentage = (len(intersection) / len(relevant_doc_ids)) * 100

    return percentage

# Example usage
relevant_file = 'relevant-content.json'
irrelevant_file = 'irrelevant-content.json'
percentage = calculate_intersection_percentage(relevant_file, irrelevant_file)
print(f"Intersection percentage: {percentage:.2f}%")