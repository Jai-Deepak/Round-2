from elasticsearch import Elasticsearch, helpers
import pandas as pd

hosts = ["http://localhost:9200"]

es = Elasticsearch(hosts=hosts)

def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Index '{p_collection_name}' created.")
    else:
        print(f"Index '{p_collection_name}' already exists.")

def indexData(p_collection_name, p_exclude_column):
    df = pd.read_csv('/Users/jaideepak/Desktop/employee_data.csv', encoding='iso-8859-1')
    print("Columns in the DataFrame:", df.columns.tolist())

    if p_exclude_column in df.columns:
        df = df.drop(columns=[p_exclude_column])
    else:
        print(f"Column '{p_exclude_column}' not found in DataFrame.")

    df = df.where(pd.notnull(df), None)

    df['Age'] = df['Age'].fillna(-1)

    actions = [
        {
            "_index": p_collection_name,
            "_source": record
        }
        for record in df.to_dict(orient="records")
    ]

    try:
        helpers.bulk(es, actions)
        print("Data indexed successfully.")
    except helpers.BulkIndexError as e:
        print(f"Failed to index {len(e.errors)} documents.")
        for error in e.errors:
            print(f"Error indexing document: {error}")

def getEmpCount(p_collection_name):
    return es.count(index=p_collection_name)['count']

def delEmpById(p_collection_name, p_employee_id):
    if es.exists(index=p_collection_name, id=p_employee_id):
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Document {p_employee_id} deleted from index {p_collection_name}.")
    else:
        print(f"Document {p_employee_id} not found in index {p_collection_name}.")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    results = [hit['_source'] for hit in response['hits']['hits']]
    print(f"Search results for {p_column_name} = {p_column_value}:")
    for result in results:
        print(result)

def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword",
                    "size": 10
                }
            }
        }
    }
    response = es.search(index=p_collection_name, body=query)
    for bucket in response['aggregations']['departments']['buckets']:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")

if __name__ == "__main__":
    v_nameCollection = "hash_jaideepak"
    v_phoneCollection = "hash_8771"

    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)

    emp_count = getEmpCount(v_nameCollection)
    print(f"Employee count in {v_nameCollection}: {emp_count}")

    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')

    delEmpById(v_nameCollection, 'E02003')

    emp_count_after_deletion = getEmpCount(v_nameCollection)
    print(f"Employee count in {v_nameCollection} after deletion: {emp_count_after_deletion}")

    searchByColumn(v_nameCollection, 'Department', 'IT')
    searchByColumn(v_nameCollection, 'Gender', 'Male')
    searchByColumn(v_phoneCollection, 'Department', 'IT')
    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)
