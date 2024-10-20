elasticsearch_script.py 

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


Function Execution Results

A. Create Collection (v_nameCollection)
•Input:
v_nameCollection = "hash_jaideepak"
createCollection(v_nameCollection)

- This function checks if an index named hash_jaideepak exists in Elasticsearch. If it does not exist, it creates the index.
•Output:
	If the index was created successfully:
	“Index 'hash_jaideepak' created.”

	If the index already exists:
	“Index 'hash_jaideepak' already exists.”


B. Create Collection (v_phoneCollection)

•Input:
	v_phoneCollection = "hash_8771"
createCollection(v_phoneCollection)

- This function checks if an index named hash_8771 exists in Elasticsearch. If it does not exist, it creates the index.

•Output:
If the index was created successfully:
	“ Index 'hash_8771' created. ”

	If the index already exists:
	“ Index 'hash_8771' already exists. ”




C. Get Employee Count (v_nameCollection)
•Input:
	emp_count = getEmpCount(v_nameCollection)
print(f"Employee count in {v_nameCollection}: {emp_count}")
	
- This function retrieves the total count of employees indexed in the hash_jaideepak collection.

•Output:
	Example output (assuming no employees have been indexed yet):
	“ Employee count in hash_jaideepak: 0 ”




D. Index Data (v_nameCollection)

•Input:
	“ indexData(v_nameCollection, 'Department') “
	- This function reads the employee data from the CSV file, excludes the ‘Department’ column, and indexes the remaining data into the hash_jaideepak collection.

•Output:
	“ Data indexed successfully. ”




E. Index Data (v_phoneCollection)

•Input:
“indexData(v_phoneCollection, 'Gender') “
	-This function reads the employee data from the CSV file, excludes the ‘Gender’ column, and indexes the remaining data into the hash_8771 collection.

•Output:
	“ Data indexed successfully. “



F. Delete Employee by ID

•Input:
	“ delEmpById(v_nameCollection, 'E02003') “
-	This function attempts to delete the employee document with the ID E02003 from the hash_jaideepak collection.
•Output:
	If the employee was found and deleted:
	“ Document E02003 deleted from index hash_jaideepak. ”
	
If the employee was not found:
“ Document E02003 not found in index hash_jaideepak. “



G. Get Employee Count After Deletion
•Input:
	emp_count_after_deletion = getEmpCount(v_nameCollection)
print(f"Employee count in {v_nameCollection} after deletion: {emp_count_after_deletion}")
	
-	This function retrieves the total count of employees indexed in the hash_jaideepak collection after the deletion.
•Output:
“ Employee count in hash_jaideepak after deletion: [updated_count] “


H. Search by Department (v_nameCollection)
•Input:
	“ searchByColumn(v_nameCollection, 'Department', 'IT') “
-	This function searches for employees in the hash_jaideepak collection whose department is ‘IT’.
•Output:
	Example output:
	“ Search results for Department = IT:
{'Name': 'John Doe', 'Department': 'IT', 'Age': 30, ...} “

I. Search by Gender (v_nameCollection)
•Input:
	searchByColumn(v_nameCollection, 'Gender', 'Male')
-	This function searches for employees in the hash_jaideepak collection whose gender is ‘Male’.
•Output:
Example output:
	“ Search results for Gender = Male:
{'Name': 'Jane Doe', 'Gender': 'Male', 'Department': 'Sales', ...} “



J. Search by Department (v_phoneCollection)
•Input:
	searchByColumn(v_phoneCollection, 'Department', 'IT')
-This function searches for employees in the hash_8771 collection whose department is ‘IT’.
•Output:
	Example output:
	“ Search results for Department = IT:
{'Name': 'Alice Smith', 'Department': 'IT', 'Gender': 'Female', ...} “



K. Get Department Facet (v_nameCollection)
•Input:
	getDepFacet(v_nameCollection)
-	This function retrieves the count of employees grouped by department from the hash_jaideepak collection.
•Output:
	Example output:
	“ Department: IT, Count: 5
Department: HR, Count: 3 “



L. Get Department Facet (v_phoneCollection)
•Input:
	getDepFacet(v_phoneCollection)
-	This function retrieves the count of employees grouped by department from the hash_8771 collection.
•Output:
	Example output:
	“ Department: Sales, Count: 2
Department: IT, Count: 4 “

Screenshort : ( Final Output )

 



Final Output : ( Since it’s not possible to capture the entire output in a single screenshot, the following text content provides a complete overview of the results from executing the functions )

jaideepak@Jais-MacBook-Air % python3 elasticsearch_script.py
/Users/jaideepak/Desktop/elasticsearch_script.py:11:
Index 'hash_jaideepak' already exists.
/Users/jaideepak/Desktop/elasticsearch_script.py:12:
es.indices.create(index=p_collection_name)
Index 'hash_8771' created.
/Users/jaideepak/Desktop/elasticsearch_script.py:50:
return es.count(index=p_collection_name)['count']
Employee count in hash_jaideepak: 11941
Columns in the DataFrame: ['Employee ID', 'Full Name', 'Job Title', 'Department', 'Business Unit', 'Gender', 'Ethnicity', 'Age', 'Hire Date', 'Annual Salary', 'Bonus %', 'Country', 'City', 'Exit Date']
/Users/jaideepak/Desktop/elasticsearch_script.py:42:
Data indexed successfully.
Columns in the DataFrame: ['Employee ID', 'Full Name', 'Job Title', 'Department', 'Business Unit', 'Gender', 'Ethnicity', 'Age', 'Hire Date', 'Annual Salary', 'Bonus %', 'Country', 'City', 'Exit Date']
/Users/jaideepak/Desktop/elasticsearch_script.py:53:
if es.exists(index=p_collection_name, id=p_employee_id):
Document E02003 not found in index hash_jaideepak.
return es.count(index=p_collection_name)['count']
Employee count in hash_jaideepak after deletion: 11941
/Users/jaideepak/Desktop/elasticsearch_script.py:67: response = es.search(index=p_collection_name, body=query)
Search results for Department = IT:
{'Employee ID': 'E02004', 'Full Name': 'Cameron Lo', 'Job Title': 'Network Administrator', 'Department': 'IT', 'Business Unit': 'Research & Development', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '34', 'Hire Date': '3/24/2019', 'Annual Salary': '$83,576 ', 'Bonus %': '0%', 'Country': 'China', 'City': 'Shanghai', 'Exit Date': ''}
{'Employee ID': 'E02005', 'Full Name': 'Harper Castillo', 'Job Title': 'IT Systems Architect', 'Department': 'IT', 'Business Unit': 'Corporate', 'Gender': 'Female', 'Ethnicity': 'Latino', 'Age': '39', 'Hire Date': '4/7/2018', 'Annual Salary': '$98,062 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Seattle', 'Exit Date': ''}
{'Employee ID': 'E02007', 'Full Name': 'Ezra Vu', 'Job Title': 'Network Administrator', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '62', 'Hire Date': '4/22/2004', 'Annual Salary': '$66,227 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Phoenix', 'Exit Date': '2/14/2014'}
{'Employee ID': 'E02010', 'Full Name': 'Gianna Holmes', 'Job Title': 'System Administrator\xa0', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Female', 'Ethnicity': 'Caucasian', 'Age': '38', 'Hire Date': '9/9/2011', 'Annual Salary': '$97,630 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Seattle', 'Exit Date': ''}
{'Employee ID': 'E02012', 'Full Name': 'Jameson Pena', 'Job Title': 'Systems Analyst', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Latino', 'Age': '49', 'Hire Date': '10/12/2003', 'Annual Salary': '$40,499 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Miami', 'Exit Date': ''}
{'Employee ID': 'E02014', 'Full Name': 'Jose Wong', 'Job Title': 'Director', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '45', 'Hire Date': '11/15/2017', 'Annual Salary': '$150,558 ', 'Bonus %': '23%', 'Country': 'China', 'City': 'Chongqing', 'Exit Date': ''}
{'Employee ID': 'E02017', 'Full Name': 'Luna Lu', 'Job Title': 'IT Systems Architect', 'Department': 'IT', 'Business Unit': 'Corporate', 'Gender': 'Female', 'Ethnicity': 'Asian', 'Age': '62', 'Hire Date': '7/26/1997', 'Annual Salary': '$64,208 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Miami', 'Exit Date': ''}
{'Employee ID': 'E02020', 'Full Name': 'Jordan Kumar', 'Job Title': 'Service Desk Analyst', 'Department': 'IT', 'Business Unit': 'Specialty Products', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '29', 'Hire Date': '11/11/2017', 'Annual Salary': '$95,729 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Seattle', 'Exit Date': ''}
{'Employee ID': 'E02023', 'Full Name': 'Lillian Lewis', 'Job Title': 'Technical Architect', 'Department': 'IT', 'Business Unit': 'Research & Development', 'Gender': 'Female', 'Ethnicity': 'Black', 'Age': '43', 'Hire Date': '8/14/2013', 'Annual Salary': '$83,323 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Phoenix', 'Exit Date': '3/31/2019'}
{'Employee ID': 'E02031', 'Full Name': 'Wyatt Dinh', 'Job Title': 'System Administrator\xa0', 'Department': 'IT', 'Business Unit': 'Specialty Products', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '50', 'Hire Date': '3/15/2002', 'Annual Salary': '$72,860 ', 'Bonus %': '0%', 'Country': 'China', 'City': 'Shanghai', 'Exit Date': ''}
Search results for Gender = Male:
{'Employee ID': 'E02002', 'Full Name': 'Kai Le', 'Job Title': 'Controls Engineer', 'Department': 'Engineering', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '47', 'Hire Date': '2/5/2022', 'Annual Salary': '$92,368 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Columbus', 'Exit Date': ''}
{'Employee ID': 'E02003', 'Full Name': 'Robert Patel', 'Job Title': 'Analyst', 'Department': 'Sales', 'Business Unit': 'Corporate', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '58', 'Hire Date': '10/23/2013', 'Annual Salary': '$45,703 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Chicago', 'Exit Date': ''}
{'Employee ID': 'E02004', 'Full Name': 'Cameron Lo', 'Job Title': 'Network Administrator', 'Department': 'IT', 'Business Unit': 'Research & Development', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '34', 'Hire Date': '3/24/2019', 'Annual Salary': '$83,576 ', 'Bonus %': '0%', 'Country': 'China', 'City': 'Shanghai', 'Exit Date': ''}
{'Employee ID': 'E02007', 'Full Name': 'Ezra Vu', 'Job Title': 'Network Administrator', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '62', 'Hire Date': '4/22/2004', 'Annual Salary': '$66,227 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Phoenix', 'Exit Date': '2/14/2014'}
{'Employee ID': 'E02009', 'Full Name': 'Miles Chang', 'Job Title': 'Analyst II', 'Department': 'Finance', 'Business Unit': 'Corporate', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '62', 'Hire Date': '2/19/1999', 'Annual Salary': '$69,674 ', 'Bonus %': '0%', 'Country': 'China', 'City': 'Chengdu', 'Exit Date': ''}
{'Employee ID': 'E02011', 'Full Name': 'Jameson Thomas', 'Job Title': 'Manager', 'Department': 'Finance', 'Business Unit': 'Specialty Products', 'Gender': 'Male', 'Ethnicity': 'Caucasian', 'Age': '52', 'Hire Date': '2/5/2015', 'Annual Salary': '$105,879 ', 'Bonus %': '10%', 'Country': 'United States', 'City': 'Miami', 'Exit Date': ''}
{'Employee ID': 'E02012', 'Full Name': 'Jameson Pena', 'Job Title': 'Systems Analyst', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Latino', 'Age': '49', 'Hire Date': '10/12/2003', 'Annual Salary': '$40,499 ', 'Bonus %': '0%', 'Country': 'United States', 'City': 'Miami', 'Exit Date': ''}
{'Employee ID': 'E02014', 'Full Name': 'Jose Wong', 'Job Title': 'Director', 'Department': 'IT', 'Business Unit': 'Manufacturing', 'Gender': 'Male', 'Ethnicity': 'Asian', 'Age': '45', 'Hire Date': '11/15/2017', 'Annual Salary': '$150,558 ', 'Bonus %': '23%', 'Country': 'China', 'City': 'Chongqing', 'Exit Date': ''}
{'Employee ID': 'E02015', 'Full Name': 'Lucas Richardson', 'Job Title': 'Manager', 'Department': 'Marketing', 'Business Unit': 'Corporate', 'Gender': 'Male', 'Ethnicity': 'Caucasian', 'Age': '36', 'Hire Date': '7/22/2018', 'Annual Salary': '$118,912 ', 'Bonus %': '8%', 'Country': 'United States', 'City': 'Miami', 'Exit Date': ''}
{'Employee ID': 'E02016', 'Full Name': 'Jacob Moore', 'Job Title': 'Sr. Manager', 'Department': 'Marketing', 'Business Unit': 'Corporate', 'Gender': 'Male', 'Ethnicity': 'Black', 'Age': '42', 'Hire Date': '3/24/2021', 'Annual Salary': '$131,422 ', 'Bonus %': '15%', 'Country': 'United States', 'City': 'Phoenix', 'Exit Date': ''}
Search results for Department = IT:
/Users/jaideepak/Desktop/elasticsearch_script.py:85: ElasticsearchWarning: Elasticsearch built-in security features are not enabled. Without authentication, your cluster could be accessible to anyone. See https://www.elastic.co/guide/en/elasticsearch/reference/7.17/security-minimal-setup.html to enable security.
  response = es.search(index=p_collection_name, body=query)
Department: IT, Count: 2256
Department: Sales, Count: 1212
Department: Engineering, Count: 1057
Department: Marketing, Count: 883
Department: Accounting, Count: 869
Department: Finance, Count: 835
Department: Human Resources, Count: 826
Department: , Count: 94





