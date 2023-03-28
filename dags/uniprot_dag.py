from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from datetime import datetime, timedelta
import os
import configparser
import xmltodict
from airflow.models import Variable


# Let's create all our Python functions
def get_program_info():
    config_path = Variable.get("appSettingPath")
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
 
    return {'url':config['DB']['url'], 
            'user':config['DB']['user'], 
            'password':config['DB']['password'], 
            'db':config['DB']['db'], 
            'xml_path':config['DB']['xml_path'],
            'log':config['DB']['log']}


program_info = get_program_info()

print(program_info)


def create_neo4j_connection(url, username, password, db):

    driver = GraphDatabase.driver(url, auth=(username, password), database=db)

    print("Connection created successfully")

    return driver


def get_all_xml_files(xml_path):

    files = os.listdir(xml_path)
    #Making sure I am only ingesting xml files
    xml_files = [file for file in files if file.split(".")[1] == 'xml']

    return xml_files


def convert_xml_to_dict(xml_path, file):
    with open(xml_path + file) as xml_file:
        xml_dict = xmltodict.parse(xml_file.read())
        xml_dict = xml_dict['uniprot']['entry']
        xml_file.close()

    return xml_dict


def cypher_queries_runner(driver, query):
     
    try:
        with driver.session() as session:
            # Execute the Cypher query
            result = session.run(query)
            # Fetch all records
            records = result.data()
            return records

    except ServiceUnavailable as exception:
        logging.error(f"{query} raised an error: \n {exception}")
        raise
      

def create_nodes(driver, node_label, properties):

    print("create_nodes")

    #using MERGE instead of CREATE to prevent duplication
    query = ("MERGE (n:{node_label} {{ {properties} }}) "
              "RETURN n"
            ).format(node_label=node_label, properties=properties)

    try:
        # Execute the Cypher query
        result = cypher_queries_runner(driver, query)

        for record in result:
            node_creation = ("Record created ", 'label:' , node_label, 'properties:', record["n"])
            if program_info["log"]:
                print("Node creation", node_creation)

        return node_creation

    except Exception as exception:
        logging.error(f"{query} raised an error: \n {exception}")
    raise
      

def create_relation(driver, relation, node_label1, properties1, node_label2, properties2):

    query = ("MERGE (n1:{node_label1} {{ {properties1} }}) "
             "MERGE (n2:{node_label2} {{ {properties2} }}) "
             "MERGE (n1)-[r:{relation}]->(n2) "
             "RETURN n1, n2"
            ).format(node_label1=node_label1, properties1=properties1, 
                    node_label2=node_label2, properties2=properties2,
                    relation = relation)
    
    if program_info["log"]==True: 
         print("query", query)

    try:
        # Execute the Cypher query
        record = cypher_queries_runner(driver, query)
        if program_info["log"]==True:
            print("Relation created", {f'Relation {relation}': [{"n1": row["n1"]["name"], "n2": row["n2"]["name"]} for row in record]})

        # return "record created ", {'label': list(record['n'].labels)[0], 'properties': dict(record['n'])}
        return ("Relation created", {f'Relation {relation}': [{"n1": row["n1"]["name"], "n2": row["n2"]["name"]} for row in record]})

    except Exception as exception:
        logging.error(f"{query} raised an error: \n {exception}")
        raise


def create_data_model(driver, xml_files):

    if program_info["log"]==True:
        print("xml files", xml_files)
        print("Neo4j driver", driver)

    for file in xml_files:

        dict_data = convert_xml_to_dict(program_info["xml_path"], file)

        # Extract the name from the query result
        #cypher_queries_runner(driver, query)
        protein_name  = dict_data['name']

        print("Protein name", protein_name)

        protein_properties = f'name: "{protein_name}", id: "{file.split(".")[0]}"'

        if program_info["log"]==True:
            print("Protein properties", protein_properties)

        create_nodes(driver, "Protein", protein_properties)

        p_full_name = dict_data['protein']['recommendedName']['fullName']

        print("Protein full name", p_full_name)

         #create Full Name node and its relation with Protein node
        create_relation (driver, "HAS_FULL_NAME", "Protein", protein_properties, "FullName", f'name: "{p_full_name}"')

        organism_name = dict_data['organism']['name'][0]['#text']
        taxonomy_id = dict_data['organism']['dbReference']['@id']

        print("Organism Name", organism_name)
        print("Organism Name", taxonomy_id)
        #create Organism node and its relation with Protein node
        create_relation (driver, "IN_ORGANISM", "Protein", protein_properties, "Organism", f'name: "{organism_name}", taxonomy_id: "{taxonomy_id}"')

        #create Feature node and its relation with Protein node
        count = 0
        for gene in dict_data['feature']:
            #type="helix" evidence="13" does not have description label
            if '@description' in gene:
                feature_name = gene['@description'][:50]
            
            feature_type = gene['@type']
            #there are different structure in this case, some have "begin position=" label others "position position=" label 
            # what to do in the case of a range of position to be determined (begin and end position)
            if "begin" in gene['location']:
                feature_position = gene['location']["begin"]["@position"]
            else:
                feature_position = gene['location']["position"]["@position"]

            if program_info["log"]==True:
                print("Organism Name", organism_name)
                print("Taxonomy id", taxonomy_id)
                print("Feature_position", feature_position)

            create_relation (driver, f'HAS_FEATURE {{position:"{feature_position}"}}', "Protein", protein_properties, "Feature", f'name: "{feature_name}", type: "{feature_type}"')

            count +=1

        print(count, f"Created Features for {protein_name} protein")

        count = 0
         #create Reference node and its relation with Protein node
        for gene in dict_data['gene']['name']:
            gene_name = gene['#text']
            gene_status = gene['@type']
            print("gene_name", gene_name)
        
            #create gene node and its relation with Protein node
            create_relation (driver, f'FROM_GENE {{status:"{gene_status}"}}', "Protein", protein_properties, "Gene", f'name: "{gene_name}"')
            count +=1

        print(count, f"Created genes for {protein_name} protein")

        #create Gene node and its relation with Protein node
        references = dict_data['reference']
        count = 0
        for reference in references:
            reference_key = reference['@key']
            reference_type = reference["citation"]['@type']
            if program_info["log"]==True:
                print("Reference key", reference_key)
                print("Reference type", reference_type)
            #there are missing name attributes in references
            if '@name' in reference["citation"]:
                reference_name = reference["citation"]['@name']
                print("Reference name", reference_name)
                create_relation (driver, f'HAS_REFERENCE', "Protein", protein_properties, "Reference", f'name: "{reference_name}", type: "{reference_type}", id: "{reference_key}"')

            #there are missing person in authorList
            #create author node and its relation with reference node
            if "person" in reference["citation"]["authorList"]:
                author_count = 0
                for author in reference["citation"]['authorList']["person"]:
                    #there are missing name attributes in person label
                    if'@name' in author:
                        author_name = author['@name']
                        if program_info["log"]==True:
                            print("Author name", author_name)
                        create_relation (driver, f'HAS_AUTHOR', "Reference", f'name: "{reference_name}", type: "{reference_type}", id: "{reference_key}"', "Author", f'name: "{author_name}"')
                        author_count += 1
                print(author_count,f'Created Authors for {reference_name} reference')
             
            count += 1
        print(count, f"Created references for {protein_name} protein name")

default_args = {
    'owner': 'Eimis Pacheco',
    'start_date': datetime(2023, 3, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('neo4j_pipeline', default_args=default_args, schedule_interval='@daily')


# All Tasks
get_program_info_task = PythonOperator(
    task_id='get_program_info_task',
    python_callable=get_program_info,
    dag=dag
)


create_data_model_task = PythonOperator(
    task_id='create_data_model_task',
    python_callable=create_data_model,
    op_args=[create_neo4j_connection(program_info['url'], program_info['user'], program_info['password'], program_info['db']), 
             get_all_xml_files(program_info['xml_path'])],
    dag=dag
)

get_program_info_task.set_downstream(create_data_model_task)

