import mysql.connector
import clickhouse_connect
import db_conn_info

# connection information
mysql_connection = mysql.connector.connect(
    host=db_conn_info.mysql_host,
    user=db_conn_info.mysql_user,
    password=db_conn_info.mysql_password,
    database=db_conn_info.mysql_database,
)
mysql_cursor = mysql_connection.cursor()

clickhouse_client = clickhouse_connect.get_client(
    host=db_conn_info.clickhouse_host,
    port=8123,
    user=db_conn_info.clickhouse_user,
    password=db_conn_info.clickhouse_password,
    database=db_conn_info.clickhouse_database,
)

# queries
query_samples_profile_entity_study_values = '''
SELECT gps.ORDERED_SAMPLE_LIST, gp.STABLE_ID, ge.STABLE_ID, cs.CANCER_STUDY_IDENTIFIER, `VALUES`
FROM genetic_profile gp
         JOIN cancer_study cs on gp.CANCER_STUDY_ID = cs.CANCER_STUDY_ID
         JOIN genetic_alteration ga on gp.GENETIC_PROFILE_ID = ga.GENETIC_PROFILE_ID
         JOIN genetic_entity ge on ga.GENETIC_ENTITY_ID = ge.ID
         JOIN genetic_profile_samples gps on gp.GENETIC_PROFILE_ID = gps.GENETIC_PROFILE_ID
WHERE cs.CANCER_STUDY_IDENTIFIER = %s AND gp.GENETIC_ALTERATION_TYPE = 'GENERIC_ASSAY'
'''

query_sample_patient = '''
SELECT s.STABLE_ID, p.INTERNAL_ID, p.STABLE_ID
FROM sample s
        JOIN patient p on s.PATIENT_ID = p.INTERNAL_ID
WHERE s.INTERNAL_ID = %s
'''

# given a study, fetch all samples, genetic profile / entity stable id and values
print('Fetching study data...')
study = 'brca_tcga_pan_can_atlas_2018'
mysql_cursor.execute(query_samples_profile_entity_study_values, (study,))
samples_profile_entity_study_values = mysql_cursor.fetchall()

# extract all sample ids
print('Extracting sample ids...')
sample_id_set = set()
for row in samples_profile_entity_study_values:
    ordered_sample_list = row[0]
    sample_array = ordered_sample_list.split(',')
    if sample_array and sample_array[-1] == '':
        sample_array.pop()
    sample_id_set.update(sample_array)

# given set of sample ids, fetch all patient data
print('Fetching patient data...')
sample_id_to_patient = {}
for sample_id in sample_id_set:
    mysql_cursor.execute(query_sample_patient, (int(sample_id),))
    sample_id_to_patient[sample_id] = mysql_cursor.fetchone()

# denormalize
print('Denormalizing...')
denormalized_data = []
for row in samples_profile_entity_study_values:
    ordered_sample_list, genetic_profile_stable_id, genetic_entity_stable_id, cancer_study_identifier, values = row

    sample_array = ordered_sample_list.split(',')
    if sample_array and sample_array[-1] == '':
        sample_array.pop()

    value_array = values.split(',')
    if value_array and value_array[-1] == '':
        value_array.pop()

    for i in range(len(sample_array)):
        sample_unique_id = sample_array[i]
        sample_stable_id, patient_unique_id, patient_stable_id = sample_id_to_patient.get(sample_unique_id)
        new_row = [sample_unique_id, sample_stable_id, str(patient_unique_id), patient_stable_id, genetic_profile_stable_id, genetic_entity_stable_id, cancer_study_identifier, value_array[i]]
        denormalized_data.append(new_row)

# insert
print('Inserting...')
target_table = 'mysql_genetic_alteration'
clickhouse_client.insert(target_table, denormalized_data)

# close
print('Done!')
mysql_cursor.close()
mysql_connection.close()
clickhouse_client.close()
