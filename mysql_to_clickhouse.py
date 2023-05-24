import mysql.connector
import clickhouse_connect

# connection information
mysql_connection = mysql.connector.connect(
    host='host',
    user='username',
    password='password',
    database='database',
)
mysql_cursor = mysql_connection.cursor()

clickhouse_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    user='username',
    password='password',
    database='database',
)

# queries
query_genetic_alteration_total_rows = '''
    SELECT COUNT(*) 
    FROM genetic_alteration
'''

query_values_samples_profile_entity = '''
    SELECT `VALUES`, ORDERED_SAMPLE_LIST, gp.STABLE_ID, ge.STABLE_ID
    FROM genetic_alteration ga
    JOIN genetic_profile_samples gps ON ga.GENETIC_PROFILE_ID = gps.GENETIC_PROFILE_ID
    JOIN genetic_profile gp ON ga.GENETIC_PROFILE_ID = gp.GENETIC_PROFILE_ID
    JOIN genetic_entity ge ON ga.GENETIC_ENTITY_ID = ge.ID
    WHERE gp.GENETIC_ALTERATION_TYPE = 'GENERIC_ASSAY'
    LIMIT %s OFFSET %s
'''

query_sample_patient_cancer_study = '''
    SELECT s.STABLE_ID, p.INTERNAL_ID, p.STABLE_ID, cs.CANCER_STUDY_IDENTIFIER
    FROM sample s
    JOIN patient p ON s.PATIENT_ID = p.INTERNAL_ID
    JOIN cancer_study cs on p.CANCER_STUDY_ID = cs.CANCER_STUDY_ID
    WHERE s.INTERNAL_ID = %(value)s
'''

# get total number of rows
mysql_cursor.execute(query_genetic_alteration_total_rows)
total_rows = mysql_cursor.fetchone()[0]

# transform all data and insert
batch_size = 10000
start_row = 0
while start_row < total_rows:
    print('Working on', start_row, 'out of', total_rows, 'rows')

    # fetch values, samples, profile and entity stable id
    mysql_cursor.execute(query_values_samples_profile_entity, (batch_size, start_row))
    profile_entity_values_samples = mysql_cursor.fetchall()

    # split data
    transformed_data = []
    sample_id_set = set()
    for row in profile_entity_values_samples:
        values, ordered_sample_list, genetic_profile_stable_id, genetic_entity_stable_id = row

        value_array = values.split(',')
        if value_array and value_array[-1] == '':
            value_array.pop()

        sample_array = ordered_sample_list.split(',')
        if sample_array and sample_array[-1] == '':
            sample_array.pop()
        sample_id_set.update(sample_array)

        if len(value_array) == len(sample_array):
            for i in range(len(value_array)):
                new_row_1 = [sample_array[i], genetic_profile_stable_id, genetic_entity_stable_id, value_array[i]]
                transformed_data.append(new_row_1)

    # fetch other columns from extracted sample ids
    sample_id_to_sample_patient_cancer_study = {}
    for sample_id in sample_id_set:
        params = {'value': sample_id}
        mysql_cursor.execute(query_sample_patient_cancer_study, params)
        sample_patient_cancer_study = mysql_cursor.fetchone()

        sample_stable_id, patient_unique_id, patient_stable_id, cancer_study_identifier = sample_patient_cancer_study

        new_row_2 = [sample_stable_id, str(patient_unique_id), patient_stable_id, cancer_study_identifier]
        sample_id_to_sample_patient_cancer_study[sample_id] = new_row_2

    # reorganize data together
    for row_1 in transformed_data:
        row_2 = sample_id_to_sample_patient_cancer_study.get(row_1[0])
        row_1[1:1] = row_2[:3]
        row_1[-1:-1] = [row_2[3]]

    # insert this batch into ClickHouse
    clickhouse_client.insert('mysql_genetic_alteration', transformed_data)
    start_row += batch_size

# close
print('Transform completed. Closing...')
mysql_cursor.close()
mysql_connection.close()
clickhouse_client.close()
