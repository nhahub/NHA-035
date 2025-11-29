FROM apache/airflow:2.10.2-python3.10

# 1. نسخ ملف المتطلبات
COPY requirements.txt /requirements.txt

# 2. تثبيت s
RUN pip install --no-cache-dir -r /requirements.txt

# 3. نسخ باقي ملفات المشروع (DAGs – Scripts – Data)
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY Data/ /opt/airflow/Data/
COPY config/ /opt/airflow/config/
COPY outputs/ /opt/airflow/outputs/
COPY model /opt/airflow/model