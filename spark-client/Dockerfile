FROM standalone-spark_spark-master:latest

# Install MinIO Client
RUN curl -o mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && mv mc /usr/local/bin/

RUN mc alias set telco_minio http://minio:9000 telcoaz Telco12345 --api S3v4

# Expose Jupyter port
EXPOSE 8888

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Start JupyterLab by default when container runs
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
