FROM bitnami/spark:3.5.1

USER root

# Install required Python packages
RUN pip install pyspark==3.5.1 kafka-python==2.0.2 python-dotenv==1.0.0 psycopg2-binary==2.9.9 textblob==0.17.1 datasketch==1.5.9 pybloom==3.0.0 psycopg2-binary==2.9.9

# Copy processing scripts
COPY processing /opt/processing/

# Set working directory
WORKDIR /opt/processing

# Switch back to spark user
USER 1001 