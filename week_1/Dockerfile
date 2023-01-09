# What base image to use
FROM python:3.9

# What packages to install
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# What is the working directory for COPY to work in
WORKDIR /app
# Copy from local host machine (cwd) to Docker container in workdir (/app)
COPY ingest_data.py ingest_data.py 

# What should be run when image is run
ENTRYPOINT [ "python", "ingest_data.py" ]