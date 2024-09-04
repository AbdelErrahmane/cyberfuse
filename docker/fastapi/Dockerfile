FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y python3-pip openjdk-17-jdk-headless wget

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy application code
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

COPY ./app /app
COPY .env /app/.env
# Set the working directory
WORKDIR /app

# Set the entrypoint
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 
