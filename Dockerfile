FROM openjdk:11

# Instala o Python e dependências
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install pandas requests openpyxl pyspark

WORKDIR /app
COPY . .

CMD ["python3", "main.py"]
