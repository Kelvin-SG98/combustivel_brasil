#!/bin/bash

# Garante que o script pare se der erro
set -e

# Executa o container usando o código local
docker run --rm -v $(pwd):/app spark-env python3 main.py